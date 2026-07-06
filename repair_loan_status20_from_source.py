#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""修复目标库 due_date/status 条件下 loan_no 中间段为 market 长号的行。

查询链路（与手工 SQL 一致）:
  1. 目标: SELECT * FROM loan WHERE due_date < ? AND status = ?
  2. 默认处理全部符合条件的 loan（长号改 loan_no + 同步；短号仅同步 status 等）
  3. ext_sn = application_no 后缀（如 166487616812019719）
  4. 源库: SELECT sn FROM ng_loan_core.application WHERE ext_sn = ?
  5. 源库: SELECT * FROM ng_loan_core.repay_plan WHERE sn = ?（取 max plan_sn）
  6. 正确 loan_no = ng-{core_sn}-01000，并同步 repay_plan 状态字段

Usage:
  python3 repair_loan_status20_from_source.py --env ./ng_migration.env --dry-run --plan-only
  python3 repair_loan_status20_from_source.py --env ./ng_migration.env --apply --workers 8 --commit-every 100
"""
import argparse
import hashlib
import multiprocessing
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pymysql
from pymysql.cursors import DictCursor

import ng_migration_run as mig
from repair_loan_no_from_audit import (
    CommitTracker,
    RepairAuditLog,
    RowChangeAuditLog,
    cols_sql,
    exec_with_retry,
    fetch_loan_row,
    loan_exists,
)

HERE = Path(__file__).resolve().parent
LOAN_COLS = mig.LOAN_INSERT_COLS
SYNC_COLS = [c for c in LOAN_COLS if c != "loan_no"]
# sync_status 不改 application_no/period/roll_sequence，避免与残留长号行 PK 冲突
STATUS_SYNC_COLS = [
    c for c in SYNC_COLS if c not in ("application_no", "period", "roll_sequence")
]
LOAN_NO_RE = re.compile(r"^[Nn][Gg]-(\d+)-(\d{5})$")
APP_NO_RE = re.compile(r"^ng\d{4}-(.+)$", re.I)


def load_env(path: Path) -> Dict[str, str]:
    cfg: Dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        cfg[k.strip()] = v.strip().strip("'\"")
    return cfg


def connect_source(cfg: Dict[str, str]):
    return pymysql.connect(
        host=cfg["SOURCE_HOST"],
        port=int(cfg.get("SOURCE_PORT", "3306")),
        user=cfg["SOURCE_USER"],
        password=cfg["SOURCE_PASSWORD"],
        charset="utf8mb4",
        cursorclass=DictCursor,
        read_timeout=3600,
        write_timeout=3600,
    )


def connect_target(cfg: Dict[str, str]):
    return pymysql.connect(
        host=cfg["TARGET_HOST"],
        port=int(cfg.get("TARGET_PORT", "3306")),
        user=cfg["TARGET_USER"],
        password=cfg["TARGET_PASSWORD"],
        database=cfg.get("TARGET_DB", "ng"),
        charset="utf8mb4",
        cursorclass=DictCursor,
        read_timeout=3600,
        write_timeout=3600,
        autocommit=False,
    )


def parse_loan_middle(loan_no: str) -> Optional[Tuple[str, int]]:
    m = LOAN_NO_RE.match(str(loan_no or "").strip())
    if not m:
        return None
    return m.group(1), len(m.group(1))


def extract_market_no(application_no: str, loan_no: str) -> str:
    m = APP_NO_RE.match(str(application_no or "").strip())
    if m:
        return m.group(1).strip()
    parsed = parse_loan_middle(loan_no)
    if parsed:
        return parsed[0]
    return ""


def is_long_loan_no(loan_no: str, min_sn_len: int) -> bool:
    parsed = parse_loan_middle(loan_no)
    return bool(parsed and parsed[1] >= min_sn_len)


def load_all_target_loans(tgt, due_before: str, status: str) -> List[dict]:
    """一次拉取目标库符合条件的全部 loan。"""
    sql = """
        SELECT %s
        FROM loan
        WHERE due_date < %%s AND status = %%s
        ORDER BY loan_no ASC
    """ % cols_sql(LOAN_COLS)
    with tgt.cursor() as cur:
        cur.execute(sql, (due_before, status))
        return list(cur.fetchall())


def filter_long_candidates(rows: List[dict], min_sn_len: int) -> List[dict]:
    out = []
    for row in rows:
        if is_long_loan_no(str(row.get("loan_no") or ""), min_sn_len):
            out.append(row)
    return out


def fetch_repay_plan_by_sns(src, core_sns: List[str]) -> Dict[str, dict]:
    """core sn -> repay_plan 行（取最大 plan_sn，与 ng_migration_run 一致）。"""
    if not core_sns:
        return {}
    uniq = sorted({str(x).strip() for x in core_sns if x})
    out: Dict[str, dict] = {}
    c = "ng_loan_core"
    for i in range(0, len(uniq), 2000):
        part = uniq[i : i + 2000]
        ph = ",".join(["%s"] * len(part))
        with src.cursor() as cur:
            cur.execute(
                f"""
                SELECT rp.sn, rp.plan_sn, rp.start_date, rp.due_date, rp.prin_amt,
                       rp.interest, rp.orig_fee, rp.penalty, rp.amt, rp.`status`,
                       rp.repaid_amt, rp.repay_last_time, rp.settle_time, rp.created_at
                FROM {c}.repay_plan rp
                INNER JOIN (
                    SELECT sn, MAX(plan_sn) AS max_plan_sn
                    FROM {c}.repay_plan
                    WHERE sn IN ({ph})
                    GROUP BY sn
                ) pick ON rp.sn = pick.sn AND rp.plan_sn = pick.max_plan_sn
                """,
                part,
            )
            for row in cur.fetchall():
                sn = str(row.get("sn") or "").strip()
                if sn:
                    out[sn] = row
    return out


def fetch_core_application_by_ext_sn(
    src, ext_sns: List[str]
) -> Dict[str, str]:
    """ext_sn(market applicationNo) -> core sn。"""
    uniq = sorted({str(x).strip() for x in ext_sns if x})
    out: Dict[str, str] = {}
    if not uniq:
        return out
    c = "ng_loan_core"
    for i in range(0, len(uniq), 2000):
        part = uniq[i : i + 2000]
        ph = ",".join(["%s"] * len(part))
        with src.cursor() as cur:
            cur.execute(
                f"""
                SELECT ext_sn, sn
                FROM {c}.application
                WHERE ext_sn IN ({ph})
                  AND sn IS NOT NULL AND sn <> ''
                """,
                part,
            )
            for row in cur.fetchall():
                ext = str(row.get("ext_sn") or "").strip()
                sn = str(row.get("sn") or "").strip()
                if ext and sn:
                    out[ext] = sn
    return out


def is_plausible_core_sn(
    sn: str, market_no: str = "", min_sn_len: int = 15
) -> bool:
    """core sn 约 12 位；market 号 15~18 位，不能当 core sn 用。"""
    s = str(sn or "").strip()
    if not s or not s.isdigit():
        return False
    if market_no and s == str(market_no).strip():
        return False
    return len(s) < min_sn_len


def build_meta_from_ext_sn(
    src,
    ext_sns: List[str],
    target_app_by_ext: Dict[str, str],
    min_sn_len: int,
) -> Dict[str, Tuple[str, str]]:
    """ext_sn -> (core_sn, target application_no)。仅查 ng_loan_core.application。"""
    uniq = sorted({str(x).strip() for x in ext_sns if x})
    core_by_ext = fetch_core_application_by_ext_sn(src, uniq)
    meta: Dict[str, Tuple[str, str]] = {}
    for ext_sn in uniq:
        core_sn = core_by_ext.get(ext_sn, "")
        app_no = target_app_by_ext.get(ext_sn, "")
        if not core_sn or not is_plausible_core_sn(core_sn, ext_sn, min_sn_len):
            continue
        if not app_no:
            continue
        meta[ext_sn] = (core_sn, app_no)
    return meta


def fetch_source_loans_bulk(
    src,
    ext_sns: List[str],
    target_app_by_ext: Dict[str, str],
    min_sn_len: int = 15,
) -> Tuple[Dict[str, dict], Dict[str, str], Dict[str, Tuple[str, str]]]:
    """ext_sn -> 目标形态 loan 行；miss_reason；meta。"""
    if not ext_sns:
        return {}, {}, {}
    uniq = sorted({str(x).strip() for x in ext_sns if x})
    meta = build_meta_from_ext_sn(src, uniq, target_app_by_ext, min_sn_len)
    repay_plans = fetch_repay_plan_by_sns(src, [v[0] for v in meta.values()])
    by_ext: Dict[str, dict] = {}
    miss_reason: Dict[str, str] = {}
    for ext_sn in uniq:
        pair = meta.get(ext_sn)
        if not pair:
            miss_reason[ext_sn] = "no_core_application"
            continue
        core_sn, app_no = pair
        rp = repay_plans.get(core_sn)
        if not rp:
            miss_reason[ext_sn] = "no_repay_plan"
            continue
        by_ext[ext_sn] = mig._build_loan_row(rp, app_no)
    return by_ext, miss_reason, meta


def build_rekey_row_from_before(
    before: dict,
    application_no: str,
    core_sn: str,
    market_no: str = "",
    min_sn_len: int = 15,
) -> Optional[dict]:
    if not is_plausible_core_sn(core_sn, market_no, min_sn_len):
        return None
    correct = mig.format_loan_no(core_sn, 1, 0)
    if not before or not correct:
        return None
    if correct == str(before.get("loan_no") or ""):
        return None
    row = dict(before)
    row["loan_no"] = correct
    if application_no:
        row["application_no"] = application_no
    return row


def row_needs_sync(target: dict, source: dict) -> bool:
    """目标行与源库组装行是否有业务字段差异。"""
    for col in SYNC_COLS:
        tv = target.get(col)
        sv = source.get(col)
        if tv is None and sv is None:
            continue
        if str(tv) != str(sv):
            return True
    return False


def build_plan_in_memory(
    candidates: List[dict],
    source_by_ext: Dict[str, dict],
    miss_reason: Dict[str, str],
    meta_by_ext: Dict[str, Tuple[str, str]],
    min_sn_len: int = 15,
) -> List[dict]:
    plan: List[dict] = []
    skip_n = 0
    for row in candidates:
        wrong = str(row["loan_no"])
        app_no = str(row.get("application_no") or "")
        ext_sn = extract_market_no(app_no, wrong)
        if not ext_sn:
            skip_n += 1
            continue

        src_row = source_by_ext.get(ext_sn)
        mode = "source"
        if not src_row:
            pair = meta_by_ext.get(ext_sn)
            if pair and miss_reason.get(ext_sn) == "no_repay_plan":
                src_row = build_rekey_row_from_before(
                    row, app_no, pair[0], ext_sn, min_sn_len
                )
                mode = "rekey_only" if src_row else ""
            if not src_row:
                skip_n += 1
                continue

        correct = str(src_row["loan_no"])
        if correct == wrong:
            if not row_needs_sync(row, src_row):
                continue
            mode = "sync_status"
        elif not is_long_loan_no(wrong, min_sn_len):
            mode = "rekey_short"

        plan.append(
            {
                "wrong_loan_no": wrong,
                "correct_loan_no": correct,
                "legacy_loan_no": "",
                "application_no": str(src_row.get("application_no") or app_no),
                "app_id": "",
                "market_no": ext_sn,
                "source_row": src_row,
                "before_wrong": dict(row),
                "sync_mode": mode,
                "target_due_date": str(row.get("due_date") or ""),
                "target_status": str(row.get("status") or ""),
            }
        )
    if skip_n:
        print("plan_skip=%s (no ext_sn / no core.application / no repay_plan)" % skip_n, flush=True)
    return plan


def _merge_source_row(before: dict, source_row: dict) -> dict:
    after = dict(before)
    for col in LOAN_COLS:
        if col in source_row:
            after[col] = source_row[col]
    return after


def _loan_present(tgt, loan_no: str, loan_no_set: Optional[set]) -> bool:
    if loan_no_set is not None:
        return loan_no in loan_no_set
    return loan_exists(tgt, loan_no)


def _build_set_sql(cols: List[str]) -> str:
    return ", ".join("`%s`=%%s" % c for c in cols)


def _sync_update(tgt, loan_no: str, source_row: dict, cols: List[str]) -> int:
    if not cols:
        return 0
    vals = [source_row[c] for c in cols]
    with tgt.cursor() as cur:
        cur.execute(
            "UPDATE loan SET %s WHERE loan_no=%%s" % _build_set_sql(cols),
            vals + [loan_no],
        )
        return cur.rowcount


def _rekey_update(
    tgt, wrong: str, correct: str, source_row: dict, cols: List[str]
) -> int:
    vals = [source_row[c] for c in cols]
    with tgt.cursor() as cur:
        cur.execute(
            "UPDATE loan SET loan_no=%%s, %s WHERE loan_no=%%s"
            % _build_set_sql(cols),
            [correct] + vals + [wrong],
        )
        return cur.rowcount


def _delete_loan(tgt, loan_no: str) -> int:
    with tgt.cursor() as cur:
        cur.execute("DELETE FROM loan WHERE loan_no=%s", (loan_no,))
        return cur.rowcount


def _finalize_write(tgt, tracker: Optional[CommitTracker], dry_run: bool) -> None:
    if tracker:
        tracker.note_write()
    elif not dry_run:
        tgt.commit()


def sort_plan_for_apply(plan: List[dict]) -> List[dict]:
    """同一 correct_loan_no：先 rekey/sync_delete，后 sync_status。"""

    def sort_key(row: dict):
        wrong = row["wrong_loan_no"]
        correct = row["correct_loan_no"]
        if wrong != correct:
            priority = 0
        elif row.get("sync_mode") == "sync_status":
            priority = 2
        else:
            priority = 1
        return (correct, priority, wrong)

    return sorted(plan, key=sort_key)


def sync_one_loan(
    tgt,
    plan_row: dict,
    dry_run: bool,
    audit: RepairAuditLog,
    row_audit: Optional[RowChangeAuditLog],
    tracker: Optional[CommitTracker],
    loan_no_set: Optional[set] = None,
    loan_by_no: Optional[Dict[str, dict]] = None,
) -> str:
    wrong = plan_row["wrong_loan_no"]
    correct = plan_row["correct_loan_no"]
    source_row = plan_row["source_row"]
    mode = plan_row.get("sync_mode", "")
    cache = loan_by_no or {}
    before_wrong = plan_row.get("before_wrong") or cache.get(wrong)

    if not _loan_present(tgt, wrong, loan_no_set):
        if _loan_present(tgt, correct, loan_no_set):
            audit.record("skip_done", plan_row, "wrong_missing_correct_exists")
            return "skip_done"
        audit.record("skip", plan_row, "wrong_missing")
        return "skip_missing"

    if not before_wrong:
        before_wrong = fetch_loan_row(tgt, wrong)
    if not before_wrong:
        audit.record("skip", plan_row, "fetch_wrong_missing")
        return "skip_missing"

    def _track_rekey(after: dict):
        if loan_no_set is not None:
            loan_no_set.discard(wrong)
            loan_no_set.add(correct)
            cache.pop(wrong, None)
            cache[correct] = after

    def _handle_1062(exc: Exception, action: str) -> str:
        """1062: correct 已存在则合并并删 wrong；sync_status 则跳过 PK 列重试。"""
        if correct != wrong and loan_exists(tgt, correct):
            before_correct = cache.get(correct) or fetch_loan_row(tgt, correct)
            if dry_run:
                if before_correct and row_audit:
                    row_audit.record_modified(
                        "would_sync_correct_after_1062",
                        before_correct,
                        _merge_source_row(before_correct, source_row),
                    )
                if row_audit:
                    row_audit.record_deleted("would_delete_wrong_after_1062", before_wrong)
            else:
                _sync_update(tgt, correct, source_row, SYNC_COLS)
                if _loan_present(tgt, wrong, loan_no_set):
                    _delete_loan(tgt, wrong)
                if row_audit and before_correct:
                    row_audit.record_modified(
                        "sync_correct_after_1062",
                        before_correct,
                        _merge_source_row(before_correct, source_row),
                    )
                if row_audit:
                    row_audit.record_deleted("delete_wrong_after_1062", before_wrong)
                if loan_no_set is not None:
                    loan_no_set.discard(wrong)
                    loan_no_set.add(correct)
                    cache.pop(wrong, None)
                    cache[correct] = _merge_source_row(before_correct or {}, source_row)
            audit.record(
                "sync_delete_after_1062",
                plan_row,
                "%s:%s" % (action, exc),
            )
            _finalize_write(tgt, tracker, dry_run)
            return "ok"
        if action == "sync_status" and _loan_present(tgt, wrong, loan_no_set):
            if dry_run:
                after = _merge_source_row(before_wrong, source_row)
                if row_audit:
                    row_audit.record_modified(
                        "would_sync_status_no_pk_cols", before_wrong, after
                    )
            else:
                if not _sync_update(tgt, wrong, source_row, STATUS_SYNC_COLS):
                    audit.record("skip", plan_row, "sync_status_no_row_after_1062")
                    return "missing"
                if loan_no_set is not None:
                    cache[wrong] = _merge_source_row(before_wrong, source_row)
                if row_audit:
                    row_audit.record_modified(
                        "sync_status_no_pk_cols",
                        before_wrong,
                        _merge_source_row(before_wrong, source_row),
                    )
            audit.record("sync_status_no_pk_cols", plan_row, "1062_fallback")
            _finalize_write(tgt, tracker, dry_run)
            return "ok"
        audit.record("skip", plan_row, "duplicate_key:%s" % exc)
        return "skip_duplicate"

    if mode == "sync_status" or wrong == correct:
        if dry_run:
            after = _merge_source_row(before_wrong, source_row)
            if row_audit:
                row_audit.record_modified("would_sync_status", before_wrong, after)
            audit.record("would_sync_status", plan_row, "sync_status")
            return "ok"
        after = _merge_source_row(before_wrong, source_row)
        try:
            if not _sync_update(tgt, wrong, source_row, STATUS_SYNC_COLS):
                audit.record("skip", plan_row, "sync_status_no_row")
                return "missing"
        except pymysql.err.IntegrityError as exc:
            if exc.args[0] != 1062:
                raise
            return _handle_1062(exc, "sync_status")
        if row_audit:
            row_audit.record_modified("sync_status", before_wrong, after)
        if loan_no_set is not None:
            cache[wrong] = after
        audit.record("sync_status", plan_row, "sync_status")
        _finalize_write(tgt, tracker, dry_run)
        return "ok"

    if _loan_present(tgt, correct, loan_no_set) and correct != wrong:
        before_correct = cache.get(correct) or fetch_loan_row(tgt, correct)
        if dry_run:
            if before_correct and row_audit:
                row_audit.record_modified(
                    "would_sync_correct",
                    before_correct,
                    _merge_source_row(before_correct, source_row),
                )
            if row_audit:
                row_audit.record_deleted("would_delete_wrong", before_wrong)
            audit.record(
                "would_sync_delete",
                plan_row,
                plan_row.get("sync_mode", "update_correct+delete_wrong"),
            )
            return "ok"
        try:
            _sync_update(tgt, correct, source_row, SYNC_COLS)
        except pymysql.err.IntegrityError as exc:
            if exc.args[0] != 1062:
                raise
            return _handle_1062(exc, "sync_delete")
        if row_audit and before_correct:
            row_audit.record_modified(
                "sync_correct",
                before_correct,
                _merge_source_row(before_correct, source_row),
            )
        if not _delete_loan(tgt, wrong):
            audit.record("skip", plan_row, "delete_wrong_failed")
            return "missing"
        if row_audit:
            row_audit.record_deleted("delete_wrong", before_wrong)
        if loan_no_set is not None:
            loan_no_set.discard(wrong)
            loan_no_set.add(correct)
        cache[correct] = _merge_source_row(before_correct or {}, source_row)
        cache.pop(wrong, None)
        audit.record("sync_delete", plan_row, plan_row.get("sync_mode", ""))
        _finalize_write(tgt, tracker, dry_run)
        return "ok"

    if loan_exists(tgt, correct) and correct != wrong:
        before_correct = fetch_loan_row(tgt, correct)
        if dry_run:
            if before_correct and row_audit:
                row_audit.record_modified(
                    "would_sync_correct",
                    before_correct,
                    _merge_source_row(before_correct, source_row),
                )
            if row_audit:
                row_audit.record_deleted("would_delete_wrong", before_wrong)
            audit.record("would_sync_delete", plan_row, "race_correct_exists")
            return "ok"
        try:
            _sync_update(tgt, correct, source_row, SYNC_COLS)
        except pymysql.err.IntegrityError as exc:
            if exc.args[0] != 1062:
                raise
            return _handle_1062(exc, "sync_delete")
        if row_audit and before_correct:
            row_audit.record_modified(
                "sync_correct",
                before_correct,
                _merge_source_row(before_correct, source_row),
            )
        if not _delete_loan(tgt, wrong):
            audit.record("skip", plan_row, "delete_wrong_failed")
            return "missing"
        if row_audit:
            row_audit.record_deleted("delete_wrong", before_wrong)
        if loan_no_set is not None:
            loan_no_set.discard(wrong)
            loan_no_set.add(correct)
        cache[correct] = _merge_source_row(before_correct or {}, source_row)
        cache.pop(wrong, None)
        audit.record("sync_delete", plan_row, "race_correct_exists")
        _finalize_write(tgt, tracker, dry_run)
        return "ok"

    if dry_run:
        after = _merge_source_row(before_wrong, source_row)
        if row_audit:
            row_audit.record_modified("would_rekey_sync", before_wrong, after)
        audit.record("would_rekey_sync", plan_row, plan_row.get("sync_mode", ""))
        return "ok"
    after = _merge_source_row(before_wrong, source_row)
    try:
        if not _rekey_update(tgt, wrong, correct, source_row, SYNC_COLS):
            audit.record("skip", plan_row, "rekey_update_no_row")
            return "missing"
    except pymysql.err.IntegrityError as exc:
        if exc.args[0] != 1062:
            raise
        return _handle_1062(exc, "rekey")
    if row_audit:
        row_audit.record_modified("rekey_sync", before_wrong, after)
    _track_rekey(after)
    audit.record("rekey_sync", plan_row, plan_row.get("sync_mode", ""))
    _finalize_write(tgt, tracker, dry_run)
    return "ok"


def split_plan_chunks(plan: List[dict], workers: int) -> List[List[dict]]:
    """按 correct_loan_no 哈希分片，避免并行 worker 同时改同一 loan_no。"""
    n = max(1, int(workers))
    if not plan:
        return []
    chunks: List[List[dict]] = [[] for _ in range(n)]
    for row in plan:
        key = str(row.get("correct_loan_no") or row["wrong_loan_no"])
        idx = int(hashlib.md5(key.encode("utf-8")).hexdigest(), 16) % n
        chunks[idx].append(row)
    return chunks


def _worker_repair_log_path(base: str, worker_id: int) -> str:
    if not base:
        return ""
    p = Path(base)
    return str(p.with_name("%s.w%s%s" % (p.stem, worker_id, p.suffix or ".csv")))


def run_sync_chunk(
    tgt,
    chunk: List[dict],
    dry_run: bool,
    audit: RepairAuditLog,
    row_audit: Optional[RowChangeAuditLog],
    commit_every: int,
    work_limit: int,
    log_every: int,
    prefix: str = "",
) -> Tuple[int, int]:
    ok = skip = 0
    tracker = CommitTracker(tgt, commit_every, dry_run)
    for i, row in enumerate(chunk, 1):
        if work_limit and ok >= work_limit:
            break
        result = exec_with_retry(
            tgt,
            lambda r=row: sync_one_loan(
                tgt, r, dry_run, audit, row_audit, tracker, None, None
            ),
            "sync %s" % row["wrong_loan_no"],
        )
        if result == "ok":
            ok += 1
        else:
            skip += 1
        if i % max(1, log_every) == 0:
            print(
                "%sprogress ok=%s skip=%s last=%s mode=%s"
                % (prefix, ok, skip, row["wrong_loan_no"], row.get("sync_mode", "")),
                flush=True,
            )
    tracker.flush()
    return ok, skip


def sync_worker_run(spec: dict) -> Tuple[int, int]:
    worker_id = spec["worker_id"]
    workers = spec["workers"]
    label = "[%s/%s] " % (worker_id, workers)
    chunk = spec.get("plan_chunk") or []
    if not chunk:
        print("%sempty chunk, skip" % label, flush=True)
        return 0, 0

    cfg = load_env(Path(spec["env"]))
    tgt = connect_target(cfg)
    if spec.get("no_repair_log"):
        audit = RepairAuditLog(None, enabled=False)
        row_audit = RowChangeAuditLog("", enabled=False)
    else:
        repair_log = spec.get("repair_log") or ""
        audit = RepairAuditLog(repair_log or None, enabled=bool(repair_log))
        row_audit = RowChangeAuditLog(repair_log or "", enabled=bool(repair_log))

    try:
        print(
            "%sstart rows=%s first=%s last=%s log=%s"
            % (
                label,
                len(chunk),
                chunk[0]["wrong_loan_no"],
                chunk[-1]["wrong_loan_no"],
                spec.get("repair_log") or "",
            ),
            flush=True,
        )
        ok, skip = run_sync_chunk(
            tgt,
            chunk,
            spec["dry_run"],
            audit,
            row_audit,
            spec["commit_every"],
            spec["work_limit"],
            spec["log_every"],
            label,
        )
        print("%sdone ok=%s skip=%s" % (label, ok, skip), flush=True)
        return ok, skip
    finally:
        audit.close()
        row_audit.close()
        tgt.close()


def run_sync_parallel(
    plan: List[dict],
    workers: int,
    env_path: str,
    dry_run: bool,
    work_limit: int,
    log_every: int,
    commit_every: int,
    repair_log_path: str,
    no_repair_log: bool,
) -> Tuple[int, int]:
    chunks = split_plan_chunks(plan, workers)
    specs = []
    for i, chunk in enumerate(chunks):
        if not chunk:
            continue
        wlog = _worker_repair_log_path(repair_log_path, i) if repair_log_path else ""
        specs.append(
            {
                "worker_id": i,
                "workers": workers,
                "env": env_path,
                "dry_run": dry_run,
                "work_limit": work_limit,
                "log_every": log_every,
                "commit_every": commit_every,
                "plan_chunk": chunk,
                "repair_log": wlog,
                "no_repair_log": no_repair_log,
            }
        )
    if not specs:
        return 0, 0

    for spec in specs:
        chunk = spec["plan_chunk"]
        print(
            "  worker %s/%s: rows=%s [%s .. %s] log=%s"
            % (
                spec["worker_id"],
                workers,
                len(chunk),
                chunk[0]["wrong_loan_no"],
                chunk[-1]["wrong_loan_no"],
                spec.get("repair_log") or "",
            ),
            flush=True,
        )

    ctx = multiprocessing.get_context("spawn")
    with ctx.Pool(processes=len(specs)) as pool:
        results = pool.map(sync_worker_run, specs)

    total_ok = sum(r[0] for r in results)
    total_skip = sum(r[1] for r in results)
    print(
        "sync parallel done workers=%s ok=%s skip=%s"
        % (len(specs), total_ok, total_skip),
        flush=True,
    )
    return total_ok, total_skip


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(
        description="Fix long loan_no + sync status from source (due_date/status filter)"
    )
    p.add_argument("--env", default=str(HERE / "ng_migration.env"))
    p.add_argument("--apply", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--due-before", default="2026-07-01", help="due_date < 该日期")
    p.add_argument("--status", default="20", help="仅处理该 status")
    p.add_argument("--min-sn-len", type=int, default=15, help="loan_no 中间段最小长度")
    p.add_argument("--workers", type=int, default=8, help="并行写库进程数")
    p.add_argument("--commit-every", type=int, default=100)
    p.add_argument("--work-limit", type=int, default=0)
    p.add_argument("--log-every", type=int, default=100)
    p.add_argument("--repair-log", default="")
    p.add_argument("--no-repair-log", action="store_true")
    p.add_argument(
        "--long-only",
        action="store_true",
        help="仅处理 loan_no 长号行（默认处理全部 status 匹配行，短号只同步 status）",
    )
    p.add_argument("--plan-only", action="store_true")
    args = p.parse_args(argv)
    if args.apply and args.dry_run:
        p.error("use either --apply or --dry-run")
    if args.workers < 1:
        p.error("--workers must be >= 1")
    dry_run = not args.apply

    cfg = load_env(Path(args.env))
    env_path = str(Path(args.env).resolve())
    repair_log = args.repair_log or (
        "/tmp/repair_loan_status20_%s.csv"
        % datetime.now().strftime("%Y%m%d_%H%M%S")
    )
    audit = RepairAuditLog(
        repair_log if not args.no_repair_log else None,
        enabled=not args.no_repair_log and not args.plan_only,
    )
    row_audit = RowChangeAuditLog(
        repair_log, enabled=not args.no_repair_log and not args.plan_only
    )

    tgt = connect_target(cfg)
    src = connect_source(cfg)
    try:
        print(
            "start bulk due_before=%s status=%s min_sn_len=%s workers=%s "
            "commit_every=%s dry_run=%s plan_only=%s"
            % (
                args.due_before,
                args.status,
                args.min_sn_len,
                args.workers,
                args.commit_every,
                dry_run,
                args.plan_only,
            ),
            flush=True,
        )
        run_t0 = time.time()

        print("load target loans ...", flush=True)
        t0 = time.time()
        all_loans = exec_with_retry(
            tgt,
            lambda: load_all_target_loans(tgt, args.due_before, args.status),
            "load_all_target_loans",
        )
        loan_by_no = {str(r["loan_no"]): r for r in all_loans}
        loan_no_set = set(loan_by_no.keys())
        print(
            "target_loans=%s elapsed=%.1fs"
            % (len(all_loans), time.time() - t0),
            flush=True,
        )

        candidates = (
            filter_long_candidates(all_loans, args.min_sn_len)
            if args.long_only
            else all_loans
        )
        long_n = sum(
            1 for r in candidates if is_long_loan_no(str(r.get("loan_no") or ""), args.min_sn_len)
        )
        print(
            "candidates=%s long=%s short=%s long_only=%s"
            % (len(candidates), long_n, len(candidates) - long_n, args.long_only),
            flush=True,
        )
        if not candidates:
            print("no candidate loans", flush=True)
            return 0

        ext_sns = sorted(
            {
                extract_market_no(
                    str(r.get("application_no") or ""),
                    str(r.get("loan_no") or ""),
                )
                for r in candidates
            }
            - {""}
        )
        target_app_by_ext = {
            extract_market_no(
                str(r.get("application_no") or ""),
                str(r.get("loan_no") or ""),
            ): str(r.get("application_no") or "")
            for r in candidates
        }
        target_app_by_ext = {k: v for k, v in target_app_by_ext.items() if k and v}

        print(
            "load source core.application ext_sn=%s ..." % len(ext_sns),
            flush=True,
        )
        t0 = time.time()
        source_by_ext, miss_reason, meta_by_ext = fetch_source_loans_bulk(
            src, ext_sns, target_app_by_ext, args.min_sn_len
        )
        print(
            "core_application=%s repay_plan_hits=%s miss=%s elapsed=%.1fs"
            % (
                len(meta_by_ext),
                len(source_by_ext),
                len(miss_reason),
                time.time() - t0,
            ),
            flush=True,
        )

        plan = build_plan_in_memory(
            candidates,
            source_by_ext,
            miss_reason,
            meta_by_ext,
            args.min_sn_len,
        )
        if args.work_limit:
            plan = plan[: args.work_limit]
        modes: Dict[str, int] = {}
        for row in plan:
            m = str(row.get("sync_mode") or "")
            modes[m] = modes.get(m, 0) + 1
        print(
            "repair_plan=%s modes=%s"
            % (len(plan), modes),
            flush=True,
        )
        for row in plan[:20]:
            src_row = row["source_row"]
            if row.get("sync_mode") == "sync_status":
                print(
                    "  %s sync_status tgt_status=%s -> src_status=%s due=%s"
                    % (
                        row["wrong_loan_no"],
                        row.get("target_status"),
                        src_row.get("status"),
                        src_row.get("due_date"),
                    ),
                    flush=True,
                )
            else:
                print(
                    "  %s -> %s mode=%s app=%s src_status=%s due=%s"
                    % (
                        row["wrong_loan_no"],
                        row["correct_loan_no"],
                        row.get("sync_mode", ""),
                        row["application_no"],
                        src_row.get("status"),
                        src_row.get("due_date"),
                    ),
                    flush=True,
                )
        if len(plan) > 20:
            print("  ... and %s more" % (len(plan) - 20), flush=True)
        if args.plan_only:
            return 0 if plan else 1
        if not plan:
            return 0

        plan = sort_plan_for_apply(plan)

        if args.workers > 1:
            audit.close()
            row_audit.close()
            ok, skip = run_sync_parallel(
                plan,
                args.workers,
                env_path,
                dry_run,
                args.work_limit,
                args.log_every,
                args.commit_every,
                repair_log if not args.no_repair_log else "",
                args.no_repair_log,
            )
        else:
            ok = skip = 0
            tracker = CommitTracker(tgt, args.commit_every, dry_run)
            for i, row in enumerate(plan, 1):
                if args.work_limit and ok >= args.work_limit:
                    break
                result = exec_with_retry(
                    tgt,
                    lambda r=row: sync_one_loan(
                        tgt,
                        r,
                        dry_run,
                        audit,
                        row_audit,
                        tracker,
                        loan_no_set,
                        loan_by_no,
                    ),
                    "sync %s" % row["wrong_loan_no"],
                )
                if result == "ok":
                    ok += 1
                else:
                    skip += 1
                if i % max(1, args.log_every) == 0:
                    print(
                        "progress ok=%s skip=%s last=%s mode=%s"
                        % (ok, skip, row["wrong_loan_no"], row.get("sync_mode", "")),
                        flush=True,
                    )
            tracker.flush()

        print(
            "finished plan=%s ok=%s skip=%s elapsed=%.1fs repair_log=%s"
            % (len(plan), ok, skip, time.time() - run_t0, repair_log),
            flush=True,
        )
        return 0
    finally:
        audit.close()
        row_audit.close()
        tgt.close()
        src.close()


if __name__ == "__main__":
    raise SystemExit(main())
