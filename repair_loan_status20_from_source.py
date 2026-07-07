#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""修复目标库 due_date/status 条件下 loan 行（与手工 SQL 一致）。

长号 loan_no（中间段为 market 长号）:
  1. ext_sn = loan_no 中间段 或 application_no 后缀
  2. 源库: SELECT sn FROM ng_loan_core.application WHERE ext_sn = ?
  3. 源库: SELECT * FROM ng_loan_core.repay_plan WHERE sn = ?（max plan_sn）
  4. 原地 UPDATE loan_no = ng-{core_sn}-01000；若短号行已存在则删长号行并同步短号行

短号 loan_no（中间段为 core sn）:
  1. core_sn = loan_no 中间段
  2. 源库: SELECT * FROM ng_loan_core.repay_plan WHERE sn = ?
  3. 仅 UPDATE status/金额/日期等，不改 loan_no；失败则 skip

同一 loan_no 重复行（application_no 后缀误用 core sn）:
  删除错 application_no 行，保留 market 长号行（按主键删，默认扫全表不限 due_date/status）

Usage:
  python3 repair_loan_status20_from_source.py --env ./ng_migration.env --dry-run --plan-only
  python3 repair_loan_status20_from_source.py --env ./ng_migration.env --list-dup
  python3 repair_loan_status20_from_source.py --env ./ng_migration.env --apply --workers 1

  # 只删重复 loan_no 下错挂 application_no 的行（不做 status 同步）
  python3 repair_loan_status20_from_source.py --env ./ng_migration.env --list-dup \\
    --dup-due-before 2026-07-06
  python3 repair_loan_status20_from_source.py --env ./ng_migration.env --dup-only \\
    --dup-due-before 2026-07-06 --dry-run --plan-only
  python3 repair_loan_status20_from_source.py --env ./ng_migration.env --dup-only \\
    --dup-due-before 2026-07-06 --apply
"""
import argparse
import hashlib
import multiprocessing
import re
import time
from collections import defaultdict
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
DUP_LIST_COLS = [
    "loan_no",
    "application_no",
    "period",
    "roll_sequence",
    "status",
    "due_date",
]
SYNC_COLS = [c for c in LOAN_COLS if c != "loan_no"]
# 更新时不改 application_no/period/roll_sequence，避免主键冲突
UPDATE_COLS = [
    c for c in SYNC_COLS if c not in ("application_no", "period", "roll_sequence")
]
LOAN_NO_RE = re.compile(r"^[Nn][Gg]-(\d+)-(\d{5})$")
APP_NO_RE = re.compile(r"^ng\d+-(.+)$", re.I)


def app_no_market_suffix(application_no: str) -> str:
    m = APP_NO_RE.match(str(application_no or "").strip())
    return m.group(1).strip() if m else ""


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


def is_plausible_core_sn(sn: str, market_no: str = "", min_sn_len: int = 15) -> bool:
    s = str(sn or "").strip()
    if not s or not s.isdigit():
        return False
    if market_no and s == str(market_no).strip():
        return False
    return len(s) < min_sn_len


def is_wrong_application_no(
    application_no: str, loan_no: str, min_sn_len: int
) -> bool:
    """application_no 后缀误用 core sn（如 ng0564-217819556201），应为 market 长号。"""
    m = APP_NO_RE.match(str(application_no or "").strip())
    if not m:
        return False
    suffix = m.group(1).strip()
    parsed = parse_loan_middle(loan_no)
    if not parsed:
        return False
    core_sn = parsed[0]
    return suffix == core_sn and len(suffix) < min_sn_len


def find_duplicate_loan_nos(tgt, due_before: Optional[str] = None) -> List[str]:
    """SQL 直接找重复 loan_no，避免全表拉内存。"""
    if due_before:
        sql = """
            SELECT loan_no
            FROM loan
            WHERE due_date < %s
              AND loan_no IS NOT NULL AND loan_no <> ''
            GROUP BY loan_no
            HAVING COUNT(*) > 1
            ORDER BY loan_no
        """
        args = (due_before,)
    else:
        sql = """
            SELECT loan_no
            FROM loan
            WHERE loan_no IS NOT NULL AND loan_no <> ''
            GROUP BY loan_no
            HAVING COUNT(*) > 1
            ORDER BY loan_no
        """
        args = ()
    with tgt.cursor() as cur:
        cur.execute(sql, args)
        return [str(r["loan_no"]).strip() for r in cur.fetchall() if r.get("loan_no")]


def _chunks_str(items: List[str], size: int):
    for i in range(0, len(items), size):
        yield items[i : i + size]


def load_loan_rows_by_nos(
    tgt,
    loan_nos: List[str],
    cols: List[str],
    due_before: Optional[str] = None,
    chunk: int = 500,
) -> List[dict]:
    uniq = sorted({str(x).strip() for x in loan_nos if x})
    if not uniq:
        return []
    out: List[dict] = []
    total = (len(uniq) + chunk - 1) // chunk
    col_sql = cols_sql(cols)
    for i, part in enumerate(_chunks_str(uniq, chunk), 1):
        ph = ",".join(["%s"] * len(part))
        if due_before:
            sql = (
                "SELECT %s FROM loan WHERE loan_no IN (%s) AND due_date < %%s"
                % (col_sql, ph)
            )
            args: Tuple = tuple(part) + (due_before,)
        else:
            sql = "SELECT %s FROM loan WHERE loan_no IN (%s)" % (col_sql, ph)
            args = tuple(part)
        with tgt.cursor() as cur:
            cur.execute(sql, args)
            out.extend(cur.fetchall())
        if i == 1 or i % 20 == 0 or i == total:
            print(
                "  load_dup_rows %s/%s cumulative=%s"
                % (i, total, len(out)),
                flush=True,
            )
    return out


def load_dup_groups(
    tgt,
    due_before: Optional[str] = None,
    full_rows: bool = False,
    chunk: int = 500,
) -> Dict[str, List[dict]]:
    """先 SQL 找重复 loan_no，再只拉这些 key 的行（快）。"""
    t0 = time.time()
    print("find duplicate loan_no via SQL ...", flush=True)
    dup_nos = find_duplicate_loan_nos(tgt, due_before)
    print(
        "dup_loan_no_keys=%s find_elapsed=%.1fs"
        % (len(dup_nos), time.time() - t0),
        flush=True,
    )
    if not dup_nos:
        return {}
    cols = LOAN_COLS if full_rows else DUP_LIST_COLS
    rows = load_loan_rows_by_nos(tgt, dup_nos, cols, due_before, chunk)
    print(
        "dup_rows_loaded=%s total_elapsed=%.1fs"
        % (len(rows), time.time() - t0),
        flush=True,
    )
    return group_dup_loan_nos(rows)


def load_loans_for_dup_check(tgt, due_before: Optional[str] = None) -> List[dict]:
    """兼容旧接口：返回所有重复 loan_no 下的行。"""
    groups = load_dup_groups(tgt, due_before, full_rows=True)
    rows: List[dict] = []
    for part in groups.values():
        rows.extend(part)
    return rows


def group_dup_loan_nos(rows: List[dict]) -> Dict[str, List[dict]]:
    grouped: Dict[str, List[dict]] = defaultdict(list)
    for row in rows:
        ln = str(row.get("loan_no") or "").strip()
        if ln:
            grouped[ln].append(row)
    return {k: v for k, v in grouped.items() if len(v) > 1}


def collect_ext_sns_from_dup_groups(
    dup_groups: Dict[str, List[dict]], min_sn_len: int
) -> List[str]:
    ext_sns = set()
    for rows in dup_groups.values():
        for r in rows:
            suffix = app_no_market_suffix(str(r.get("application_no") or ""))
            if suffix and len(suffix) >= min_sn_len:
                ext_sns.add(suffix)
    return sorted(ext_sns)


def resolve_dup_keep_and_drop(
    loan_no: str,
    rows: List[dict],
    market_app_by_ext: Dict[str, str],
    min_sn_len: int,
) -> Tuple[List[dict], Optional[dict], str]:
    """判定重复 loan_no 下删哪些、留哪行。返回 (drop_rows, keep_row, reason)。"""
    wrong_core = [
        r for r in rows
        if is_wrong_application_no(str(r.get("application_no") or ""), loan_no, min_sn_len)
    ]
    good_core = [
        r for r in rows
        if not is_wrong_application_no(str(r.get("application_no") or ""), loan_no, min_sn_len)
    ]
    if wrong_core and good_core:
        return wrong_core, good_core[0], "core_sn_suffix"

    suffixes = [app_no_market_suffix(str(r.get("application_no") or "")) for r in rows]
    uniq_suffix = {s for s in suffixes if s}
    if len(uniq_suffix) == 1 and len(rows) > 1:
        ext_sn = next(iter(uniq_suffix))
        canonical = str(market_app_by_ext.get(ext_sn) or "").strip()
        if canonical:
            keep = None
            drop = []
            for r in rows:
                if str(r.get("application_no") or "").strip() == canonical:
                    keep = r
                else:
                    drop.append(r)
            if keep and drop:
                return drop, keep, "market_canonical"

        # 同源 market 长号、不同 appId 前缀：保留更长/更完整 application_no
        keep = max(
            rows,
            key=lambda r: len(str(r.get("application_no") or "")),
        )
        drop = [r for r in rows if row_pk(r) != row_pk(keep)]
        if drop:
            return drop, keep, "same_ext_keep_longest_app_no"

    return [], None, ""


def report_dup_loan_nos(
    dup_groups: Dict[str, List[dict]],
    min_sn_len: int,
    market_app_by_ext: Optional[Dict[str, str]] = None,
) -> Dict[str, int]:
    """打印同一 loan_no 多行明细，返回统计。"""
    market_app_by_ext = market_app_by_ext or {}
    stats = {
        "dup_loan_no_count": len(dup_groups),
        "dup_row_count": sum(len(v) for v in dup_groups.values()),
        "auto_fixable": 0,
        "need_manual": 0,
        "wrong_app_rows": 0,
    }
    print(
        "dup_loan_no_count=%s dup_row_count=%s"
        % (stats["dup_loan_no_count"], stats["dup_row_count"]),
        flush=True,
    )
    for loan_no, rows in sorted(dup_groups.items()):
        drop_rows, keep, reason = resolve_dup_keep_and_drop(
            loan_no, rows, market_app_by_ext, min_sn_len
        )
        if drop_rows and keep:
            tag = "auto_fixable"
            stats["auto_fixable"] += 1
        else:
            tag = "need_manual"
            stats["need_manual"] += 1
        wrong_core_n = sum(
            1 for r in rows
            if is_wrong_application_no(str(r.get("application_no") or ""), loan_no, min_sn_len)
        )
        stats["wrong_app_rows"] += wrong_core_n
        print(
            "\nloan_no=%s rows=%s pattern=%s reason=%s"
            % (loan_no, len(rows), tag, reason or "-"),
            flush=True,
        )
        for r in rows:
            app_no = str(r.get("application_no") or "")
            if is_wrong_application_no(app_no, loan_no, min_sn_len):
                row_tag = "wrong_core_sn_suffix"
            elif keep and row_pk(r) == row_pk(keep):
                row_tag = "keep"
            elif r in drop_rows:
                row_tag = "drop"
            else:
                row_tag = "other"
            print(
                "  [%s] application_no=%s status=%s due=%s"
                % (row_tag, app_no, r.get("status"), r.get("due_date")),
                flush=True,
            )
        if drop_rows and keep:
            ext_sn = app_no_market_suffix(str(keep.get("application_no") or ""))
            canonical = market_app_by_ext.get(ext_sn, "")
            print(
                "  => DELETE %s KEEP %s canonical=%s"
                % (
                    [str(w.get("application_no")) for w in drop_rows],
                    str(keep.get("application_no")),
                    canonical or "-",
                ),
                flush=True,
            )
    print(
        "\nsummary auto_fixable=%s need_manual=%s wrong_app_rows=%s"
        % (stats["auto_fixable"], stats["need_manual"], stats["wrong_app_rows"]),
        flush=True,
    )
    return stats


def build_dup_app_no_plan(
    dup_groups: Dict[str, List[dict]],
    min_sn_len: int,
    market_app_by_ext: Optional[Dict[str, str]] = None,
) -> Tuple[List[dict], Dict[str, int]]:
    """同一 loan_no 多行：删错 application_no，保留 market canonical 行。"""
    market_app_by_ext = market_app_by_ext or {}
    plan: List[dict] = []
    stats: Dict[str, int] = {}
    seen_pk = set()
    for loan_no, rows in sorted(dup_groups.items()):
        drop_rows, keep, reason = resolve_dup_keep_and_drop(
            loan_no, rows, market_app_by_ext, min_sn_len
        )
        if not drop_rows or not keep:
            stats["skip_dup_no_pattern"] = stats.get("skip_dup_no_pattern", 0) + len(rows)
            continue
        for w in drop_rows:
            pk = row_pk(w)
            if pk in seen_pk:
                continue
            seen_pk.add(pk)
            plan.append(
                {
                    "loan_no": loan_no,
                    "correct_loan_no": loan_no,
                    "application_no": pk[0],
                    "source_row": {},
                    "before": dict(w),
                    "mode": "drop_wrong_app_no",
                    "update_cols": [],
                    "keep_application_no": str(keep.get("application_no") or ""),
                    "dup_reason": reason,
                }
            )
            key = "drop_%s" % reason
            stats[key] = stats.get(key, 0) + 1
            stats["drop_wrong_app_no"] = stats.get("drop_wrong_app_no", 0) + 1
    return plan, stats


def row_pk(row: dict) -> Tuple[str, object, object]:
    return (
        str(row.get("application_no") or ""),
        row.get("period", 1),
        row.get("roll_sequence", 0),
    )


def load_all_target_loans(tgt, due_before: str, status: str) -> List[dict]:
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
    return [
        r for r in rows
        if is_long_loan_no(str(r.get("loan_no") or ""), min_sn_len)
    ]


def fetch_repay_plan_by_sns(src, core_sns: List[str]) -> Dict[str, dict]:
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


def fetch_market_app_no_by_ext_sn(src, ext_sns: List[str]) -> Dict[str, str]:
    """ext_sn(market applicationNo) -> 目标 application_no。"""
    uniq = sorted({str(x).strip() for x in ext_sns if x})
    out: Dict[str, str] = {}
    if not uniq:
        return out
    m = "ng_loan_market"
    for i in range(0, len(uniq), 2000):
        part = uniq[i : i + 2000]
        ph = ",".join(["%s"] * len(part))
        with src.cursor() as cur:
            cur.execute(
                f"""
                SELECT applicationNo AS ext_sn, `appId` AS app_id
                FROM {m}.application
                WHERE applicationNo IN ({ph})
                """,
                part,
            )
            for row in cur.fetchall():
                ext = str(row.get("ext_sn") or "").strip()
                app_no = mig.format_application_no(row.get("app_id"), ext)
                if ext and app_no:
                    out[ext] = app_no
    return out


def fetch_short_loan_by_app_nos(
    tgt, app_nos: List[str], min_sn_len: int
) -> Dict[str, str]:
    """application_no -> 短号 loan_no（目标库已有行，不限 status）。"""
    uniq = sorted({str(x).strip() for x in app_nos if x})
    out: Dict[str, str] = {}
    if not uniq:
        return out
    for i in range(0, len(uniq), 2000):
        part = uniq[i : i + 2000]
        ph = ",".join(["%s"] * len(part))
        with tgt.cursor() as cur:
            cur.execute(
                f"SELECT loan_no, application_no FROM loan WHERE application_no IN ({ph})",
                part,
            )
            for row in cur.fetchall():
                app_no = str(row.get("application_no") or "").strip()
                ln = str(row.get("loan_no") or "").strip()
                parsed = parse_loan_middle(ln)
                if app_no and parsed and parsed[1] < min_sn_len:
                    out[app_no] = ln
    return out


def fetch_core_application_by_ext_sn(src, ext_sns: List[str]) -> Dict[str, str]:
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


def fetch_source_for_long(
    src,
    ext_sns: List[str],
    target_app_by_ext: Dict[str, str],
    market_app_by_ext: Dict[str, str],
    min_sn_len: int,
) -> Tuple[Dict[str, dict], Dict[str, int]]:
    """ext_sn -> 目标形态 loan 行（application + repay_plan）。"""
    uniq = sorted({str(x).strip() for x in ext_sns if x})
    stats: Dict[str, int] = {"ext_sn": len(uniq)}
    core_by_ext = fetch_core_application_by_ext_sn(src, uniq)
    stats["core_hit"] = len(core_by_ext)
    core_sns: List[str] = []
    ext_to_pair: Dict[str, Tuple[str, str]] = {}
    for ext_sn in uniq:
        core_sn = core_by_ext.get(ext_sn, "")
        if not core_sn or not is_plausible_core_sn(core_sn, ext_sn, min_sn_len):
            stats["skip_no_core"] = stats.get("skip_no_core", 0) + 1
            continue
        app_no = target_app_by_ext.get(ext_sn, "") or market_app_by_ext.get(ext_sn, "")
        if not app_no:
            stats["skip_no_app_no"] = stats.get("skip_no_app_no", 0) + 1
            continue
        ext_to_pair[ext_sn] = (core_sn, app_no)
        core_sns.append(core_sn)
    repay_plans = fetch_repay_plan_by_sns(src, core_sns)
    stats["repay_hit"] = len(repay_plans)
    out: Dict[str, dict] = {}
    for ext_sn, (core_sn, app_no) in ext_to_pair.items():
        rp = repay_plans.get(core_sn)
        if not rp:
            stats["skip_no_repay"] = stats.get("skip_no_repay", 0) + 1
            continue
        out[ext_sn] = mig._build_loan_row(rp, app_no)
    stats["source_hit"] = len(out)
    return out, stats


def diff_update_cols(before: dict, source: dict) -> List[str]:
    """返回 before 与 source 不一致、需要写入的列（不含 loan_no）。"""
    cols: List[str] = []
    for col in UPDATE_COLS:
        tv = before.get(col)
        sv = source.get(col)
        if tv is None and sv is None:
            continue
        if str(tv) != str(sv):
            cols.append(col)
    return cols


def row_needs_sync(target: dict, source: dict) -> bool:
    return bool(diff_update_cols(target, source))


def _build_after(before: dict, source_row: dict, update_cols: List[str], loan_no: str) -> dict:
    after = dict(before)
    for col in update_cols:
        after[col] = source_row[col]
    after["loan_no"] = loan_no
    return after


def _plan_audit_row(plan_row: dict) -> dict:
    return {
        "wrong_loan_no": plan_row["loan_no"],
        "correct_loan_no": plan_row.get("correct_loan_no") or plan_row["loan_no"],
        "application_no": plan_row.get("application_no") or "",
        "sync_mode": plan_row.get("mode") or "",
    }


def build_plan(
    candidates: List[dict],
    source_by_ext: Dict[str, dict],
    repay_by_core_sn: Dict[str, dict],
    short_loan_by_app: Dict[str, str],
    target_app_by_ext: Dict[str, str],
    min_sn_len: int,
) -> Tuple[List[dict], Dict[str, int]]:
    plan: List[dict] = []
    stats: Dict[str, int] = {}
    for row in candidates:
        loan_no = str(row.get("loan_no") or "")
        app_no = str(row.get("application_no") or "")

        if is_long_loan_no(loan_no, min_sn_len):
            ext_sn = extract_market_no(app_no, loan_no)
            if not ext_sn:
                stats["skip_no_ext_sn"] = stats.get("skip_no_ext_sn", 0) + 1
                continue
            src_row = source_by_ext.get(ext_sn)
            if src_row:
                # 始终用目标库当前行的 application_no，不用源库 market 拼出来的 appId
                src_row = dict(src_row)
                src_row["application_no"] = app_no
            if not src_row:
                # 源库无 repay_plan：若目标库已有短号行，仅删长号残留
                app_key = (
                    str(row.get("application_no") or "").strip()
                    or target_app_by_ext.get(ext_sn, "")
                )
                correct = short_loan_by_app.get(app_key, "") if app_key else ""
                if correct and correct != loan_no:
                    plan.append(
                        {
                            "loan_no": loan_no,
                            "correct_loan_no": correct,
                            "application_no": app_key,
                            "source_row": {},
                            "before": dict(row),
                            "mode": "drop_long_only",
                            "update_cols": [],
                            "ext_sn": ext_sn,
                        }
                    )
                    stats["drop_long_only"] = stats.get("drop_long_only", 0) + 1
                else:
                    stats["skip_no_source_long"] = stats.get("skip_no_source_long", 0) + 1
                continue
            correct = str(src_row["loan_no"])
            update_cols = diff_update_cols(row, src_row)
            if correct == loan_no:
                if not update_cols:
                    stats["skip_already_ok"] = stats.get("skip_already_ok", 0) + 1
                    continue
                mode = "sync_status"
            else:
                mode = "rekey_long"
        else:
            parsed = parse_loan_middle(loan_no)
            if not parsed:
                stats["skip_bad_loan_no"] = stats.get("skip_bad_loan_no", 0) + 1
                continue
            core_sn = parsed[0]
            if not is_plausible_core_sn(core_sn, "", min_sn_len):
                stats["skip_bad_core_sn"] = stats.get("skip_bad_core_sn", 0) + 1
                continue
            rp = repay_by_core_sn.get(core_sn)
            if not rp:
                stats["skip_no_repay_plan"] = stats.get("skip_no_repay_plan", 0) + 1
                continue
            src_row = mig._build_loan_row(rp, app_no)
            correct = loan_no
            update_cols = diff_update_cols(row, src_row)
            if not update_cols:
                stats["skip_already_ok"] = stats.get("skip_already_ok", 0) + 1
                continue
            mode = "sync_status"

        if mode == "rekey_long":
            update_cols = diff_update_cols(row, src_row)

        plan.append(
            {
                "loan_no": loan_no,
                "correct_loan_no": correct,
                "application_no": app_no,
                "source_row": src_row,
                "before": dict(row),
                "mode": mode,
                "update_cols": update_cols,
                "ext_sn": extract_market_no(app_no, loan_no) if mode == "rekey_long" else "",
            }
        )
        stats[mode] = stats.get(mode, 0) + 1
    return plan, stats


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


def _delete_loan_row(tgt, row: dict) -> int:
    """按主键 (application_no, period, roll_sequence) 删除单行。"""
    with tgt.cursor() as cur:
        cur.execute(
            """
            DELETE FROM loan
            WHERE application_no=%s AND period=%s AND roll_sequence=%s
            """,
            (
                row["application_no"],
                row.get("period", 1),
                row.get("roll_sequence", 0),
            ),
        )
        return cur.rowcount


def _loan_row_exists(tgt, row: dict) -> bool:
    with tgt.cursor() as cur:
        cur.execute(
            """
            SELECT 1 FROM loan
            WHERE application_no=%s AND period=%s AND roll_sequence=%s
            LIMIT 1
            """,
            (
                row["application_no"],
                row.get("period", 1),
                row.get("roll_sequence", 0),
            ),
        )
        return cur.fetchone() is not None


def _delete_loan(tgt, loan_no: str) -> int:
    with tgt.cursor() as cur:
        cur.execute("DELETE FROM loan WHERE loan_no=%s", (loan_no,))
        return cur.rowcount


def _rekey_update(
    tgt, loan_no: str, correct: str, source_row: dict, cols: List[str]
) -> int:
    if cols:
        vals = [source_row[c] for c in cols]
        sql = "UPDATE loan SET loan_no=%%s, %s WHERE loan_no=%%s" % _build_set_sql(cols)
        args = [correct] + vals + [loan_no]
    else:
        sql = "UPDATE loan SET loan_no=%s WHERE loan_no=%s"
        args = [correct, loan_no]
    with tgt.cursor() as cur:
        cur.execute(sql, args)
        return cur.rowcount


def apply_one_loan(
    tgt,
    plan_row: dict,
    dry_run: bool,
    audit: RepairAuditLog,
    row_audit: Optional[RowChangeAuditLog],
    tracker: Optional[CommitTracker],
) -> str:
    loan_no = plan_row["loan_no"]
    mode = plan_row["mode"]
    source_row = plan_row["source_row"]
    correct = plan_row["correct_loan_no"]
    before = plan_row["before"]
    want_app_no = str(
        plan_row.get("application_no") or before.get("application_no") or ""
    ).strip()
    source_for_row = dict(source_row) if source_row else {}
    if want_app_no and source_for_row:
        source_for_row["application_no"] = want_app_no
    update_cols = diff_update_cols(before, source_for_row) if source_for_row else []
    audit_row = _plan_audit_row(plan_row)
    cols_hint = ",".join(update_cols) if update_cols else "-"

    if mode == "drop_wrong_app_no":
        keep = plan_row.get("keep_application_no") or ""
        if dry_run:
            if row_audit:
                row_audit.record_deleted("would_delete_wrong_app_no", before)
            audit.record(
                "would_drop_wrong_app_no",
                audit_row,
                "keep=%s" % keep,
            )
            return "ok"
        try:
            if not _delete_loan_row(tgt, before):
                tgt.rollback()
                audit.record("skip", audit_row, "delete_wrong_app_no_failed")
                return "skip"
            if row_audit:
                row_audit.record_deleted("delete_wrong_app_no", before)
        except pymysql.err.IntegrityError as exc:
            tgt.rollback()
            audit.record("skip", audit_row, "drop_wrong_app_no:%s" % exc)
            return "skip"
        audit.record("drop_wrong_app_no", audit_row, "keep=%s" % keep)
    elif mode == "drop_long_only":
        if dry_run:
            if row_audit:
                row_audit.record_deleted("would_delete_long", before)
            audit.record("would_drop_long_only", audit_row, "drop_long_only")
            return "ok"
        try:
            if not _delete_loan_row(tgt, before):
                tgt.rollback()
                audit.record("skip", audit_row, "delete_long_failed")
                return "skip"
            if row_audit:
                row_audit.record_deleted("delete_long", before)
        except pymysql.err.IntegrityError as exc:
            tgt.rollback()
            audit.record("skip", audit_row, "drop_long_only:%s" % exc)
            return "skip"
        audit.record("drop_long_only", audit_row, "drop_long_only")
    elif mode == "rekey_long":
        if loan_exists(tgt, correct) and correct != loan_no:
            before_correct = fetch_loan_row(tgt, correct) or {}
            correct_app_no = str(before_correct.get("application_no") or "").strip()
            app_mismatch = bool(
                want_app_no and correct_app_no and want_app_no != correct_app_no
            )
            if app_mismatch:
                # 短号行 application_no 与待修复长号行不一致：删错误短号，原地 rekey 长号
                update_cols_rekey = diff_update_cols(before, source_for_row)
                hint = ",".join(update_cols_rekey) if update_cols_rekey else "-"
                after = _build_after(before, source_for_row, update_cols_rekey, correct)
                if dry_run:
                    if row_audit:
                        row_audit.record_deleted("would_delete_wrong_short", before_correct)
                        row_audit.record_modified("would_rekey_long", before, after)
                    audit.record(
                        "would_rekey_keep_app_no",
                        audit_row,
                        "drop_short_app=%s rekey:%s" % (correct_app_no, hint),
                    )
                    return "ok"
                try:
                    if _loan_row_exists(tgt, before_correct):
                        if not _delete_loan_row(tgt, before_correct):
                            tgt.rollback()
                            audit.record("skip", audit_row, "delete_wrong_short_failed")
                            return "skip"
                        if row_audit:
                            row_audit.record_deleted("delete_wrong_short", before_correct)
                    if not _rekey_update(
                        tgt, loan_no, correct, source_for_row, update_cols_rekey
                    ):
                        tgt.rollback()
                        audit.record("skip", audit_row, "rekey_after_drop_short_failed")
                        return "skip"
                except pymysql.err.IntegrityError as exc:
                    tgt.rollback()
                    audit.record("skip", audit_row, "rekey_keep_app_no:%s" % exc)
                    return "skip"
                if row_audit:
                    row_audit.record_modified("rekey_long", before, after)
                audit.record(
                    "rekey_keep_app_no",
                    audit_row,
                    "drop_short_app=%s rekey:%s" % (correct_app_no, hint),
                )
            else:
                cols_on_correct = diff_update_cols(before_correct, source_for_row)
                hint = ",".join(cols_on_correct) if cols_on_correct else "-"
                if dry_run:
                    if row_audit:
                        row_audit.record_deleted("would_delete_long", before)
                        if cols_on_correct:
                            row_audit.record_modified(
                                "would_sync_correct",
                                before_correct,
                                _build_after(
                                    before_correct,
                                    source_for_row,
                                    cols_on_correct,
                                    correct,
                                ),
                            )
                    audit.record("would_drop_long", audit_row, "drop_long:%s" % hint)
                    return "ok"
                try:
                    if _loan_row_exists(tgt, before):
                        if not _delete_loan_row(tgt, before):
                            tgt.rollback()
                            audit.record("skip", audit_row, "delete_long_failed")
                            return "skip"
                        if row_audit:
                            row_audit.record_deleted("delete_long", before)
                    if cols_on_correct:
                        if not _sync_update(tgt, correct, source_for_row, cols_on_correct):
                            tgt.rollback()
                            audit.record("skip", audit_row, "sync_correct_no_row")
                            return "skip"
                        if row_audit:
                            row_audit.record_modified(
                                "sync_correct",
                                before_correct,
                                _build_after(
                                    before_correct,
                                    source_for_row,
                                    cols_on_correct,
                                    correct,
                                ),
                            )
                except pymysql.err.IntegrityError as exc:
                    tgt.rollback()
                    audit.record("skip", audit_row, "drop_long_duplicate:%s" % exc)
                    return "skip"
                audit.record("drop_long", audit_row, "drop_long:%s" % hint)
        else:
            update_cols = diff_update_cols(before, source_for_row)
            cols_hint = ",".join(update_cols) if update_cols else "-"
            after = _build_after(before, source_for_row, update_cols, correct)
            if dry_run:
                if row_audit:
                    row_audit.record_modified("would_rekey_long", before, after)
                audit.record("would_rekey_long", audit_row, "rekey_long:%s" % cols_hint)
                return "ok"
            try:
                if not _rekey_update(
                    tgt, loan_no, correct, source_for_row, update_cols
                ):
                    tgt.rollback()
                    audit.record("skip", audit_row, "rekey_no_row")
                    return "skip"
            except pymysql.err.IntegrityError as exc:
                tgt.rollback()
                audit.record("skip", audit_row, "rekey_duplicate:%s" % exc)
                return "skip"
            if row_audit:
                row_audit.record_modified("rekey_long", before, after)
            audit.record("rekey_long", audit_row, "rekey_long:%s" % cols_hint)
    else:
        if not update_cols:
            audit.record("skip", audit_row, "already_ok")
            return "skip"
        after = _build_after(before, source_for_row, update_cols, loan_no)
        if dry_run:
            if row_audit:
                row_audit.record_modified("would_sync_status", before, after)
            audit.record("would_sync_status", audit_row, "sync:%s" % cols_hint)
            return "ok"
        try:
            if not _sync_update(tgt, loan_no, source_for_row, update_cols):
                tgt.rollback()
                audit.record("skip", audit_row, "sync_no_row")
                return "skip"
        except pymysql.err.IntegrityError as exc:
            tgt.rollback()
            audit.record("skip", audit_row, "sync_duplicate:%s" % exc)
            return "skip"
        if row_audit:
            row_audit.record_modified("sync_status", before, after)
        audit.record("sync_status", audit_row, "sync:%s" % cols_hint)

    if tracker:
        tracker.note_write()
    elif not dry_run:
        tgt.commit()
    return "ok"


def split_plan_chunks(plan: List[dict], workers: int) -> List[List[dict]]:
    n = max(1, int(workers))
    if not plan:
        return []
    chunks: List[List[dict]] = [[] for _ in range(n)]
    for row in plan:
        key = str(row.get("loan_no") or "")
        idx = int(hashlib.md5(key.encode("utf-8")).hexdigest(), 16) % n
        chunks[idx].append(row)
    return chunks


def _worker_repair_log_path(base: str, worker_id: int) -> str:
    if not base:
        return ""
    p = Path(base)
    return str(p.with_name("%s.w%s%s" % (p.stem, worker_id, p.suffix or ".csv")))


def run_apply_chunk(
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
            lambda r=row: apply_one_loan(tgt, r, dry_run, audit, row_audit, tracker),
            "apply %s" % row["loan_no"],
        )
        if result == "ok":
            ok += 1
        else:
            skip += 1
        if i % max(1, log_every) == 0:
            print(
                "%sprogress ok=%s skip=%s last=%s mode=%s"
                % (prefix, ok, skip, row["loan_no"], row.get("mode", "")),
                flush=True,
            )
    tracker.flush()
    if not dry_run and tracker.pending <= 0:
        try:
            tgt.commit()
        except Exception:
            pass
    return ok, skip


def apply_worker_run(spec: dict) -> Tuple[int, int]:
    worker_id = spec["worker_id"]
    workers = spec["workers"]
    label = "[%s/%s] " % (worker_id, workers)
    chunk = spec.get("plan_chunk") or []
    if not chunk:
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
            "%sstart rows=%s first=%s last=%s"
            % (label, len(chunk), chunk[0]["loan_no"], chunk[-1]["loan_no"]),
            flush=True,
        )
        ok, skip = run_apply_chunk(
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


def run_apply_parallel(
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
    ctx = multiprocessing.get_context("spawn")
    with ctx.Pool(processes=len(specs)) as pool:
        results = pool.map(apply_worker_run, specs)
    total_ok = sum(r[0] for r in results)
    total_skip = sum(r[1] for r in results)
    print("parallel done workers=%s ok=%s skip=%s" % (len(specs), total_ok, total_skip), flush=True)
    return total_ok, total_skip


    return 0 if ok or skip else 1


def run_dup_only(cfg: Dict[str, str], args, dry_run: bool) -> int:
    """仅处理同一 loan_no 多行：删 application_no 后缀误用 core sn 的错行。"""
    dup_due_before = (args.dup_due_before or "").strip() or None
    repair_log = args.repair_log or (
        "/tmp/repair_dup_loan_no_%s.csv" % datetime.now().strftime("%Y%m%d_%H%M%S")
    )
    audit = RepairAuditLog(
        repair_log if not args.no_repair_log else None,
        enabled=not args.no_repair_log and not args.plan_only,
    )
    row_audit = RowChangeAuditLog(
        repair_log, enabled=not args.no_repair_log and not args.plan_only
    )
    scope = dup_due_before or "ALL"
    print(
        "dup-only scope=due_date<%s dry_run=%s min_sn_len=%s"
        % (scope, dry_run, args.min_sn_len),
        flush=True,
    )
    tgt = connect_target(cfg)
    src = connect_source(cfg)
    run_t0 = time.time()
    try:
        dup_rows = exec_with_retry(
            tgt,
            lambda: load_dup_groups(tgt, dup_due_before, full_rows=True),
            "load dup groups",
        )
        print("dup_scan_groups=%s" % len(dup_rows), flush=True)
        dup_groups = dup_rows
        if not dup_groups:
            print("no duplicate loan_no found", flush=True)
            return 0
        dup_ext_sns = collect_ext_sns_from_dup_groups(dup_groups, args.min_sn_len)
        dup_market = fetch_market_app_no_by_ext_sn(src, dup_ext_sns)
        print("market_canonical_hits=%s" % len(dup_market), flush=True)
        report_dup_loan_nos(dup_groups, args.min_sn_len, dup_market)
        dup_plan, dup_stats = build_dup_app_no_plan(
            dup_groups, args.min_sn_len, dup_market
        )
        print(
            "dup_plan=%s dup_stats=%s" % (len(dup_plan), dup_stats),
            flush=True,
        )
        for row in dup_plan[:20]:
            print(
                "  drop loan=%s app=%s keep=%s reason=%s"
                % (
                    row["loan_no"],
                    row["before"].get("application_no"),
                    row.get("keep_application_no"),
                    row.get("dup_reason"),
                ),
                flush=True,
            )
        if len(dup_plan) > 20:
            print("  ... and %s more" % (len(dup_plan) - 20), flush=True)
        if args.plan_only:
            return 0 if dup_plan else 1
        if not dup_plan:
            return 0
        if args.work_limit > 0:
            dup_plan = dup_plan[: args.work_limit]
        ok = skip = 0
        ok, skip = run_apply_chunk(
            tgt,
            dup_plan,
            dry_run,
            audit,
            row_audit,
            args.commit_every,
            args.work_limit,
            args.log_every,
        )
        print(
            "finished dup-only plan=%s ok=%s skip=%s elapsed=%.1fs log=%s"
            % (len(dup_plan), ok, skip, time.time() - run_t0, repair_log),
            flush=True,
        )
        return 0 if ok or skip else 1
    finally:
        audit.close()
        row_audit.close()
        tgt.close()
        src.close()


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(
        description="Fix long loan_no + sync status from ng_loan_core.repay_plan"
    )
    p.add_argument("--env", default=str(HERE / "ng_migration.env"))
    p.add_argument("--apply", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--due-before", default="2026-07-01", help="sync 用：due_date < 该日期")
    p.add_argument(
        "--dup-due-before",
        default="",
        help="dup 用：due_date < 该日期；默认空=扫全表所有 loan",
    )
    p.add_argument("--status", default="20")
    p.add_argument("--min-sn-len", type=int, default=15)
    p.add_argument("--workers", type=int, default=1)
    p.add_argument("--commit-every", type=int, default=50)
    p.add_argument("--work-limit", type=int, default=0)
    p.add_argument("--log-every", type=int, default=100)
    p.add_argument("--repair-log", default="")
    p.add_argument("--no-repair-log", action="store_true")
    p.add_argument("--long-only", action="store_true")
    p.add_argument(
        "--no-fix-dup-app-no",
        action="store_true",
        help="不删重复 loan_no 下 application_no 后缀为 core sn 的错行",
    )
    p.add_argument(
        "--list-dup",
        action="store_true",
        help="仅列出同一 loan_no 多行（不写库）；默认扫全表",
    )
    p.add_argument(
        "--dup-only",
        action="store_true",
        help="只删重复 loan_no 下错挂 application_no 的行，不做 status/loan_no 同步",
    )
    p.add_argument("--plan-only", action="store_true")
    args = p.parse_args(argv)
    if args.apply and args.dry_run:
        p.error("use either --apply or --dry-run")
    if args.list_dup and args.apply:
        p.error("--list-dup 与 --apply 不能同时使用")
    if args.dup_only and args.long_only:
        p.error("--dup-only 与 --long-only 不能同时使用")
    dry_run = not args.apply

    cfg = load_env(Path(args.env))

    dup_due_before = (args.dup_due_before or "").strip() or None

    if args.dup_only:
        return run_dup_only(cfg, args, dry_run)

    if args.list_dup:
        tgt = connect_target(cfg)
        src = connect_source(cfg)
        try:
            scope = dup_due_before or "ALL"
            print("list-dup scope=%s (all status)" % scope, flush=True)
            dup_groups = exec_with_retry(
                tgt,
                lambda: load_dup_groups(tgt, dup_due_before, full_rows=False),
                "load dup groups",
            )
            if not dup_groups:
                print("no duplicate loan_no found", flush=True)
                return 0
            dup_ext_sns = collect_ext_sns_from_dup_groups(dup_groups, args.min_sn_len)
            market = fetch_market_app_no_by_ext_sn(src, dup_ext_sns)
            print("market_canonical_hits=%s" % len(market), flush=True)
            report_dup_loan_nos(dup_groups, args.min_sn_len, market)
            return 0
        finally:
            tgt.close()
            src.close()

    env_path = str(Path(args.env).resolve())
    repair_log = args.repair_log or (
        "/tmp/repair_loan_status20_%s.csv" % datetime.now().strftime("%Y%m%d_%H%M%S")
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
            "start sync_due_before=%s status=%s dup_scope=%s workers=%s dry_run=%s"
            % (
                args.due_before,
                args.status,
                dup_due_before or "ALL",
                args.workers,
                dry_run,
            ),
            flush=True,
        )
        run_t0 = time.time()

        dup_plan: List[dict] = []
        dup_stats: Dict[str, int] = {}
        dup_skip_pks = set()
        if not args.no_fix_dup_app_no:
            dup_groups = exec_with_retry(
                tgt,
                lambda: load_dup_groups(tgt, dup_due_before, full_rows=True),
                "load dup groups",
            )
            print("dup_scan_groups=%s" % len(dup_groups), flush=True)
            dup_ext_sns = collect_ext_sns_from_dup_groups(dup_groups, args.min_sn_len)
            dup_market = fetch_market_app_no_by_ext_sn(src, dup_ext_sns)
            dup_plan, dup_stats = build_dup_app_no_plan(
                dup_groups, args.min_sn_len, dup_market
            )
            dup_skip_pks = {row_pk(p["before"]) for p in dup_plan}
            print(
                "dup_plan=%s dup_stats=%s scope=%s"
                % (len(dup_plan), dup_stats, dup_due_before or "ALL"),
                flush=True,
            )
            for row in dup_plan[:10]:
                print(
                    "  dup %s drop_app=%s keep_app=%s"
                    % (
                        row["loan_no"],
                        row["before"].get("application_no"),
                        row.get("keep_application_no"),
                    ),
                    flush=True,
                )

        all_loans = exec_with_retry(
            tgt,
            lambda: load_all_target_loans(tgt, args.due_before, args.status),
            "load loans",
        )
        print("target_loans=%s status=%s" % (len(all_loans), args.status), flush=True)
        if not all_loans and not dup_plan:
            return 0

        candidates = (
            filter_long_candidates(all_loans, args.min_sn_len)
            if args.long_only
            else all_loans
        )
        if dup_skip_pks:
            candidates = [r for r in candidates if row_pk(r) not in dup_skip_pks]
        long_n = sum(
            1 for r in candidates
            if is_long_loan_no(str(r.get("loan_no") or ""), args.min_sn_len)
        )
        print(
            "candidates=%s long=%s short=%s"
            % (len(candidates), long_n, len(candidates) - long_n),
            flush=True,
        )

        ext_sns: List[str] = []
        core_sns: List[str] = []
        target_app_by_ext: Dict[str, str] = {}
        long_app_nos: List[str] = []
        for r in candidates:
            loan_no = str(r.get("loan_no") or "")
            app_no = str(r.get("application_no") or "").strip()
            if is_long_loan_no(loan_no, args.min_sn_len):
                ext = extract_market_no(app_no, loan_no)
                if ext:
                    ext_sns.append(ext)
                    if app_no:
                        target_app_by_ext[ext] = app_no
                        long_app_nos.append(app_no)
            else:
                parsed = parse_loan_middle(loan_no)
                if parsed and is_plausible_core_sn(parsed[0], "", args.min_sn_len):
                    core_sns.append(parsed[0])

        uniq_ext = sorted(set(ext_sns))
        print(
            "load source long ext_sn=%s short core_sn=%s ..."
            % (len(uniq_ext), len(set(core_sns))),
            flush=True,
        )
        t0 = time.time()
        market_app_by_ext = fetch_market_app_no_by_ext_sn(src, uniq_ext)
        for ext, app_no in market_app_by_ext.items():
            target_app_by_ext.setdefault(ext, app_no)
        source_by_ext, source_stats = fetch_source_for_long(
            src, uniq_ext, target_app_by_ext, market_app_by_ext, args.min_sn_len
        )
        repay_by_core_sn = fetch_repay_plan_by_sns(src, sorted(set(core_sns)))
        short_loan_by_app = fetch_short_loan_by_app_nos(
            tgt, list(set(long_app_nos) | set(target_app_by_ext.values())), args.min_sn_len
        )
        print(
            "source_stats=%s short_in_target=%s short_repay_hits=%s elapsed=%.1fs"
            % (source_stats, len(short_loan_by_app), len(repay_by_core_sn), time.time() - t0),
            flush=True,
        )

        plan, plan_stats = build_plan(
            candidates,
            source_by_ext,
            repay_by_core_sn,
            short_loan_by_app,
            target_app_by_ext,
            args.min_sn_len,
        )
        if dup_plan:
            plan = dup_plan + plan
            for k, v in dup_stats.items():
                plan_stats[k] = plan_stats.get(k, 0) + v
        if args.work_limit:
            plan = plan[: args.work_limit]
        print("repair_plan=%s plan_stats=%s" % (len(plan), plan_stats), flush=True)
        for row in plan[:15]:
            print(
                "  %s mode=%s cols=%s status %s=>%s"
                % (
                    row["loan_no"],
                    row["mode"],
                    ",".join(row.get("update_cols") or []) or "-",
                    row["before"].get("status"),
                    (row.get("source_row") or {}).get("status"),
                ),
                flush=True,
            )
        if len(plan) > 15:
            print("  ... and %s more" % (len(plan) - 15), flush=True)
        if args.plan_only:
            return 0 if plan else 1
        if not plan:
            return 0

        if args.workers > 1:
            audit.close()
            row_audit.close()
            ok, skip = run_apply_parallel(
                plan, args.workers, env_path, dry_run,
                args.work_limit, args.log_every, args.commit_every,
                repair_log if not args.no_repair_log else "", args.no_repair_log,
            )
        else:
            ok = skip = 0
            ok, skip = run_apply_chunk(
                tgt, plan, dry_run, audit, row_audit,
                args.commit_every, args.work_limit, args.log_every,
            )

        print(
            "finished plan=%s ok=%s skip=%s elapsed=%.1fs log=%s"
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
