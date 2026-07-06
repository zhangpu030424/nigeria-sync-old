#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""修复目标库中 due_date 已到期且 status=20、但 loan_no 中间段仍为 market 长号的 loan 行。

范围（默认）:
  due_date < 2026-07-01  AND  status = 20
  loan_no 形如 ng-178178196712036899-01000（中间段 >= --min-sn-len）

逻辑:
  1. 分页扫目标库符合条件的 loan
  2. 从 application_no 提取 market 号，批量查源库 repay_plan
  3. 用源库拼出正确 loan_no / application_no 及 status 相关字段
     源库无 repay_plan 时回退：目标 application.sn 仅修 loan_no（rekey_only）
  4. 若正确 loan_no 已存在 → 用源数据 UPDATE 正确行，DELETE 长号行
     否则 → UPDATE 长号行的 loan_no 及全部业务字段

Usage:
  python3 repair_loan_status20_from_source.py --env ./ng_migration.env --dry-run --plan-only
  python3 repair_loan_status20_from_source.py --env ./ng_migration.env --dry-run --work-limit 10
  python3 repair_loan_status20_from_source.py --env ./ng_migration.env --apply --commit-every 20
"""
import argparse
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
    exec_with_retry,
    fetch_loan_row,
    loan_exists,
)

HERE = Path(__file__).resolve().parent
LOAN_COLS = mig.LOAN_INSERT_COLS
SYNC_COLS = [c for c in LOAN_COLS if c != "loan_no"]
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


def scan_target_batch(
    tgt,
    due_before: str,
    status: str,
    after: str,
    limit: int,
) -> List[dict]:
    sql = """
        SELECT loan_no, application_no, due_date, status
        FROM loan
        WHERE due_date < %s AND status = %s AND loan_no > %s
        ORDER BY loan_no ASC
        LIMIT %s
    """
    with tgt.cursor() as cur:
        cur.execute(sql, (due_before, status, after or "", limit))
        return list(cur.fetchall())


def fetch_target_sn_by_application_nos(
    tgt, application_nos: List[str]
) -> Dict[str, str]:
    """application_no -> core sn（目标库 application 表）。"""
    uniq = sorted({str(x).strip() for x in application_nos if x})
    out: Dict[str, str] = {}
    if not uniq:
        return out
    for i in range(0, len(uniq), 500):
        part = uniq[i : i + 500]
        ph = ",".join(["%s"] * len(part))
        with tgt.cursor() as cur:
            cur.execute(
                f"""
                SELECT application_no, sn
                FROM application
                WHERE application_no IN ({ph})
                  AND sn IS NOT NULL AND sn <> ''
                """,
                part,
            )
            for row in cur.fetchall():
                app_no = str(row["application_no"]).strip()
                sn = str(row["sn"]).strip()
                if app_no and sn:
                    out[app_no] = sn
    return out


def fetch_source_market_meta(
    src, market_nos: List[str]
) -> Dict[str, Tuple[str, str]]:
    """market applicationNo -> (core_sn, application_no)。"""
    if not market_nos:
        return {}
    uniq = sorted({str(x).strip() for x in market_nos if x})
    out: Dict[str, Tuple[str, str]] = {}
    m, c = "ng_loan_market", "ng_loan_core"
    for i in range(0, len(uniq), 500):
        part = uniq[i : i + 500]
        ph = ",".join(["%s"] * len(part))
        with src.cursor() as cur:
            cur.execute(
                f"""
                SELECT a.applicationNo AS market_no, a.`appId` AS app_id, ca.sn AS core_sn
                FROM {m}.application a
                INNER JOIN {c}.application ca ON ca.ext_sn = a.applicationNo
                WHERE a.applicationNo IN ({ph})
                  AND ca.sn IS NOT NULL AND ca.sn <> ''
                """,
                part,
            )
            rows = list(cur.fetchall())
        for row in rows:
            market_no = str(row["market_no"]).strip()
            core_sn = str(row["core_sn"]).strip()
            app_no = mig.format_application_no(row.get("app_id"), market_no)
            if market_no and core_sn and app_no:
                out[market_no] = (core_sn, app_no)
    return out


def fetch_source_loans_by_core_map(
    src, sn_to_app_no: Dict[str, str]
) -> Dict[str, dict]:
    """core_sn -> 源库 loan 行。"""
    if not sn_to_app_no:
        return {}
    loan_rows = mig._fetch_loan_rows_from_source(src, sn_to_app_no)
    out: Dict[str, dict] = {}
    for row in loan_rows:
        app_no = str(row.get("application_no") or "")
        m = APP_NO_RE.match(app_no)
        if m:
            out[m.group(1)] = row
        loan_no = str(row.get("loan_no") or "")
        parsed = parse_loan_middle(loan_no)
        if parsed:
            out.setdefault(parsed[0], row)
    return out


def fetch_source_loans_by_market_nos(
    src, market_nos: List[str]
) -> Tuple[Dict[str, dict], Dict[str, str]]:
    """market applicationNo -> loan 行；以及未命中原因 market_no -> reason。"""
    if not market_nos:
        return {}, {}
    uniq = sorted({str(x).strip() for x in market_nos if x})
    meta = fetch_source_market_meta(src, uniq)
    sn_to_app_no = {core: app for core, app in meta.values()}
    by_market = fetch_source_loans_by_core_map(src, sn_to_app_no)
    miss_reason: Dict[str, str] = {}
    for market_no in uniq:
        if market_no in by_market:
            continue
        if market_no not in meta:
            miss_reason[market_no] = "no_market_app"
        else:
            miss_reason[market_no] = "no_repay_plan"
    return by_market, miss_reason


def build_rekey_row_from_target(
    tgt, wrong_loan_no: str, application_no: str, core_sn: str
) -> Optional[dict]:
    """源库无 repay_plan 时：仅按目标行 + core_sn 拼正确 loan_no，status 等保持目标现状。"""
    before = fetch_loan_row(tgt, wrong_loan_no)
    if not before:
        return None
    correct = mig.format_loan_no(core_sn, 1, 0)
    if not correct:
        return None
    row = dict(before)
    row["loan_no"] = correct
    if application_no:
        row["application_no"] = application_no
    return row


def resolve_loan_row(
    tgt,
    src,
    wrong_loan_no: str,
    application_no: str,
    market_no: str,
    source_map: Dict[str, dict],
    miss_reason: Dict[str, str],
    target_sn_map: Dict[str, str],
) -> Tuple[Optional[dict], str]:
    src_row = source_map.get(market_no)
    if src_row:
        return src_row, "source"

    app_no = str(application_no or "").strip()
    core_sn = target_sn_map.get(app_no, "")
    if not core_sn and app_no:
        core_sn = fetch_target_sn_by_application_nos(tgt, [app_no]).get(app_no, "")
        if core_sn:
            target_sn_map[app_no] = core_sn

    if core_sn and app_no:
        src_by_core = fetch_source_loans_by_core_map(src, {core_sn: app_no})
        src_row = src_by_core.get(market_no) or src_by_core.get(core_sn)
        if src_row:
            row = dict(src_row)
            if row.get("application_no") != app_no:
                row["application_no"] = app_no
            return row, "source_via_target_sn"

    if core_sn:
        rekey = build_rekey_row_from_target(tgt, wrong_loan_no, app_no, core_sn)
        if rekey:
            return rekey, "rekey_only"

    reason = miss_reason.get(market_no, "no_market_no")
    return None, reason


def build_plan(
    tgt,
    src,
    due_before: str,
    status: str,
    min_sn_len: int,
    scan_size: int,
    work_limit: int,
) -> List[dict]:
    plan: List[dict] = []
    after = ""
    while True:
        rows = scan_target_batch(tgt, due_before, status, after, scan_size)
        if not rows:
            break
        after = str(rows[-1]["loan_no"])
        candidates = [
            r
            for r in rows
            if is_long_loan_no(str(r["loan_no"]), min_sn_len)
        ]
        if not candidates:
            if len(rows) < scan_size:
                break
            continue
        market_nos = [
            extract_market_no(str(r["application_no"]), str(r["loan_no"]))
            for r in candidates
        ]
        source_map, miss_reason = fetch_source_loans_by_market_nos(src, market_nos)
        target_sn_map = fetch_target_sn_by_application_nos(
            tgt, [str(r["application_no"]) for r in candidates]
        )
        for row, market_no in zip(candidates, market_nos):
            wrong = str(row["loan_no"])
            app_no = str(row["application_no"] or "")
            if not market_no:
                print("skip no_market_no loan_no=%s" % wrong, flush=True)
                continue
            src_row, mode = resolve_loan_row(
                tgt,
                src,
                wrong,
                app_no,
                market_no,
                source_map,
                miss_reason,
                target_sn_map,
            )
            if not src_row:
                print(
                    "skip no_source loan_no=%s market_no=%s reason=%s"
                    % (wrong, market_no, mode),
                    flush=True,
                )
                continue
            correct = str(src_row["loan_no"])
            if correct == wrong and mode == "source":
                print("skip already_correct loan_no=%s" % wrong, flush=True)
                continue
            plan.append(
                {
                    "wrong_loan_no": wrong,
                    "correct_loan_no": correct,
                    "legacy_loan_no": "",
                    "application_no": str(src_row.get("application_no") or app_no),
                    "app_id": "",
                    "market_no": market_no,
                    "source_row": src_row,
                    "sync_mode": mode,
                    "target_due_date": str(row.get("due_date") or ""),
                    "target_status": str(row.get("status") or ""),
                }
            )
            if work_limit and len(plan) >= work_limit:
                return plan
        if len(rows) < scan_size:
            break
    return plan


def _merge_source_row(before: dict, source_row: dict) -> dict:
    after = dict(before)
    for col in LOAN_COLS:
        if col in source_row:
            after[col] = source_row[col]
    return after


def sync_one_loan(
    tgt,
    plan_row: dict,
    dry_run: bool,
    audit: RepairAuditLog,
    row_audit: Optional[RowChangeAuditLog],
    tracker: Optional[CommitTracker],
) -> str:
    wrong = plan_row["wrong_loan_no"]
    correct = plan_row["correct_loan_no"]
    source_row = plan_row["source_row"]

    if not loan_exists(tgt, wrong):
        if loan_exists(tgt, correct):
            audit.record("skip_done", plan_row, "wrong_missing_correct_exists")
            return "skip_done"
        audit.record("skip", plan_row, "wrong_missing")
        return "skip_missing"

    before_wrong = fetch_loan_row(tgt, wrong)
    if not before_wrong:
        audit.record("skip", plan_row, "fetch_wrong_missing")
        return "skip_missing"

    set_parts = ["`%s`=%%s" % c for c in SYNC_COLS]
    set_sql = ", ".join(set_parts)
    sync_vals = [source_row[c] for c in SYNC_COLS]

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
            audit.record(
                "would_sync_delete",
                plan_row,
                "update_correct_from_source+delete_wrong",
            )
            return "ok"
        if before_correct and row_audit:
            row_audit.record_modified(
                "sync_correct",
                before_correct,
                _merge_source_row(before_correct, source_row),
            )
        with tgt.cursor() as cur:
            cur.execute(
                "UPDATE loan SET %s WHERE loan_no=%%s" % set_sql,
                sync_vals + [correct],
            )
        if row_audit:
            row_audit.record_deleted("delete_wrong", before_wrong)
        with tgt.cursor() as cur:
            cur.execute("DELETE FROM loan WHERE loan_no=%s", (wrong,))
            if not cur.rowcount:
                audit.record("skip", plan_row, "delete_wrong_failed")
                return "missing"
        audit.record("sync_delete", plan_row, "update_correct+delete_wrong")
    else:
        if dry_run:
            after = _merge_source_row(before_wrong, source_row)
            if row_audit:
                row_audit.record_modified("would_rekey_sync", before_wrong, after)
            audit.record("would_rekey_sync", plan_row, "update_loan_no_and_fields")
            return "ok"
        after = _merge_source_row(before_wrong, source_row)
        with tgt.cursor() as cur:
            cur.execute(
                "UPDATE loan SET loan_no=%%s, %s WHERE loan_no=%%s"
                % set_sql,
                [correct] + sync_vals + [wrong],
            )
            if not cur.rowcount:
                audit.record("skip", plan_row, "rekey_update_no_row")
                return "missing"
        if row_audit:
            row_audit.record_modified("rekey_sync", before_wrong, after)
        audit.record("rekey_sync", plan_row, plan_row.get("sync_mode", "update_loan_no_and_fields"))

    if tracker:
        tracker.note_write()
    else:
        tgt.commit()
    return "ok"


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
    p.add_argument("--scan-size", type=int, default=200)
    p.add_argument("--commit-every", type=int, default=20)
    p.add_argument("--work-limit", type=int, default=0)
    p.add_argument("--log-every", type=int, default=50)
    p.add_argument("--repair-log", default="")
    p.add_argument("--no-repair-log", action="store_true")
    p.add_argument("--plan-only", action="store_true")
    args = p.parse_args(argv)
    if args.apply and args.dry_run:
        p.error("use either --apply or --dry-run")
    dry_run = not args.apply

    cfg = load_env(Path(args.env))
    tgt = connect_target(cfg)
    src = connect_source(cfg)
    try:
        print(
            "scan due_before=%s status=%s min_sn_len=%s dry_run=%s"
            % (args.due_before, args.status, args.min_sn_len, dry_run),
            flush=True,
        )
        t0 = time.time()
        plan = build_plan(
            tgt,
            src,
            args.due_before,
            args.status,
            args.min_sn_len,
            args.scan_size,
            args.work_limit,
        )
        print(
            "repair_plan=%s elapsed=%.1fs"
            % (len(plan), time.time() - t0),
            flush=True,
        )
        for row in plan[:20]:
            src_row = row["source_row"]
            print(
                "  %s -> %s mode=%s app=%s src_status=%s due=%s tgt_due=%s"
                % (
                    row["wrong_loan_no"],
                    row["correct_loan_no"],
                    row.get("sync_mode", ""),
                    row["application_no"],
                    src_row.get("status"),
                    src_row.get("due_date"),
                    row.get("target_due_date"),
                ),
                flush=True,
            )
        if len(plan) > 20:
            print("  ... and %s more" % (len(plan) - 20), flush=True)
        if args.plan_only:
            return 0 if plan else 1
        if not plan:
            print("no matching loans to fix", flush=True)
            return 0
    finally:
        tgt.close()
        src.close()

    repair_log = args.repair_log or (
        "/tmp/repair_loan_status20_%s.csv"
        % datetime.now().strftime("%Y%m%d_%H%M%S")
    )
    audit = RepairAuditLog(
        repair_log if not args.no_repair_log else None,
        enabled=not args.no_repair_log,
    )
    row_audit = RowChangeAuditLog(repair_log, enabled=not args.no_repair_log)

    tgt = connect_target(cfg)
    try:
        ok = skip = 0
        tracker = CommitTracker(tgt, args.commit_every, dry_run)
        for i, row in enumerate(plan, 1):
            result = exec_with_retry(
                tgt,
                lambda r=row: sync_one_loan(
                    tgt, r, dry_run, audit, row_audit, tracker
                ),
                "sync %s" % row["wrong_loan_no"],
            )
            if result == "ok":
                ok += 1
            else:
                skip += 1
            if i % max(1, args.log_every) == 0:
                print(
                    "progress ok=%s skip=%s last=%s"
                    % (ok, skip, row["wrong_loan_no"]),
                    flush=True,
                )
        tracker.flush()
        print(
            "done ok=%s skip=%s repair_log=%s" % (ok, skip, repair_log),
            flush=True,
        )
        return 0
    finally:
        audit.close()
        row_audit.close()
        tgt.close()


if __name__ == "__main__":
    raise SystemExit(main())
