#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""修复目标库中 due_date 已到期且 status=20、但 loan_no 中间段仍为 market 长号的 loan 行。

策略（默认 bulk）:
  1. 一次查出目标库 due_date/status 符合条件的全部 loan
  2. 内存中按 loan_no 中间段长度筛长号
  3. 源库批量: ng_loan_core.application(ext_sn) + ng_loan_core.repay_plan(sn)
  4. 内存拼修复计划后批量写库

Usage:
  python3 repair_loan_status20_from_source.py --env ./ng_migration.env --dry-run --plan-only
  python3 repair_loan_status20_from_source.py --env ./ng_migration.env --apply --commit-every 50
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
    cols_sql,
    exec_with_retry,
    fetch_loan_row,
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


def fetch_source_loans_bulk(
    src,
    market_nos: List[str],
    target_app_by_market: Optional[Dict[str, str]] = None,
) -> Tuple[Dict[str, dict], Dict[str, str], Dict[str, Tuple[str, str]]]:
    """market applicationNo -> loan 行；miss_reason；meta(ext_sn->core_sn,app_no)。"""
    if not market_nos:
        return {}, {}, {}
    uniq = sorted({str(x).strip() for x in market_nos if x})
    target_app_by_market = target_app_by_market or {}
    meta = fetch_source_market_meta(src, uniq)
    missing = [m for m in uniq if m not in meta]
    if missing:
        core_only = fetch_core_application_by_ext_sn(src, missing)
        for ext_sn, core_sn in core_only.items():
            meta[ext_sn] = (core_sn, target_app_by_market.get(ext_sn, ""))
    repay_plans = fetch_repay_plan_by_sns(src, [v[0] for v in meta.values()])
    by_market: Dict[str, dict] = {}
    miss_reason: Dict[str, str] = {}
    for market_no in uniq:
        pair = meta.get(market_no)
        if not pair:
            miss_reason[market_no] = "no_core_application"
            continue
        core_sn, app_no = pair
        if not app_no:
            miss_reason[market_no] = "no_app_no"
            continue
        rp = repay_plans.get(core_sn)
        if not rp:
            miss_reason[market_no] = "no_repay_plan"
            continue
        by_market[market_no] = mig._build_loan_row(rp, app_no)
    return by_market, miss_reason, meta


def build_rekey_row_from_before(
    before: dict, application_no: str, core_sn: str
) -> Optional[dict]:
    correct = mig.format_loan_no(core_sn, 1, 0)
    if not before or not correct:
        return None
    row = dict(before)
    row["loan_no"] = correct
    if application_no:
        row["application_no"] = application_no
    return row


def build_plan_in_memory(
    candidates: List[dict],
    source_by_market: Dict[str, dict],
    miss_reason: Dict[str, str],
    target_sn_by_app: Dict[str, str],
    meta_by_market: Dict[str, Tuple[str, str]],
) -> List[dict]:
    plan: List[dict] = []
    for row in candidates:
        wrong = str(row["loan_no"])
        app_no = str(row.get("application_no") or "")
        market_no = extract_market_no(app_no, wrong)
        if not market_no:
            print("skip no_market_no loan_no=%s" % wrong, flush=True)
            continue

        src_row = source_by_market.get(market_no)
        mode = "source"
        if not src_row:
            core_sn = target_sn_by_app.get(app_no, "")
            if not core_sn:
                pair = meta_by_market.get(market_no)
                if pair:
                    core_sn = pair[0]
            if core_sn:
                src_row = build_rekey_row_from_before(row, app_no, core_sn)
                mode = "rekey_only" if src_row else ""
            if not src_row:
                reason = miss_reason.get(market_no, "no_core_sn")
                print(
                    "skip no_source loan_no=%s market_no=%s reason=%s"
                    % (wrong, market_no, reason),
                    flush=True,
                )
                continue

        correct = str(src_row["loan_no"])
        if correct == wrong and mode == "source":
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
                "before_wrong": dict(row),
                "sync_mode": mode,
                "target_due_date": str(row.get("due_date") or ""),
                "target_status": str(row.get("status") or ""),
            }
        )
    return plan


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
    loan_no_set: set,
    loan_by_no: Dict[str, dict],
) -> str:
    wrong = plan_row["wrong_loan_no"]
    correct = plan_row["correct_loan_no"]
    source_row = plan_row["source_row"]
    before_wrong = plan_row.get("before_wrong") or loan_by_no.get(wrong)

    if wrong not in loan_no_set:
        if correct in loan_no_set:
            audit.record("skip_done", plan_row, "wrong_missing_correct_exists")
            return "skip_done"
        audit.record("skip", plan_row, "wrong_missing")
        return "skip_missing"

    if not before_wrong:
        before_wrong = fetch_loan_row(tgt, wrong)
    if not before_wrong:
        audit.record("skip", plan_row, "fetch_wrong_missing")
        return "skip_missing"

    set_parts = ["`%s`=%%s" % c for c in SYNC_COLS]
    set_sql = ", ".join(set_parts)
    sync_vals = [source_row[c] for c in SYNC_COLS]

    if correct in loan_no_set and correct != wrong:
        before_correct = loan_by_no.get(correct) or fetch_loan_row(tgt, correct)
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
        loan_no_set.discard(wrong)
        loan_no_set.add(correct)
        loan_by_no[correct] = _merge_source_row(before_correct or {}, source_row)
        loan_by_no.pop(wrong, None)
        audit.record("sync_delete", plan_row, plan_row.get("sync_mode", ""))
    else:
        if dry_run:
            after = _merge_source_row(before_wrong, source_row)
            if row_audit:
                row_audit.record_modified("would_rekey_sync", before_wrong, after)
            audit.record("would_rekey_sync", plan_row, plan_row.get("sync_mode", ""))
            return "ok"
        after = _merge_source_row(before_wrong, source_row)
        with tgt.cursor() as cur:
            cur.execute(
                "UPDATE loan SET loan_no=%%s, %s WHERE loan_no=%%s" % set_sql,
                [correct] + sync_vals + [wrong],
            )
            if not cur.rowcount:
                audit.record("skip", plan_row, "rekey_update_no_row")
                return "missing"
        if row_audit:
            row_audit.record_modified("rekey_sync", before_wrong, after)
        loan_no_set.discard(wrong)
        loan_no_set.add(correct)
        loan_by_no[correct] = after
        loan_by_no.pop(wrong, None)
        audit.record("rekey_sync", plan_row, plan_row.get("sync_mode", ""))

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
    p.add_argument("--commit-every", type=int, default=50)
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
            "start bulk due_before=%s status=%s min_sn_len=%s dry_run=%s plan_only=%s"
            % (
                args.due_before,
                args.status,
                args.min_sn_len,
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

        candidates = filter_long_candidates(all_loans, args.min_sn_len)
        print("long_loan_candidates=%s" % len(candidates), flush=True)
        if not candidates:
            print("no long loan_no rows", flush=True)
            return 0

        market_nos = sorted(
            {
                extract_market_no(
                    str(r.get("application_no") or ""),
                    str(r.get("loan_no") or ""),
                )
                for r in candidates
            }
            - {""}
        )
        target_app_by_market = {
            extract_market_no(
                str(r.get("application_no") or ""),
                str(r.get("loan_no") or ""),
            ): str(r.get("application_no") or "")
            for r in candidates
        }
        target_app_by_market = {
            k: v for k, v in target_app_by_market.items() if k and v
        }
        app_nos = sorted(
            {str(r.get("application_no") or "").strip() for r in candidates} - {""}
        )

        print("load source repay_plan market_nos=%s ..." % len(market_nos), flush=True)
        t0 = time.time()
        source_by_market, miss_reason, meta_by_market = fetch_source_loans_bulk(
            src, market_nos, target_app_by_market
        )
        print(
            "source_hits=%s miss=%s meta=%s elapsed=%.1fs"
            % (
                len(source_by_market),
                len(miss_reason),
                len(meta_by_market),
                time.time() - t0,
            ),
            flush=True,
        )

        print("load target application.sn app_nos=%s ..." % len(app_nos), flush=True)
        t0 = time.time()
        target_sn_by_app = fetch_target_sn_by_application_nos(tgt, app_nos)
        print(
            "target_sn_map=%s elapsed=%.1fs"
            % (len(target_sn_by_app), time.time() - t0),
            flush=True,
        )

        plan = build_plan_in_memory(
            candidates,
            source_by_market,
            miss_reason,
            target_sn_by_app,
            meta_by_market,
        )
        if args.work_limit:
            plan = plan[: args.work_limit]
        print("repair_plan=%s" % len(plan), flush=True)
        for row in plan[:20]:
            src_row = row["source_row"]
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

        ok = skip = 0
        tracker = CommitTracker(tgt, args.commit_every, dry_run)
        for i, row in enumerate(plan, 1):
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
