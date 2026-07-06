#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""修复 loan.application_no：以源库 ng_loan_market.application 为准。

正确 application_no:
  SELECT appId FROM ng_loan_market.application WHERE applicationNo = '{market号}';
  → ng{appId:04d}-{applicationNo}   (如 ng0515-178241404412018047)

误伤示例:
  loan: ng-217824140501-01000 / ng20606122-178241404412018047
  源库 market: applicationNo=178241404412018047, appId=515
  应改为: ng0515-178241404412018047

Usage:
  python3 repair_loan_app_no_mismatch.py --env ./ng_migration.env --dry-run
  python3 repair_loan_app_no_mismatch.py --env ./ng_migration.env --apply --commit-every 50
  python3 repair_loan_app_no_mismatch.py --env ./ng_migration.env --dry-run \\
    --due-before 2026-07-06 --status 20 --bad-prefix-only
"""
import argparse
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pymysql
from pymysql.cursors import DictCursor

import ng_migration_run as mig
from repair_loan_no_from_audit import CommitTracker, exec_with_retry

HERE = Path(__file__).resolve().parent
LOAN_NO_RE = re.compile(r"^[Nn][Gg]-(\d+)-(\d{5})$")
APP_NO_RE = re.compile(r"^ng\d+-(.+)$", re.I)
BAD_APP_PREFIX_RE = re.compile(r"^ng\d{5,}-", re.I)


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


def extract_market_no(application_no: str, loan_no: str) -> str:
    m = APP_NO_RE.match(str(application_no or "").strip())
    if m:
        return m.group(1).strip()
    parsed = LOAN_NO_RE.match(str(loan_no or "").strip())
    if parsed and len(parsed.group(1)) >= 15:
        return parsed.group(1)
    return ""


def scan_loan_batch(
    tgt,
    after_loan_no: str,
    limit: int,
    due_before: Optional[str],
    status: Optional[str],
    min_market_len: int,
) -> List[dict]:
    sql = """
        SELECT loan_no, application_no, period, roll_sequence
        FROM loan
        WHERE loan_no > %s
          AND loan_no REGEXP '^[Nn][Gg]-[0-9]+-[0-9]{5}$'
    """
    params: List = [after_loan_no or ""]
    if due_before:
        sql += " AND due_date < %s"
        params.append(due_before)
    if status is not None and status != "":
        sql += " AND status = %s"
        params.append(status)
    sql += " ORDER BY loan_no ASC LIMIT %s"
    params.append(limit)
    with tgt.cursor() as cur:
        cur.execute(sql, params)
        rows = list(cur.fetchall())
    if min_market_len <= 0:
        return rows
    out = []
    for row in rows:
        market = extract_market_no(
            str(row.get("application_no") or ""),
            str(row.get("loan_no") or ""),
        )
        if len(market) >= min_market_len:
            out.append(row)
    return out


def fetch_market_app_id_by_no(src, market_nos: List[str]) -> Dict[str, int]:
    """applicationNo -> appId（源库 ng_loan_market.application）。"""
    uniq = sorted({str(x).strip() for x in market_nos if x})
    out: Dict[str, int] = {}
    if not uniq:
        return out
    m = "ng_loan_market"
    for i in range(0, len(uniq), 2000):
        part = uniq[i : i + 2000]
        ph = ",".join(["%s"] * len(part))
        with src.cursor() as cur:
            cur.execute(
                f"""
                SELECT applicationNo AS market_no, `appId` AS app_id
                FROM {m}.application
                WHERE applicationNo IN ({ph})
                  AND `appId` IS NOT NULL
                """,
                part,
            )
            for row in cur.fetchall():
                market = str(row.get("market_no") or "").strip()
                if market:
                    out[market] = int(row["app_id"])
    return out


def canonical_application_no(market_no: str, app_id: int) -> str:
    return mig.format_application_no(app_id, market_no)


def build_plan(
    tgt,
    src,
    scan_size: int,
    work_limit: int,
    due_before: Optional[str],
    status: Optional[str],
    bad_prefix_only: bool,
    min_market_len: int,
) -> Tuple[List[dict], Dict[str, int]]:
    plan: List[dict] = []
    stats: Dict[str, int] = {}
    after = ""
    batches = 0
    pending_market: Dict[str, List[dict]] = {}

    def flush_pending():
        nonlocal plan, stats, pending_market
        if not pending_market:
            return
        market_nos = sorted(pending_market.keys())
        app_ids = fetch_market_app_id_by_no(src, market_nos)
        stats["market_queried"] = stats.get("market_queried", 0) + len(market_nos)
        stats["market_hit"] = stats.get("market_hit", 0) + len(app_ids)
        for market_no in market_nos:
            app_id = app_ids.get(market_no)
            if app_id is None:
                stats["skip_no_market"] = stats.get("skip_no_market", 0) + len(
                    pending_market[market_no]
                )
                continue
            good = canonical_application_no(market_no, app_id)
            if not good:
                stats["skip_bad_format"] = stats.get("skip_bad_format", 0) + len(
                    pending_market[market_no]
                )
                continue
            for row in pending_market[market_no]:
                bad = str(row["application_no"] or "").strip()
                if bad == good:
                    stats["skip_already_ok"] = stats.get("skip_already_ok", 0) + 1
                    continue
                if bad_prefix_only and not BAD_APP_PREFIX_RE.match(bad):
                    stats["skip_not_bad_prefix"] = stats.get("skip_not_bad_prefix", 0) + 1
                    continue
                plan.append(
                    {
                        "loan_no": str(row["loan_no"]),
                        "bad_application_no": bad,
                        "good_application_no": good,
                        "market_no": market_no,
                        "app_id": app_id,
                        "period": row.get("period", 1),
                        "roll_sequence": row.get("roll_sequence", 0),
                    }
                )
                stats["plan"] = stats.get("plan", 0) + 1
                if work_limit and len(plan) >= work_limit:
                    pending_market = {}
                    return
        pending_market = {}

    while True:
        try:
            tgt.ping(reconnect=True)
        except Exception:
            pass
        rows = exec_with_retry(
            tgt,
            lambda a=after: scan_loan_batch(
                tgt, a, scan_size, due_before, status, min_market_len
            ),
            "scan loan after=%s" % (after or "(start)"),
        )
        batches += 1
        if not rows:
            flush_pending()
            break
        after = str(rows[-1]["loan_no"])
        for row in rows:
            market = extract_market_no(
                str(row.get("application_no") or ""),
                str(row.get("loan_no") or ""),
            )
            if not market:
                stats["skip_no_market_no"] = stats.get("skip_no_market_no", 0) + 1
                continue
            pending_market.setdefault(market, []).append(row)
            if sum(len(v) for v in pending_market.values()) >= 2000:
                flush_pending()
                if work_limit and len(plan) >= work_limit:
                    return plan, stats
        if batches % 10 == 0:
            print(
                "scan batches=%s plan=%s last=%s"
                % (batches, len(plan), after),
                flush=True,
            )
        if len(rows) < scan_size:
            flush_pending()
            break
    return plan, stats


def loan_pk_exists(tgt, app_no: str, period, roll_sequence) -> bool:
    with tgt.cursor() as cur:
        cur.execute(
            """
            SELECT 1 FROM loan
            WHERE application_no=%s AND period=%s AND roll_sequence=%s
            LIMIT 1
            """,
            (app_no, period, roll_sequence),
        )
        return cur.fetchone() is not None


def apply_one(tgt, row: dict, dry_run: bool, tracker: Optional[CommitTracker]) -> str:
    good = row["good_application_no"]
    if loan_pk_exists(tgt, good, row["period"], row["roll_sequence"]):
        print(
            "skip pk_conflict loan_no=%s want_app=%s"
            % (row["loan_no"], good),
            flush=True,
        )
        return "skip_pk"
    if dry_run:
        print(
            "would_fix loan_no=%s market=%s appId=%s  %s -> %s"
            % (
                row["loan_no"],
                row["market_no"],
                row["app_id"],
                row["bad_application_no"],
                good,
            ),
            flush=True,
        )
        return "ok"
    with tgt.cursor() as cur:
        cur.execute(
            """
            UPDATE loan
            SET application_no=%s
            WHERE loan_no=%s AND application_no=%s
            """,
            (good, row["loan_no"], row["bad_application_no"]),
        )
        if not cur.rowcount:
            return "skip_no_row"
    if tracker:
        tracker.note_write()
    else:
        tgt.commit()
    return "ok"


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(
        description="Fix loan.application_no from ng_loan_market.application (appId + applicationNo)"
    )
    p.add_argument("--env", default=str(HERE / "ng_migration.env"))
    p.add_argument("--apply", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--due-before", default="", help="可选：只处理 due_date < 该日期")
    p.add_argument("--status", default="", help="可选：只处理指定 status")
    p.add_argument(
        "--bad-prefix-only",
        action="store_true",
        help="只修 ng+5位以上数字前缀的异常 application_no",
    )
    p.add_argument(
        "--min-market-len",
        type=int,
        default=15,
        help="application_no 后缀 market 号最短长度（默认 15）",
    )
    p.add_argument("--scan-size", type=int, default=500)
    p.add_argument("--commit-every", type=int, default=50)
    p.add_argument("--work-limit", type=int, default=0)
    p.add_argument("--log-every", type=int, default=100)
    args = p.parse_args(argv)
    if args.apply and args.dry_run:
        p.error("use either --apply or --dry-run")
    dry_run = not args.apply

    due_before = (args.due_before or "").strip() or None
    status = (args.status or "").strip() or None

    cfg = load_env(Path(args.env))
    tgt = connect_target(cfg)
    src = connect_source(cfg)
    try:
        print(
            "fix application_no from source market "
            "due_before=%s status=%s bad_prefix_only=%s dry_run=%s"
            % (due_before or "-", status or "-", args.bad_prefix_only, dry_run),
            flush=True,
        )
        plan, stats = build_plan(
            tgt,
            src,
            args.scan_size,
            args.work_limit,
            due_before,
            status,
            args.bad_prefix_only,
            args.min_market_len,
        )
        print("fix_plan=%s stats=%s" % (len(plan), stats), flush=True)
        for row in plan[:20]:
            print(
                "  %s  %s -> %s  (market=%s appId=%s)"
                % (
                    row["loan_no"],
                    row["bad_application_no"],
                    row["good_application_no"],
                    row["market_no"],
                    row["app_id"],
                ),
                flush=True,
            )
        if len(plan) > 20:
            print("  ... and %s more" % (len(plan) - 20), flush=True)
        if not plan:
            print("no rows to fix", flush=True)
            return 0
    finally:
        tgt.close()
        src.close()

    tgt = connect_target(cfg)
    try:
        ok = skip = 0
        tracker = CommitTracker(tgt, args.commit_every, dry_run)
        for i, row in enumerate(plan, 1):
            st = exec_with_retry(
                tgt,
                lambda r=row: apply_one(tgt, r, dry_run, tracker),
                "fix %s" % row["loan_no"],
            )
            if st == "ok":
                ok += 1
            else:
                skip += 1
            if i % max(1, args.log_every) == 0:
                print(
                    "progress ok=%s skip=%s last=%s"
                    % (ok, skip, row["loan_no"]),
                    flush=True,
                )
        tracker.flush()
        print("done ok=%s skip=%s" % (ok, skip), flush=True)
        return 0
    finally:
        tgt.close()


if __name__ == "__main__":
    raise SystemExit(main())
