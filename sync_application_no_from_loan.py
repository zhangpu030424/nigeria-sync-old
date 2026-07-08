#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""将 application.application_no 对齐到 loan 表（按 sn → loan_no 关联）。

场景：application_no 后缀误写成 core sn，loan 上已是 market 长号。
  application: ng0564-217832529551  （错，后缀=sn）
  loan:        loan_no=ng-217832529551-01000, application_no=ng0564-17832529551...（对）

等价思路:
  SELECT l.application_no
  FROM loan l
  WHERE l.loan_no = 'ng-{application.sn}-01000';
  → UPDATE application SET application_no = l.application_no WHERE ...

Usage:
  python3 sync_application_no_from_loan.py --env ./ng_migration.env --dry-run \\
    --plan-file /tmp/sync_app_no_from_loan_plan.json

  python3 sync_application_no_from_loan.py --env ./ng_migration.env --apply-only \\
    --plan-file /tmp/sync_app_no_from_loan_plan.json --batch-size 200
"""
import argparse
import json
import re
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pymysql
from pymysql.cursors import DictCursor

import ng_migration_run as mig
from repair_loan_no_from_audit import exec_with_retry

HERE = Path(__file__).resolve().parent
APP_SUFFIX_RE = re.compile(r"^ng\d+-(.+)$", re.I)
LOAN_NO_RE = re.compile(r"^[Nn][Gg]-(\d+)-(\d{5})$")


def load_env(path: Path) -> Dict[str, str]:
    cfg: Dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        cfg[k.strip()] = v.strip().strip("'\"")
    return cfg


def connect_target(cfg: Dict[str, str], for_apply: bool = False):
    return pymysql.connect(
        host=cfg["TARGET_HOST"],
        port=int(cfg.get("TARGET_PORT", "3306")),
        user=cfg["TARGET_USER"],
        password=cfg["TARGET_PASSWORD"],
        database=cfg.get("TARGET_DB", "ng"),
        charset="utf8mb4",
        cursorclass=DictCursor,
        connect_timeout=int(cfg.get("mysql_connect_timeout") or 60),
        read_timeout=30 if for_apply else 3600,
        write_timeout=30 if for_apply else 3600,
        autocommit=False,
    )


def app_suffix(application_no: str) -> str:
    m = APP_SUFFIX_RE.match(str(application_no or "").strip())
    return m.group(1).strip() if m else ""


def pick_good_application_no(loan_rows: List[dict], min_market_len: int) -> Optional[str]:
    """同一 loan_no 多行时优先 market 长号后缀。"""
    candidates: List[str] = []
    for row in loan_rows:
        app = str(row.get("application_no") or "").strip()
        if not app:
            continue
        sfx = app_suffix(app)
        if sfx and len(sfx) >= min_market_len:
            candidates.append(app)
    if candidates:
        return sorted(set(candidates), key=len, reverse=True)[0]
    for row in loan_rows:
        app = str(row.get("application_no") or "").strip()
        if app:
            return app
    return None


def build_plan(
    tgt,
    period: int,
    roll_sequence: int,
    min_market_len: int,
    orphan_only: bool,
    core_sn_suffix_only: bool,
) -> Tuple[List[dict], Dict[str, int]]:
    """扫描 application（已放款、与 loan 按 application_no 对不上），按 sn 找 loan_no。"""
    stats: Dict[str, int] = {}
    t0 = time.time()
    print("scan orphan applications (disbursed, no loan on application_no) ...", flush=True)
    sql = """
        SELECT a.application_no AS bad_application_no, a.sn, a.app_id
        FROM application a
        LEFT JOIN loan l ON l.application_no = a.application_no
        WHERE a.disbursed_time > 0
          AND a.application_no IS NOT NULL AND a.application_no <> ''
          AND a.sn IS NOT NULL AND a.sn <> ''
          AND l.application_no IS NULL
        ORDER BY a.application_no ASC
    """
    with tgt.cursor() as cur:
        cur.execute(sql)
        orphan_apps = list(cur.fetchall())
    stats["orphan_apps"] = len(orphan_apps)
    print(
        "orphan_apps=%s elapsed=%.1fs" % (len(orphan_apps), time.time() - t0),
        flush=True,
    )

    if not orphan_only:
        print("also scan application_no <> loan.application_no by sn ...", flush=True)
        sql2 = """
            SELECT a.application_no AS bad_application_no, a.sn, a.app_id
            FROM application a
            INNER JOIN loan l0 ON l0.application_no = a.application_no
            INNER JOIN loan l ON l.loan_no = CONCAT('ng-', a.sn, '-', %s, %s)
            WHERE a.disbursed_time > 0
              AND a.application_no <> l.application_no
            ORDER BY a.application_no ASC
        """
        period_str = "%02d" % int(period)
        roll_str = "%03d" % int(roll_sequence)
        with tgt.cursor() as cur:
            cur.execute(sql2, (period_str, roll_str))
            mismatched = list(cur.fetchall())
        seen = {str(r["bad_application_no"]) for r in orphan_apps}
        for row in mismatched:
            bad = str(row["bad_application_no"])
            if bad not in seen:
                orphan_apps.append(row)
                seen.add(bad)
        stats["mismatched_join_apps"] = len(mismatched)

    loan_nos = sorted(
        {
            mig.format_loan_no(str(r.get("sn") or "").strip(), period, roll_sequence)
            for r in orphan_apps
            if r.get("sn")
        }
    )
    loan_nos = [x for x in loan_nos if x]
    stats["unique_loan_no"] = len(loan_nos)
    print("lookup loan by loan_no keys=%s ..." % len(loan_nos), flush=True)

    loan_by_no: Dict[str, List[dict]] = {}
    chunk = 500
    for i in range(0, len(loan_nos), chunk):
        part = loan_nos[i : i + chunk]
        ph = ",".join(["%s"] * len(part))
        with tgt.cursor() as cur:
            cur.execute(
                "SELECT loan_no, application_no FROM loan WHERE loan_no IN (%s)" % ph,
                part,
            )
            for row in cur.fetchall():
                ln = str(row.get("loan_no") or "").strip()
                if ln:
                    loan_by_no.setdefault(ln, []).append(row)

    existing_apps: Dict[str, str] = {}
    with tgt.cursor() as cur:
        cur.execute("SELECT application_no, sn FROM application")
        for row in cur.fetchall():
            existing_apps[str(row["application_no"]).strip()] = str(row.get("sn") or "")

    plan: List[dict] = []
    for app in orphan_apps:
        bad = str(app.get("bad_application_no") or "").strip()
        sn = str(app.get("sn") or "").strip()
        if not bad or not sn:
            stats["skip_empty"] = stats.get("skip_empty", 0) + 1
            continue
        sfx = app_suffix(bad)
        if core_sn_suffix_only:
            if not sfx or sfx != sn:
                stats["skip_suffix_not_sn"] = stats.get("skip_suffix_not_sn", 0) + 1
                continue
            if len(sfx) >= min_market_len:
                stats["skip_already_market_suffix"] = stats.get(
                    "skip_already_market_suffix", 0
                ) + 1
                continue
        loan_no = mig.format_loan_no(sn, period, roll_sequence)
        loan_rows = loan_by_no.get(loan_no) or []
        if not loan_rows:
            stats["skip_no_loan"] = stats.get("skip_no_loan", 0) + 1
            continue
        good = pick_good_application_no(loan_rows, min_market_len)
        if not good or good == bad:
            stats["skip_same_or_empty"] = stats.get("skip_same_or_empty", 0) + 1
            continue
        if good in existing_apps and existing_apps.get(good) != sn:
            stats["skip_good_app_taken"] = stats.get("skip_good_app_taken", 0) + 1
            continue
        plan.append(
            {
                "bad_application_no": bad,
                "good_application_no": good,
                "loan_no": loan_no,
                "sn": sn,
                "app_id": app.get("app_id"),
            }
        )
    stats["plan"] = len(plan)
    print("plan=%s stats=%s elapsed=%.1fs" % (len(plan), stats, time.time() - t0), flush=True)
    return plan, stats


def apply_batch(tgt, rows: List[dict], dry_run: bool) -> int:
    if not rows:
        return 0
    if dry_run:
        return len(rows)
    parts: List[str] = []
    params: List = []
    for r in rows:
        parts.append("SELECT %s AS bad_app, %s AS good_app, %s AS sn")
        params.extend([r["bad_application_no"], r["good_application_no"], r["sn"]])
    sql = (
        """
        UPDATE application a
        INNER JOIN (
        """
        + " UNION ALL ".join(parts)
        + """
        ) x ON a.application_no = x.bad_app AND a.sn = x.sn
        SET a.application_no = x.good_app
        """
    )
    with tgt.cursor() as cur:
        cur.execute(sql, tuple(params))
        return int(cur.rowcount or 0)


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(
        description="Sync application.application_no from loan (match by sn→loan_no)"
    )
    p.add_argument("--env", default=str(HERE / "ng_migration.env"))
    p.add_argument("--apply", action="store_true")
    p.add_argument("--apply-only", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--plan-file", default="/tmp/sync_app_no_from_loan_plan.json")
    p.add_argument("--batch-size", type=int, default=200)
    p.add_argument("--commit-every", type=int, default=20)
    p.add_argument("--period", type=int, default=1)
    p.add_argument("--roll-sequence", type=int, default=0)
    p.add_argument("--min-market-len", type=int, default=15)
    p.add_argument(
        "--include-mismatched",
        action="store_true",
        help="除 orphan 外，还处理已能 JOIN loan 但 application_no 与 sn 对应 loan 不一致的",
    )
    p.add_argument(
        "--all-suffix",
        action="store_true",
        help="不限制 application 后缀必须等于 sn（默认只修 core sn 后缀）",
    )
    p.add_argument("--work-limit", type=int, default=0)
    args = p.parse_args(argv)
    if args.apply and args.dry_run:
        p.error("use either --apply or --dry-run")
    if args.apply_only:
        args.apply = True
    dry_run = not args.apply

    plan_path = Path(args.plan_file)
    cfg = load_env(Path(args.env))

    if args.apply_only and plan_path.is_file():
        plan = json.loads(plan_path.read_text(encoding="utf-8"))
        print("apply-only loaded plan=%s" % len(plan), flush=True)
    else:
        tgt = connect_target(cfg)
        try:
            plan, stats = build_plan(
                tgt,
                args.period,
                args.roll_sequence,
                args.min_market_len,
                orphan_only=not args.include_mismatched,
                core_sn_suffix_only=not args.all_suffix,
            )
        finally:
            tgt.close()
        if args.work_limit > 0:
            plan = plan[: args.work_limit]
        plan_path.write_text(
            json.dumps(plan, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        print("wrote plan_file=%s rows=%s" % (plan_path, len(plan)), flush=True)

    for row in plan[:15]:
        print(
            "  %s -> %s  loan=%s sn=%s"
            % (
                row["bad_application_no"],
                row["good_application_no"],
                row["loan_no"],
                row.get("sn"),
            ),
            flush=True,
        )
    if len(plan) > 15:
        print("  ... and %s more" % (len(plan) - 15), flush=True)
    if not plan:
        return 0
    if dry_run and not args.apply_only:
        print("dry-run only, no DB writes (use --apply or --apply-only)", flush=True)
        return 0

    tgt = connect_target(cfg, for_apply=True)
    ok = skip = 0
    batch_size = max(1, args.batch_size)
    commits = 0
    try:
        for i in range(0, len(plan), batch_size):
            part = plan[i : i + batch_size]
            bno = i // batch_size + 1
            n = exec_with_retry(
                tgt,
                lambda p=part: apply_batch(tgt, p, False),
                "apply batch %s" % bno,
            )
            tgt.commit()
            commits += 1
            ok += n
            skip += len(part) - n
            print(
                "batch %s updated=%s/%s total_ok=%s"
                % (bno, n, len(part), ok),
                flush=True,
            )
    finally:
        tgt.close()
    print("done ok=%s skip=%s" % (ok, skip), flush=True)
    return 0 if ok or skip else 1


if __name__ == "__main__":
    raise SystemExit(main())
