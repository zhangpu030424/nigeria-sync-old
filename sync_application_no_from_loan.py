#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""将 application.application_no 对齐到 loan 表（按 sn → loan_no 关联）。

加载策略（尽量少查库）:
  1. 一条 SQL 拉出 orphan application（已放款但 JOIN 不上 loan）
  2. 分页拉全量 loan(loan_no, application_no) 进内存建索引
  3. 分页拉已放款 application(application_no, sn) 做 PK 冲突检测
  4. 内存匹配生成 plan，再批量 UPDATE

场景：application_no 后缀误写成 core sn，loan 上已是 market 长号。
  application: ng0564-217832529551
  loan:        loan_no=ng-217832529551-01000, application_no=ng0564-1783...

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
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pymysql
from pymysql.cursors import DictCursor

import ng_migration_run as mig
from repair_loan_no_from_audit import exec_with_retry

HERE = Path(__file__).resolve().parent
APP_SUFFIX_RE = re.compile(r"^ng\d+-(.+)$", re.I)

ORPHAN_APPS_SQL = """
    SELECT a.application_no AS bad_application_no, a.sn, a.app_id
    FROM application a
    LEFT JOIN loan l ON l.application_no = a.application_no
    WHERE a.disbursed_time > 0
      AND a.application_no IS NOT NULL AND a.application_no <> ''
      AND a.sn IS NOT NULL AND a.sn <> ''
      AND l.application_no IS NULL
    ORDER BY a.application_no ASC
"""


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


def _ping(conn) -> None:
    try:
        conn.ping(reconnect=True)
    except Exception:
        pass


def app_suffix(application_no: str) -> str:
    m = APP_SUFFIX_RE.match(str(application_no or "").strip())
    return m.group(1).strip() if m else ""


def load_orphan_apps(tgt) -> List[dict]:
    print("phase1: orphan applications (one SQL) ...", flush=True)
    t0 = time.time()
    with tgt.cursor() as cur:
        cur.execute(ORPHAN_APPS_SQL)
        rows = list(cur.fetchall())
    print("orphan_apps=%s elapsed=%.1fs" % (len(rows), time.time() - t0), flush=True)
    return rows


def load_all_loans_index(
    tgt, page_size: int = 50000
) -> Dict[str, List[dict]]:
    """分页拉全表 loan_no + application_no，返回 loan_no -> [rows]。"""
    print("phase2: load loan table into memory page_size=%s ..." % page_size, flush=True)
    t0 = time.time()
    by_loan_no: Dict[str, List[dict]] = defaultdict(list)
    sql = """
        SELECT loan_no, application_no
        FROM loan
        WHERE loan_no IS NOT NULL AND loan_no <> ''
          AND loan_no > %s
        ORDER BY loan_no ASC
        LIMIT %s
    """
    after = ""
    page_no = 0
    total = 0
    while True:
        def _page(a=after, lim=page_size):
            _ping(tgt)
            with tgt.cursor() as cur:
                cur.execute(sql, (a, lim))
                return list(cur.fetchall())

        page_no += 1
        batch = exec_with_retry(tgt, _page, "load loan page=%s" % page_no)
        if not batch:
            break
        for row in batch:
            ln = str(row.get("loan_no") or "").strip()
            if not ln:
                continue
            by_loan_no[ln].append(
                {
                    "loan_no": ln,
                    "application_no": str(row.get("application_no") or "").strip(),
                }
            )
            total += 1
        after = str(batch[-1]["loan_no"])
        if page_no == 1 or total % 500000 == 0 or len(batch) < page_size:
            print(
                "  loan rows=%s unique_loan_no=%s pages=%s elapsed=%.1fs"
                % (total, len(by_loan_no), page_no, time.time() - t0),
                flush=True,
            )
        if len(batch) < page_size:
            break
    print(
        "loaded loan rows=%s unique_loan_no=%s elapsed=%.1fs"
        % (total, len(by_loan_no), time.time() - t0),
        flush=True,
    )
    return dict(by_loan_no)


def load_disbursed_app_index(
    tgt, page_size: int = 50000
) -> Dict[str, str]:
    """分页拉已放款 application(application_no, sn) 进内存，用于 PK 冲突检测。"""
    print(
        "phase2b: load disbursed application index page_size=%s ..."
        % page_size,
        flush=True,
    )
    t0 = time.time()
    by_app_no: Dict[str, str] = {}
    sql = """
        SELECT application_no, sn
        FROM application
        WHERE disbursed_time > 0
          AND application_no IS NOT NULL AND application_no <> ''
          AND application_no > %s
        ORDER BY application_no ASC
        LIMIT %s
    """
    after = ""
    page_no = 0
    total = 0
    while True:
        def _page(a=after, lim=page_size):
            _ping(tgt)
            with tgt.cursor() as cur:
                cur.execute(sql, (a, lim))
                return list(cur.fetchall())

        page_no += 1
        batch = exec_with_retry(tgt, _page, "load application page=%s" % page_no)
        if not batch:
            break
        for row in batch:
            app = str(row.get("application_no") or "").strip()
            if not app:
                continue
            by_app_no[app] = str(row.get("sn") or "").strip()
            total += 1
        after = str(batch[-1]["application_no"])
        if page_no == 1 or total % 500000 == 0 or len(batch) < page_size:
            print(
                "  application rows=%s pages=%s elapsed=%.1fs"
                % (total, page_no, time.time() - t0),
                flush=True,
            )
        if len(batch) < page_size:
            break
    print(
        "loaded disbursed application rows=%s elapsed=%.1fs"
        % (total, time.time() - t0),
        flush=True,
    )
    return by_app_no


def pick_good_application_no(loan_rows: List[dict], min_market_len: int) -> Optional[str]:
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


def build_plan_in_memory(
    orphan_apps: List[dict],
    loan_by_no: Dict[str, List[dict]],
    existing_apps: Dict[str, str],
    period: int,
    roll_sequence: int,
    min_market_len: int,
    core_sn_suffix_only: bool,
) -> Tuple[List[dict], Dict[str, int]]:
    stats: Dict[str, int] = {"orphan_apps": len(orphan_apps)}
    t0 = time.time()
    planned_good: set = set()

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
        # good 在 loan 上出现是正常的；只检查 application 表 PK 是否已被占用
        owner_sn = existing_apps.get(good)
        if owner_sn is not None:
            if owner_sn == sn:
                stats["skip_good_exists_same_sn"] = stats.get(
                    "skip_good_exists_same_sn", 0
                ) + 1
            else:
                stats["skip_good_app_taken"] = stats.get("skip_good_app_taken", 0) + 1
            continue
        if good in planned_good:
            stats["skip_good_planned_dup"] = stats.get("skip_good_planned_dup", 0) + 1
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
        planned_good.add(good)

    stats["plan"] = len(plan)
    print(
        "phase3: plan=%s stats=%s elapsed=%.1fs"
        % (len(plan), stats, time.time() - t0),
        flush=True,
    )
    return plan, stats


def build_plan(
    tgt,
    period: int,
    roll_sequence: int,
    min_market_len: int,
    core_sn_suffix_only: bool,
    loan_page_size: int,
) -> Tuple[List[dict], Dict[str, int]]:
    orphan_apps = load_orphan_apps(tgt)
    loan_by_no = load_all_loans_index(tgt, loan_page_size)
    existing_apps = load_disbursed_app_index(tgt, loan_page_size)
    return build_plan_in_memory(
        orphan_apps,
        loan_by_no,
        existing_apps,
        period,
        roll_sequence,
        min_market_len,
        core_sn_suffix_only,
    )


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
        description="Sync application.application_no from loan (2 queries + memory)"
    )
    p.add_argument("--env", default=str(HERE / "ng_migration.env"))
    p.add_argument("--apply", action="store_true")
    p.add_argument("--apply-only", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--plan-file", default="/tmp/sync_app_no_from_loan_plan.json")
    p.add_argument("--batch-size", type=int, default=200)
    p.add_argument("--loan-page-size", type=int, default=50000)
    p.add_argument("--period", type=int, default=1)
    p.add_argument("--roll-sequence", type=int, default=0)
    p.add_argument("--min-market-len", type=int, default=15)
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
                core_sn_suffix_only=not args.all_suffix,
                loan_page_size=max(1000, args.loan_page_size),
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
