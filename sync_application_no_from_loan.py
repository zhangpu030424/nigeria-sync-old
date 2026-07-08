#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""将 application.application_no 对齐到 loan 表（按 sn → loan_no 关联）。

加载策略（尽量少查库）:
  1. 一条 SQL 拉出 orphan application（已放款但 JOIN 不上 loan）
  2. 分页拉全量 loan(loan_no, application_no) 进内存建索引
  3. 内存匹配直接出 plan（不预查 application PK）
  4. apply 多进程逐条 UPDATE；目标 application_no 已存在则跳过

场景：application_no 后缀误写成 core sn，loan 上已是 market 长号。
  application: ng0564-217832529551
  loan:        loan_no=ng-217832529551-01000, application_no=ng0564-1783...

Usage:
  python3 sync_application_no_from_loan.py --env ./ng_migration.env --dry-run \\
    --plan-file /tmp/sync_app_no_from_loan_plan.json

  python3 sync_application_no_from_loan.py --env ./ng_migration.env --apply \\
    --plan-file /tmp/sync_app_no_from_loan_plan.json --workers 8

  python3 sync_application_no_from_loan.py --env ./ng_migration.env --apply-only \\
    --plan-file /tmp/sync_app_no_from_loan_plan.json --workers 8
"""
import argparse
import json
import multiprocessing
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


def load_all_loans_index(tgt, page_size: int = 50000) -> Dict[str, List[dict]]:
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


def apply_one(tgt, row: dict) -> str:
    """返回 ok / skip_exists / skip_missing / skip_dup_key / err。"""
    bad = row["bad_application_no"]
    good = row["good_application_no"]
    sn = row["sn"]

    def _run():
        _ping(tgt)
        with tgt.cursor() as cur:
            cur.execute(
                "SELECT application_no FROM application WHERE application_no=%s LIMIT 1",
                (good,),
            )
            if cur.fetchone():
                return "skip_exists"
            cur.execute(
                """
                UPDATE application
                SET application_no=%s
                WHERE application_no=%s AND sn=%s
                """,
                (good, bad, sn),
            )
            if int(cur.rowcount or 0) <= 0:
                return "skip_missing"
            return "ok"

    try:
        return exec_with_retry(
            tgt, _run, "update %s->%s" % (bad, good)
        )
    except pymysql.err.IntegrityError as e:
        # 并发下目标号已被占用
        if getattr(e, "args", None) and e.args and e.args[0] == 1062:
            try:
                tgt.rollback()
            except Exception:
                pass
            return "skip_dup_key"
        raise


def split_chunks(rows: List[dict], workers: int) -> List[List[dict]]:
    n = max(1, int(workers))
    if n == 1:
        return [rows]
    chunks: List[List[dict]] = [[] for _ in range(n)]
    for i, row in enumerate(rows):
        chunks[i % n].append(row)
    return [c for c in chunks if c]


def worker_run(spec: dict) -> Tuple[int, int, Dict[str, int]]:
    label = "[%s/%s] " % (spec["worker_id"], spec["workers"])
    chunk = spec.get("plan_chunk") or []
    stats: Dict[str, int] = {}
    if not chunk:
        return 0, 0, stats
    cfg = load_env(Path(spec["env"]))
    commit_every = max(1, int(spec.get("commit_every") or 50))
    print("%sstart rows=%s" % (label, len(chunk)), flush=True)
    tgt = connect_target(cfg, for_apply=True)
    ok = skip = 0
    since_commit = 0
    try:
        for i, row in enumerate(chunk, 1):
            result = apply_one(tgt, row)
            if result == "ok":
                ok += 1
                since_commit += 1
                if since_commit >= commit_every:
                    tgt.commit()
                    since_commit = 0
            else:
                skip += 1
                stats[result] = stats.get(result, 0) + 1
                try:
                    tgt.rollback()
                except Exception:
                    pass
            if i == 1 or i % 200 == 0 or i == len(chunk):
                print(
                    "%sprogress %s/%s ok=%s skip=%s"
                    % (label, i, len(chunk), ok, skip),
                    flush=True,
                )
        if since_commit:
            tgt.commit()
    finally:
        tgt.close()
    print("%sdone ok=%s skip=%s stats=%s" % (label, ok, skip, stats), flush=True)
    return ok, skip, stats


def run_parallel(
    plan: List[dict],
    workers: int,
    env_path: str,
    commit_every: int,
) -> Tuple[int, int, Dict[str, int]]:
    workers = min(max(1, int(workers)), 16)
    chunks = split_chunks(plan, workers)
    specs = []
    for i, chunk in enumerate(chunks):
        specs.append(
            {
                "worker_id": i + 1,
                "workers": len(chunks),
                "env": env_path,
                "commit_every": commit_every,
                "plan_chunk": chunk,
            }
        )
    print(
        "parallel apply workers=%s rows=%s commit_every=%s"
        % (len(specs), len(plan), commit_every),
        flush=True,
    )
    if len(specs) == 1:
        return worker_run(specs[0])
    ctx = multiprocessing.get_context("spawn")
    with ctx.Pool(processes=len(specs)) as pool:
        results = pool.map(worker_run, specs)
    ok = sum(r[0] for r in results)
    skip = sum(r[1] for r in results)
    stats: Dict[str, int] = {}
    for _, _, s in results:
        for k, v in s.items():
            stats[k] = stats.get(k, 0) + v
    print(
        "parallel done ok=%s skip=%s skip_stats=%s" % (ok, skip, stats),
        flush=True,
    )
    return ok, skip, stats


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(
        description="Sync application.application_no from loan (memory plan + parallel apply)"
    )
    p.add_argument("--env", default=str(HERE / "ng_migration.env"))
    p.add_argument("--apply", action="store_true")
    p.add_argument("--apply-only", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--plan-file", default="/tmp/sync_app_no_from_loan_plan.json")
    p.add_argument("--loan-page-size", type=int, default=50000)
    p.add_argument("--period", type=int, default=1)
    p.add_argument("--roll-sequence", type=int, default=0)
    p.add_argument("--min-market-len", type=int, default=15)
    p.add_argument("--workers", type=int, default=8, help="apply 并行进程数")
    p.add_argument("--commit-every", type=int, default=50)
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
    env_path = str(Path(args.env).resolve())

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

    ok, skip, skip_stats = run_parallel(
        plan,
        args.workers,
        env_path,
        args.commit_every,
    )
    print("done ok=%s skip=%s skip_stats=%s" % (ok, skip, skip_stats), flush=True)
    return 0 if ok or skip else 1


if __name__ == "__main__":
    raise SystemExit(main())
