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
  # 只出计划
  python3 sync_application_no_from_loan.py --env ./ng_migration.env --dry-run \\
    --plan-file /tmp/sync_app_no_from_loan_plan.json

  # 已有 plan：跳过扫描，直接多进程修复
  python3 sync_application_no_from_loan.py --env ./ng_migration.env --apply-only \\
    --plan-file /tmp/sync_app_no_from_loan_plan.json --workers 4 --batch-size 50
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
    # apply 也要给足超时：PK UPDATE 可能锁等待，30s 容易 2013
    timeout = 120 if for_apply else 3600
    return pymysql.connect(
        host=cfg["TARGET_HOST"],
        port=int(cfg.get("TARGET_PORT", "3306")),
        user=cfg["TARGET_USER"],
        password=cfg["TARGET_PASSWORD"],
        database=cfg.get("TARGET_DB", "ng"),
        charset="utf8mb4",
        cursorclass=DictCursor,
        connect_timeout=int(cfg.get("mysql_connect_timeout") or 60),
        read_timeout=timeout,
        write_timeout=timeout,
        autocommit=False,
    )


def _ping(conn) -> None:
    try:
        conn.ping(reconnect=True)
    except Exception:
        pass


def _reconnect(cfg: Dict[str, str], old=None):
    if old is not None:
        try:
            old.close()
        except Exception:
            pass
    return connect_target(cfg, for_apply=True)


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


def apply_batch(tgt, rows: List[dict]) -> Tuple[int, int, Dict[str, int]]:
    """批量 UPDATE：先查出已存在的目标号并跳过，再更新其余行。"""
    if not rows:
        return 0, 0, {}
    goods = sorted({str(r["good_application_no"]).strip() for r in rows})
    existing: set = set()

    def _lookup():
        _ping(tgt)
        if not goods:
            return []
        ph = ",".join(["%s"] * len(goods))
        with tgt.cursor() as cur:
            cur.execute(
                "SELECT application_no FROM application WHERE application_no IN (%s)"
                % ph,
                goods,
            )
            return list(cur.fetchall())

    for row in exec_with_retry(tgt, _lookup, "lookup exists batch=%s" % len(goods)) or []:
        app = str(row.get("application_no") or "").strip()
        if app:
            existing.add(app)

    todo = [r for r in rows if str(r["good_application_no"]).strip() not in existing]
    skipped_exists = len(rows) - len(todo)
    stats: Dict[str, int] = {}
    if skipped_exists:
        stats["skip_exists"] = skipped_exists
    if not todo:
        return 0, skipped_exists, stats

    parts: List[str] = []
    params: List = []
    for r in todo:
        parts.append("SELECT %s AS bad_app, %s AS good_app, %s AS sn")
        params.extend(
            [r["bad_application_no"], r["good_application_no"], r["sn"]]
        )
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

    def _run():
        _ping(tgt)
        with tgt.cursor() as cur:
            cur.execute(sql, tuple(params))
            return int(cur.rowcount or 0)

    try:
        ok = int(exec_with_retry(tgt, _run, "apply batch size=%s" % len(todo)) or 0)
        missing = max(0, len(todo) - ok)
        if missing:
            stats["skip_missing"] = missing
        return ok, skipped_exists + missing, stats
    except pymysql.err.IntegrityError as e:
        if getattr(e, "args", None) and e.args and e.args[0] == 1062:
            try:
                tgt.rollback()
            except Exception:
                pass
            return apply_rows_fallback(tgt, todo, skipped_exists)
        raise


def apply_rows_fallback(
    tgt, rows: List[dict], already_skipped: int = 0
) -> Tuple[int, int, Dict[str, int]]:
    """批量撞 1062 时逐条修：存在则跳过。"""
    ok = skip = already_skipped
    stats: Dict[str, int] = {"fallback_row": len(rows)}
    if already_skipped:
        stats["skip_exists"] = already_skipped
    for r in rows:
        bad, good, sn = r["bad_application_no"], r["good_application_no"], r["sn"]

        def _one(b=bad, g=good, s=sn):
            _ping(tgt)
            with tgt.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM application WHERE application_no=%s LIMIT 1",
                    (g,),
                )
                if cur.fetchone():
                    return "skip_exists"
                cur.execute(
                    """
                    UPDATE application
                    SET application_no=%s
                    WHERE application_no=%s AND sn=%s
                    """,
                    (g, b, s),
                )
                return "ok" if int(cur.rowcount or 0) > 0 else "skip_missing"

        try:
            result = exec_with_retry(tgt, _one, "fallback %s->%s" % (bad, good))
        except pymysql.err.IntegrityError as e:
            if getattr(e, "args", None) and e.args and e.args[0] == 1062:
                try:
                    tgt.rollback()
                except Exception:
                    pass
                result = "skip_dup_key"
            else:
                raise
        if result == "ok":
            ok += 1
            tgt.commit()
        else:
            skip += 1
            stats[result] = stats.get(result, 0) + 1
            try:
                tgt.rollback()
            except Exception:
                pass
    return ok, skip, stats


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
    batch_size = max(1, int(spec.get("batch_size") or 50))
    print(
        "%sstart rows=%s batch_size=%s" % (label, len(chunk), batch_size),
        flush=True,
    )
    tgt = connect_target(cfg, for_apply=True)
    ok = skip = 0
    try:
        total_batches = (len(chunk) + batch_size - 1) // batch_size
        for bi in range(0, len(chunk), batch_size):
            part = chunk[bi : bi + batch_size]
            bno = bi // batch_size + 1

            def _do(p=part):
                return apply_batch(tgt, p)

            try:
                bok, bskip, bstats = _do()
                tgt.commit()
            except Exception as exc:
                print("%sbatch %s err=%s, reconnect" % (label, bno, exc), flush=True)
                try:
                    tgt.rollback()
                except Exception:
                    pass
                tgt = _reconnect(cfg, tgt)
                bok, bskip, bstats = apply_batch(tgt, part)
                tgt.commit()
            ok += bok
            skip += bskip
            for k, v in bstats.items():
                stats[k] = stats.get(k, 0) + v
            print(
                "%sbatch %s/%s updated=%s skip=%s total_ok=%s"
                % (label, bno, total_batches, bok, bskip, ok),
                flush=True,
            )
    finally:
        try:
            tgt.close()
        except Exception:
            pass
    print("%sdone ok=%s skip=%s stats=%s" % (label, ok, skip, stats), flush=True)
    return ok, skip, stats


def run_parallel(
    plan: List[dict],
    workers: int,
    env_path: str,
    batch_size: int,
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
                "batch_size": batch_size,
                "plan_chunk": chunk,
            }
        )
    print(
        "parallel apply workers=%s rows=%s batch_size=%s"
        % (len(specs), len(plan), batch_size),
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
    p.add_argument(
        "--apply-only",
        action="store_true",
        help="只读 plan-file 执行修复，跳过 orphan/loan 扫描",
    )
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--plan-file", default="/tmp/sync_app_no_from_loan_plan.json")
    p.add_argument("--loan-page-size", type=int, default=50000)
    p.add_argument("--period", type=int, default=1)
    p.add_argument("--roll-sequence", type=int, default=0)
    p.add_argument("--min-market-len", type=int, default=15)
    p.add_argument("--workers", type=int, default=4, help="apply 并行进程数")
    p.add_argument("--batch-size", type=int, default=50, help="每批 UPDATE 行数")
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

    if args.apply_only:
        if not plan_path.is_file():
            p.error("--apply-only requires existing --plan-file: %s" % plan_path)
        plan = json.loads(plan_path.read_text(encoding="utf-8"))
        print(
            "apply-only loaded plan=%s from %s (skip scan)"
            % (len(plan), plan_path),
            flush=True,
        )
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
        max(1, args.batch_size),
    )
    print("done ok=%s skip=%s skip_stats=%s" % (ok, skip, skip_stats), flush=True)
    return 0 if ok or skip else 1


if __name__ == "__main__":
    raise SystemExit(main())
