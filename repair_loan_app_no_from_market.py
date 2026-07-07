#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""修复 loan.application_no：源库 market.applicationNo -> appId 拼正确单号。

适用：sn 对齐修完后仍剩的异常行（目标 application 表找不到对应 sn）。
  例: ng20515427-178072863512023153
  → 取后缀 178072863512023153
  → SELECT appId FROM ng_loan_market.application WHERE applicationNo='178072863512023153'
  → 更新为 ng{appId:04d}-178072863512023153

  主键冲突时只 skip，不 DELETE 任何 loan 行（需人工处理重复）。

Usage:
  python3 repair_loan_app_no_from_market.py --env ./ng_migration.env --dry-run
  python3 repair_loan_app_no_from_market.py --env ./ng_migration.env --dry-run \\
    --plan-file /tmp/fix_app_no_market_plan.json

  python3 repair_loan_app_no_from_market.py --env ./ng_migration.env --apply-only \\
    --plan-file /tmp/fix_app_no_market_plan.json --batch-size 200 --workers 4
"""
import argparse
import csv
import hashlib
import json
import multiprocessing
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pymysql
from pymysql.cursors import DictCursor

import ng_migration_run as mig
from repair_loan_no_from_audit import CommitTracker, exec_with_retry

HERE = Path(__file__).resolve().parent
LOAN_COLS = mig.LOAN_INSERT_COLS
LOAN_NO_RE = re.compile(r"^[Nn][Gg]-(\d+)-(\d{5})$")
APP_SUFFIX_RE = re.compile(r"^ng\d+-(.+)$", re.I)
BAD_APP_PREFIX_RE = re.compile(r"^ng\d{5,}-", re.I)


def _sql_escape(s: str) -> str:
    return str(s).replace("\\", "\\\\").replace("'", "''")


def save_plan(path: Path, plan: List[dict]) -> None:
    path.write_text(json.dumps(plan, ensure_ascii=False, indent=2), encoding="utf-8")


def load_plan(path: Optional[Path]) -> List[dict]:
    if path is None or not path.is_file():
        return []
    return json.loads(path.read_text(encoding="utf-8"))


def write_sql_out(plan: List[dict], out_path: Path, batch_size: int) -> None:
    lines: List[str] = []
    batch_size = max(1, batch_size)
    for bi in range(0, len(plan), batch_size):
        part = plan[bi : bi + batch_size]
        lines.append("START TRANSACTION;")
        for row in part:
            good = _sql_escape(row["good_application_no"])
            bad = _sql_escape(row["bad_application_no"])
            ln = _sql_escape(row["loan_no"])
            lines.append(
                "UPDATE loan SET application_no='%s' "
                "WHERE loan_no='%s' AND application_no='%s';"
                % (good, ln, bad)
            )
        lines.append("COMMIT;")
        lines.append("")
    out_path.write_text("\n".join(lines), encoding="utf-8")


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


def connect_target(cfg: Dict[str, str], for_apply: bool = False):
    return pymysql.connect(
        host=cfg["TARGET_HOST"],
        port=int(cfg.get("TARGET_PORT", "3306")),
        user=cfg["TARGET_USER"],
        password=cfg["TARGET_PASSWORD"],
        database=cfg.get("TARGET_DB", "ng"),
        charset="utf8mb4",
        cursorclass=DictCursor,
        connect_timeout=10,
        read_timeout=120 if for_apply else 3600,
        write_timeout=120 if for_apply else 3600,
        autocommit=False,
    )


def market_suffix(application_no: str) -> str:
    m = APP_SUFFIX_RE.match(str(application_no or "").strip())
    return m.group(1).strip() if m else ""


def is_short_loan_no(loan_no: str, max_core_len: int = 14) -> bool:
    m = LOAN_NO_RE.match(str(loan_no or "").strip())
    return bool(m and len(m.group(1)) <= max_core_len)


def fetch_pk_conflict_row(
    tgt, app_no: str, period, roll_sequence, exclude_loan_no: str
) -> Optional[dict]:
    with tgt.cursor() as cur:
        cur.execute(
            """
            SELECT loan_no, application_no FROM loan
            WHERE application_no=%s AND period=%s AND roll_sequence=%s
              AND loan_no <> %s LIMIT 1
            """,
            (app_no, period, roll_sequence, exclude_loan_no),
        )
        return cur.fetchone()


def load_all_bad_loans(tgt, min_market_len: int) -> List[dict]:
    """一次拉全表候选行进内存（分页拼接）。"""
    out: List[dict] = []
    after = ""
    batch = 2000
    while True:
        try:
            tgt.ping(reconnect=True)
        except Exception:
            pass
        rows = exec_with_retry(
            tgt,
            lambda a=after: _scan_bad_batch(tgt, a, batch),
            "scan bad loans after=%s" % (after or "(start)"),
        )
        if not rows:
            break
        after = str(rows[-1]["loan_no"])
        for row in rows:
            ln = str(row.get("loan_no") or "")
            app = str(row.get("application_no") or "")
            if not is_short_loan_no(ln):
                continue
            if not BAD_APP_PREFIX_RE.match(app):
                continue
            suffix = market_suffix(app)
            if not suffix or len(suffix) < min_market_len:
                continue
            out.append(dict(row))
        if len(rows) < batch:
            break
    return out


def _scan_bad_batch(tgt, after_loan_no: str, limit: int) -> List[dict]:
    cols = ", ".join("`%s`" % c for c in LOAN_COLS)
    sql = """
        SELECT %s FROM loan
        WHERE loan_no > %%s
          AND application_no REGEXP '^ng[0-9]{5,}-'
          AND loan_no REGEXP '^[Nn][Gg]-[0-9]+-[0-9]{5}$'
        ORDER BY loan_no ASC
        LIMIT %%s
    """ % cols
    with tgt.cursor() as cur:
        cur.execute(sql, (after_loan_no or "", limit))
        return list(cur.fetchall())


def fetch_market_app_ids(src, market_nos: List[str]) -> Dict[str, int]:
    uniq = sorted({str(x).strip() for x in market_nos if x})
    out: Dict[str, int] = {}
    m = "ng_loan_market"
    for i in range(0, len(uniq), 2000):
        part = uniq[i : i + 2000]
        ph = ",".join(["%s"] * len(part))
        with src.cursor() as cur:
            cur.execute(
                f"""
                SELECT applicationNo AS market_no, `appId` AS app_id
                FROM {m}.application
                WHERE applicationNo IN ({ph}) AND `appId` IS NOT NULL
                """,
                part,
            )
            for row in cur.fetchall():
                mk = str(row.get("market_no") or "").strip()
                if mk:
                    out[mk] = int(row["app_id"])
    return out


def build_plan(rows: List[dict], market_app_id: Dict[str, int]) -> Tuple[List[dict], Dict[str, int]]:
    plan: List[dict] = []
    stats: Dict[str, int] = {"input": len(rows)}
    for row in rows:
        loan_no = str(row["loan_no"])
        bad_app = str(row["application_no"])
        suffix = market_suffix(bad_app)
        if not suffix:
            stats["skip_no_suffix"] = stats.get("skip_no_suffix", 0) + 1
            continue
        app_id = market_app_id.get(suffix)
        if app_id is None:
            stats["skip_no_market"] = stats.get("skip_no_market", 0) + 1
            continue
        good_app = mig.format_application_no(app_id, suffix)
        if not good_app:
            stats["skip_bad_format"] = stats.get("skip_bad_format", 0) + 1
            continue
        if good_app == bad_app:
            stats["skip_already_ok"] = stats.get("skip_already_ok", 0) + 1
            continue
        plan.append(
            {
                "loan_no": loan_no,
                "bad_application_no": bad_app,
                "good_application_no": good_app,
                "market_no": suffix,
                "app_id": app_id,
                "period": row.get("period", 1),
                "roll_sequence": row.get("roll_sequence", 0),
            }
        )
    stats["plan"] = len(plan)
    return plan, stats


class RepairLog(object):
    HEADER = "ts,action,loan_no,bad_application_no,good_application_no,market_no,app_id,result"

    def __init__(self, path: Optional[str]):
        self.path = path
        self._fp = None
        if path:
            self._fp = open(path, "a", encoding="utf-8")
            if self._fp.tell() == 0:
                self._fp.write(self.HEADER + "\n")

    def close(self):
        if self._fp:
            self._fp.close()
            self._fp = None

    def record(self, action: str, row: dict, result: str):
        line = "%s,%s,%s,%s,%s,%s,%s,%s" % (
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            action,
            row.get("loan_no", ""),
            row.get("bad_application_no", ""),
            row.get("good_application_no", ""),
            row.get("market_no", ""),
            row.get("app_id", ""),
            result,
        )
        print("REPAIR %s" % line, flush=True)
        if self._fp:
            self._fp.write(line + "\n")
            self._fp.flush()



def apply_one(
    tgt, row: dict, dry_run: bool, tracker: Optional[CommitTracker], log: Optional[RepairLog]
) -> str:
    loan_no = row["loan_no"]
    bad = row["bad_application_no"]
    good = row["good_application_no"]
    period = row.get("period", 1)
    roll = row.get("roll_sequence", 0)

    with tgt.cursor() as cur:
        cur.execute(
            "SELECT application_no FROM loan WHERE loan_no=%s LIMIT 1", (loan_no,)
        )
        cur_row = cur.fetchone()
    if not cur_row:
        if log:
            log.record("skip", row, "missing")
        return "skip_missing"
    current = str(cur_row["application_no"])
    if current == good:
        if log:
            log.record("skip", row, "already_ok")
        return "skip_ok"
    if current != bad:
        if log:
            log.record("skip", row, "app_changed:%s" % current)
        return "skip_changed"

    conflict = fetch_pk_conflict_row(tgt, good, period, roll, loan_no)
    if conflict:
        conflict_ln = str(conflict["loan_no"])
        if log:
            log.record(
                "skip",
                row,
                "pk_conflict:good_app_taken_by:%s" % conflict_ln,
            )
        return "skip_pk_conflict"

    if dry_run:
        if log:
            log.record("would_update", row, "update")
        return "ok"

    try:
        with tgt.cursor() as cur:
            cur.execute(
                """
                UPDATE loan SET application_no=%s
                WHERE loan_no=%s AND application_no=%s
                """,
                (good, loan_no, bad),
            )
            if not cur.rowcount:
                if log:
                    log.record("skip", row, "no_row")
                tgt.rollback()
                return "skip_no_row"
    except pymysql.err.IntegrityError as exc:
        tgt.rollback()
        if log:
            log.record("skip", row, "integrity:%s" % exc)
        return "skip_integrity"

    if log:
        log.record("update", row, "ok")
    if tracker:
        tracker.note_write()
    else:
        tgt.commit()
    return "ok"


def apply_batch_update(tgt, rows: List[dict]) -> int:
    if not rows:
        return 0
    parts: List[str] = []
    params: List = []
    for r in rows:
        parts.append("SELECT %s AS loan_no, %s AS bad_app, %s AS good_app")
        params.extend(
            [r["loan_no"], r["bad_application_no"], r["good_application_no"]]
        )
    sql = (
        """
        UPDATE loan l
        INNER JOIN (
        """
        + " UNION ALL ".join(parts)
        + """
        ) x ON l.loan_no = x.loan_no AND l.application_no = x.bad_app
        SET l.application_no = x.good_app
        """
    )
    with tgt.cursor() as cur:
        cur.execute(sql, tuple(params))
        return int(cur.rowcount or 0)


def _apply_with_retry(
    tgt,
    cfg: Dict[str, str],
    row: dict,
    dry_run: bool,
    tracker: Optional[CommitTracker],
    log: Optional[RepairLog],
) -> Tuple[str, object]:
    conn = tgt
    for attempt in range(4):
        try:
            return apply_one(conn, row, dry_run, tracker, log), conn
        except pymysql.Error:
            try:
                conn.rollback()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass
            if attempt >= 3:
                raise
            time.sleep(1 + attempt)
            conn = connect_target(cfg, for_apply=True)
    return "skip_no_row", conn


def run_apply_batch_chunk(
    cfg: Dict[str, str],
    plan: List[dict],
    batch_size: int,
    dry_run: bool,
    repair_log: str = "",
    prefix: str = "",
) -> Tuple[int, int]:
    if batch_size <= 0:
        batch_size = 200
    tgt = connect_target(cfg, for_apply=True)
    log = RepairLog(repair_log if not dry_run else None)
    ok = skip = 0
    total_batches = (len(plan) + batch_size - 1) // batch_size if plan else 0
    try:
        for bi in range(0, len(plan), batch_size):
            part = plan[bi : bi + batch_size]
            bno = bi // batch_size + 1
            if dry_run:
                ok += len(part)
                print(
                    "%sbatch %s/%s would_update=%s"
                    % (prefix, bno, total_batches, len(part)),
                    flush=True,
                )
                continue
            try:
                n = exec_with_retry(
                    tgt,
                    lambda p=part: apply_batch_update(tgt, p),
                    "%sbatch update %s" % (prefix, bno),
                )
                tgt.commit()
                ok += n
                skip += len(part) - n
                print(
                    "%sbatch %s/%s updated=%s batch_rows=%s total_ok=%s"
                    % (prefix, bno, total_batches, n, len(part), ok),
                    flush=True,
                )
            except pymysql.err.IntegrityError:
                tgt.rollback()
                print(
                    "%sbatch %s/%s integrity_error fallback row-by-row"
                    % (prefix, bno, total_batches),
                    flush=True,
                )
                for row in part:
                    st, tgt = _apply_with_retry(
                        tgt, cfg, row, False, None, log
                    )
                    if st == "ok":
                        ok += 1
                    else:
                        skip += 1
                tgt.commit()
    finally:
        log.close()
        tgt.close()
    return ok, skip


def run_apply_batch(
    cfg: Dict[str, str],
    plan: List[dict],
    batch_size: int,
    dry_run: bool,
    repair_log: str,
) -> Tuple[int, int]:
    return run_apply_batch_chunk(
        cfg, plan, batch_size, dry_run, repair_log=repair_log
    )


def batch_worker_run(spec: dict) -> Tuple[int, int]:
    label = "[%s/%s] " % (spec["worker_id"], spec["workers"])
    chunk = spec.get("plan_chunk") or []
    if not chunk:
        return 0, 0
    cfg = load_env(Path(spec["env"]))
    print(
        "%sstart rows=%s batch_size=%s"
        % (label, len(chunk), spec["batch_size"]),
        flush=True,
    )
    ok, skip = run_apply_batch_chunk(
        cfg,
        chunk,
        spec["batch_size"],
        spec["dry_run"],
        spec.get("repair_log") or "",
        label,
    )
    print("%sdone ok=%s skip=%s" % (label, ok, skip), flush=True)
    return ok, skip


def run_parallel_batch(
    plan: List[dict],
    workers: int,
    env_path: str,
    batch_size: int,
    dry_run: bool,
    repair_log: str,
) -> Tuple[int, int]:
    workers = min(max(1, int(workers)), 16)
    chunks = split_chunks(plan, workers)
    specs = []
    for i, chunk in enumerate(chunks):
        if not chunk:
            continue
        wlog = _worker_log_path(repair_log, i) if repair_log else ""
        specs.append(
            {
                "worker_id": i,
                "workers": workers,
                "env": env_path,
                "dry_run": dry_run,
                "batch_size": batch_size,
                "plan_chunk": chunk,
                "repair_log": wlog,
            }
        )
    print(
        "parallel_batch workers=%s rows=%s batch_size=%s"
        % (len(specs), len(plan), batch_size),
        flush=True,
    )
    ctx = multiprocessing.get_context("spawn")
    with ctx.Pool(processes=len(specs)) as pool:
        results = pool.map(batch_worker_run, specs)
    ok = sum(r[0] for r in results)
    skip = sum(r[1] for r in results)
    print("parallel_batch done ok=%s skip=%s" % (ok, skip), flush=True)
    return ok, skip


def run_apply_chunk(
    tgt,
    chunk: List[dict],
    dry_run: bool,
    commit_every: int,
    log_every: int,
    log: Optional[RepairLog],
    prefix: str = "",
) -> Tuple[int, int]:
    ok = skip = 0
    tracker = CommitTracker(tgt, commit_every, dry_run)
    for i, row in enumerate(chunk, 1):
        st = exec_with_retry(
            tgt,
            lambda r=row: apply_one(tgt, r, dry_run, tracker, log),
            "%sfix %s" % (prefix, row["loan_no"]),
        )
        if st == "ok":
            ok += 1
        else:
            skip += 1
        if i % max(1, log_every) == 0:
            print(
                "%sprogress ok=%s skip=%s last=%s"
                % (prefix, ok, skip, row["loan_no"]),
                flush=True,
            )
    tracker.flush()
    return ok, skip


def split_chunks(rows: List[dict], workers: int) -> List[List[dict]]:
    n = max(1, int(workers))
    chunks: List[List[dict]] = [[] for _ in range(n)]
    for row in rows:
        key = str(row.get("loan_no") or "")
        idx = int(hashlib.md5(key.encode("utf-8")).hexdigest(), 16) % n
        chunks[idx].append(row)
    return [c for c in chunks if c]


def _worker_log_path(base: str, worker_id: int) -> str:
    p = Path(base)
    return str(p.with_name("%s.w%s%s" % (p.stem, worker_id, p.suffix or ".csv")))


def worker_run(spec: dict) -> Tuple[int, int]:
    label = "[%s/%s] " % (spec["worker_id"], spec["workers"])
    chunk = spec.get("plan_chunk") or []
    if not chunk:
        return 0, 0
    cfg = load_env(Path(spec["env"]))
    tgt = connect_target(cfg)
    log = RepairLog(spec.get("repair_log") or None)
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
            spec["commit_every"],
            spec["log_every"],
            log,
            label,
        )
        print("%sdone ok=%s skip=%s" % (label, ok, skip), flush=True)
        return ok, skip
    finally:
        log.close()
        tgt.close()


def run_parallel(
    plan: List[dict],
    workers: int,
    env_path: str,
    dry_run: bool,
    commit_every: int,
    log_every: int,
    repair_log: str,
) -> Tuple[int, int]:
    chunks = split_chunks(plan, workers)
    specs = []
    for i, chunk in enumerate(chunks):
        if not chunk:
            continue
        wlog = _worker_log_path(repair_log, i) if repair_log else ""
        specs.append(
            {
                "worker_id": i,
                "workers": workers,
                "env": env_path,
                "dry_run": dry_run,
                "commit_every": commit_every,
                "log_every": log_every,
                "plan_chunk": chunk,
                "repair_log": wlog,
            }
        )
    print("parallel workers=%s rows=%s" % (len(specs), len(plan)), flush=True)
    ctx = multiprocessing.get_context("spawn")
    with ctx.Pool(processes=len(specs)) as pool:
        results = pool.map(worker_run, specs)
    ok = sum(r[0] for r in results)
    skip = sum(r[1] for r in results)
    print("parallel done ok=%s skip=%s" % (ok, skip), flush=True)
    return ok, skip


def scan_and_build_plan(
    cfg: Dict[str, str],
    min_market_len: int,
    work_limit: int,
) -> Tuple[List[dict], Dict[str, int], List[dict]]:
    """返回 (plan, stats, no_market_rows)。"""
    t0 = time.time()
    tgt = connect_target(cfg)
    try:
        print("load target bad loans into memory ...", flush=True)
        rows = load_all_bad_loans(tgt, min_market_len)
        print(
            "target_bad_rows=%s elapsed=%.1fs" % (len(rows), time.time() - t0),
            flush=True,
        )
    finally:
        tgt.close()

    if work_limit:
        rows = rows[:work_limit]

    suffixes = sorted({market_suffix(str(r["application_no"])) for r in rows})
    suffixes = [s for s in suffixes if s]
    print("unique_market_nos=%s" % len(suffixes), flush=True)

    src = connect_source(cfg)
    try:
        t1 = time.time()
        market_app_id = fetch_market_app_ids(src, suffixes)
        print(
            "source_market_hit=%s/%s elapsed=%.1fs"
            % (len(market_app_id), len(suffixes), time.time() - t1),
            flush=True,
        )
    finally:
        src.close()

    plan, stats = build_plan(rows, market_app_id)
    no_market: List[dict] = []
    for row in rows:
        suffix = market_suffix(str(row["application_no"]))
        if not suffix:
            continue
        if suffix not in market_app_id:
            no_market.append(
                {
                    "loan_no": str(row["loan_no"]),
                    "application_no": str(row["application_no"]),
                    "market_no": suffix,
                }
            )
    stats["no_market"] = len(no_market)
    return plan, stats, no_market


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(description="Fix loan.application_no via source market appId")
    p.add_argument("--env", default=str(HERE / "ng_migration.env"))
    p.add_argument("--apply", action="store_true")
    p.add_argument(
        "--apply-only",
        action="store_true",
        help="只读 plan-file 并批更新，不再扫库",
    )
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--min-market-len", type=int, default=15)
    p.add_argument("--workers", type=int, default=4, help="批/逐条模式并发进程数")
    p.add_argument("--batch-size", type=int, default=200, help="批更新每批行数，0=逐条")
    p.add_argument(
        "--row-by-row",
        action="store_true",
        help="逐条 UPDATE（含主键冲突删行逻辑）",
    )
    p.add_argument("--commit-every", type=int, default=50)
    p.add_argument("--log-every", type=int, default=200)
    p.add_argument("--work-limit", type=int, default=0)
    p.add_argument("--plan-file", default="", help="保存/读取 plan json")
    p.add_argument("--rebuild-plan", action="store_true")
    p.add_argument("--sql-out", default="", help="导出 UPDATE SQL（IDEA 执行）")
    p.add_argument("--sql-batch", type=int, default=50, help="每个事务几条 UPDATE")
    p.add_argument("--repair-log", default="")
    args = p.parse_args(argv)
    if args.apply and args.dry_run:
        p.error("use either --apply or --dry-run")
    if args.apply_only and not args.plan_file.strip():
        p.error("--apply-only requires --plan-file")
    if args.apply_only:
        args.apply = True
    if args.row_by_row:
        args.batch_size = 0
    dry_run = not args.apply
    plan_path = Path(args.plan_file) if args.plan_file.strip() else None

    repair_log = args.repair_log or (
        "/tmp/repair_loan_app_no_market_%s.csv"
        % datetime.now().strftime("%Y%m%d_%H%M%S")
    )

    cfg = load_env(Path(args.env))
    plan: List[dict] = []
    stats: Dict[str, int] = {}
    no_market: List[dict] = []

    if args.apply_only:
        plan = load_plan(plan_path)
        print("apply-only loaded plan rows=%s" % len(plan), flush=True)
    else:
        use_cached = (
            plan_path is not None
            and plan_path.is_file()
            and not args.rebuild_plan
            and (args.sql_out or not dry_run)
        )
        if use_cached:
            plan = load_plan(plan_path)
            print(
                "loaded plan from %s rows=%s" % (plan_path, len(plan)),
                flush=True,
            )
        else:
            plan, stats, no_market = scan_and_build_plan(
                cfg, args.min_market_len, args.work_limit
            )
            print("plan_stats=%s plan=%s" % (stats, len(plan)), flush=True)
            if no_market:
                print(
                    "no_market_in_source=%s (sample below)"
                    % len(no_market),
                    flush=True,
                )
                for row in no_market[:10]:
                    print(
                        "  %s  %s  market=%s"
                        % (row["loan_no"], row["application_no"], row["market_no"]),
                        flush=True,
                    )
            if plan_path and plan:
                save_plan(plan_path, plan)
                print("saved plan -> %s" % plan_path, flush=True)

    for row in plan[:15]:
        print(
            "  %s  %s -> %s (market=%s appId=%s)"
            % (
                row["loan_no"],
                row["bad_application_no"],
                row["good_application_no"],
                row["market_no"],
                row["app_id"],
            ),
            flush=True,
        )
    if len(plan) > 15:
        print("  ... and %s more" % (len(plan) - 15), flush=True)

    if args.sql_out:
        write_sql_out(plan, Path(args.sql_out), args.sql_batch)
        print("wrote sql -> %s rows=%s" % (args.sql_out, len(plan)), flush=True)
        if dry_run and not args.apply_only:
            return 0

    if not plan:
        return 0

    env_path = str(Path(args.env).resolve())
    if args.batch_size > 0:
        if args.workers > 1:
            print(
                "apply parallel_batch workers=%s batch_size=%s rows=%s"
                % (args.workers, args.batch_size, len(plan)),
                flush=True,
            )
            ok, skip = run_parallel_batch(
                plan,
                args.workers,
                env_path,
                args.batch_size,
                dry_run,
                repair_log if not dry_run else "",
            )
        else:
            print(
                "apply batch_mode batch_size=%s rows=%s"
                % (args.batch_size, len(plan)),
                flush=True,
            )
            ok, skip = run_apply_batch(
                cfg, plan, args.batch_size, dry_run, repair_log
            )
    elif args.workers > 1:
        ok, skip = run_parallel(
            plan,
            args.workers,
            env_path,
            dry_run,
            args.commit_every,
            args.log_every,
            repair_log if not dry_run else "",
        )
    else:
        tgt = connect_target(cfg, for_apply=True)
        log = RepairLog(repair_log if not dry_run else None)
        try:
            ok, skip = run_apply_chunk(
                tgt,
                plan,
                dry_run,
                args.commit_every,
                args.log_every,
                log,
            )
        finally:
            log.close()
            tgt.close()

    print("done ok=%s skip=%s repair_log=%s" % (ok, skip, repair_log), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
