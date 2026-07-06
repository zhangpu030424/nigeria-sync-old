#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""从 repair_loan_status20 审计 CSV 恢复 loan.application_no。

流程：只读 CSV 进内存（correct_loan_no -> application_no），再 UPDATE 目标库。
并行 --workers N 时读 .w0.csv ... .wN.csv（drop_long 行含正确 application_no）。

Usage:
  python3 restore_loan_app_no_from_audit.py \\
    --env ./ng_migration.env \\
    --audit /tmp/repair_loan_status20_20260706_093302.csv \\
    --dry-run

  python3 restore_loan_app_no_from_audit.py \\
    --env ./ng_migration.env \\
    --audit /tmp/repair_loan_status20_20260706_093302.csv \\
    --apply --commit-every 50 --workers 8
"""
import argparse
import csv
import hashlib
import multiprocessing
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pymysql
from pymysql.cursors import DictCursor

from repair_loan_no_from_audit import CommitTracker, exec_with_retry

HERE = Path(__file__).resolve().parent
LOAN_NO_RE = re.compile(r"^[Nn][Gg]-(\d+)-(\d{5})$")
RESTORE_ACTIONS = frozenset(
    {
        "drop_long",
        "rekey_long",
        "rekey_keep_app_no",
    }
)
APP_SUFFIX_RE = re.compile(r"^ng\d+-(.+)$", re.I)
TS_LEN = 19  # YYYY-MM-DD HH:MM:SS
# 只匹配 .w0.csv / .w1.csv，排除 .w0.deleted.csv / .w0.modified.csv
WORKER_REPAIR_RE = re.compile(r"\.w\d+\.csv$", re.I)


def load_env(path: Path) -> Dict[str, str]:
    cfg: Dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        cfg[k.strip()] = v.strip().strip("'\"")
    return cfg


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


def discover_repair_logs(audit_path: str) -> List[str]:
    p = Path(audit_path)
    out: List[str] = []
    if p.is_file():
        out.append(str(p))
    suffix = p.suffix or ".csv"
    for f in sorted(p.parent.glob("%s.w*%s" % (p.stem, suffix))):
        if WORKER_REPAIR_RE.search(f.name):
            out.append(str(f))
    return out


def discover_deleted_logs(audit_path: str) -> List[str]:
    p = Path(audit_path)
    suffix = p.suffix or ".csv"
    patterns = [
        "%s.deleted%s" % (p.stem, suffix),
        "%s.w*.deleted%s" % (p.stem, suffix),
    ]
    out: List[str] = []
    seen = set()
    for pat in patterns:
        for f in sorted(p.parent.glob(pat)):
            s = str(f)
            if s not in seen:
                seen.add(s)
                out.append(s)
    return out


def market_suffix(application_no: str) -> str:
    m = APP_SUFFIX_RE.match(str(application_no or "").strip())
    return m.group(1).strip() if m else ""


def is_short_loan_no(loan_no: str, min_sn_len: int = 15) -> bool:
    m = LOAN_NO_RE.match(str(loan_no or "").strip())
    return bool(m and len(m.group(1)) < min_sn_len)


def parse_repair_line(line: str) -> Optional[dict]:
    line = line.strip()
    if not line or line.startswith("ts,"):
        return None
    if line.startswith("REPAIR_AUDIT "):
        line = line[len("REPAIR_AUDIT ") :]
    row = next(csv.reader([line]))
    if len(row) < 8:
        return None
    return {
        "ts": row[0],
        "action": row[1].strip(),
        "wrong_loan_no": row[2].strip(),
        "correct_loan_no": row[3].strip(),
        "legacy_loan_no": row[4].strip(),
        "application_no": row[5].strip(),
        "app_id": row[6].strip(),
        "result": ",".join(row[7:]).strip(),
    }


def parse_deleted_line(line: str) -> Optional[dict]:
    line = line.strip()
    if not line or line.startswith("ts,"):
        return None
    if line.startswith("ROW_DELETED "):
        line = line[len("ROW_DELETED ") :]
    json_start = line.find("{")
    if json_start < 0:
        return None
    prefix = line[:json_start].rstrip(",")
    row_json = line[json_start:]
    parts = prefix.split(",")
    if len(parts) < 4:
        if len(line) > TS_LEN and line[TS_LEN] == ",":
            ts = line[:TS_LEN]
            rest = line[TS_LEN + 1 :]
            json_start = rest.find("{")
            if json_start < 0:
                return None
            parts = rest[:json_start].rstrip(",").split(",")
            if len(parts) < 3:
                return None
            return {
                "ts": ts,
                "action": parts[0].strip(),
                "loan_no": parts[1].strip(),
                "application_no": parts[2].strip(),
                "row_json": rest[json_start:],
            }
        return None
    return {
        "ts": parts[0].strip(),
        "action": parts[1].strip(),
        "loan_no": parts[2].strip(),
        "application_no": parts[3].strip(),
        "row_json": row_json,
    }


def load_plan_from_repair_files(paths: List[str]) -> Tuple[Dict[str, str], Dict[str, int]]:
    plan: Dict[str, str] = {}
    stats: Dict[str, int] = {"files": 0, "lines": 0}
    for path in paths:
        p = Path(path)
        if not p.is_file():
            stats["missing_files"] = stats.get("missing_files", 0) + 1
            continue
        stats["files"] += 1
        with p.open("r", encoding="utf-8") as fp:
            for raw in fp:
                rec = parse_repair_line(raw)
                if not rec:
                    continue
                stats["lines"] += 1
                action = rec["action"]
                stats[action] = stats.get(action, 0) + 1
                if action not in RESTORE_ACTIONS:
                    continue
                loan_no = rec["correct_loan_no"]
                app_no = rec["application_no"]
                if not loan_no or not app_no:
                    stats["skip_incomplete"] = stats.get("skip_incomplete", 0) + 1
                    continue
                plan[loan_no] = app_no
    return plan, stats


def load_market_map_from_deleted_files(paths: List[str]) -> Tuple[Dict[str, str], Dict[str, int]]:
    """market_suffix -> 正确 application_no（来自 delete_long 被删长号行）。"""
    by_market: Dict[str, str] = {}
    stats: Dict[str, int] = {"files": 0, "lines": 0, "delete_long": 0}
    for path in paths:
        p = Path(path)
        if not p.is_file():
            continue
        stats["files"] += 1
        with p.open("r", encoding="utf-8") as fp:
            for raw in fp:
                rec = parse_deleted_line(raw)
                if not rec:
                    continue
                stats["lines"] += 1
                if rec["action"] != "delete_long":
                    continue
                stats["delete_long"] += 1
                app_no = rec["application_no"]
                suffix = market_suffix(app_no)
                if app_no and suffix:
                    by_market[suffix] = app_no
    return by_market, stats


def find_loans_by_market_suffix(tgt, suffix: str) -> List[dict]:
    with tgt.cursor() as cur:
        cur.execute(
            """
            SELECT loan_no, application_no
            FROM loan
            WHERE application_no LIKE %s
            LIMIT 10
            """,
            ("%%-%s" % suffix,),
        )
        return list(cur.fetchall())


def enrich_plan_from_market_map(
    tgt, by_market: Dict[str, str], plan: Dict[str, str], min_sn_len: int
) -> int:
    """按 market 后缀逐条小查询，避免大 OR 导致 2013。"""
    if not by_market:
        return 0
    added = 0
    suffixes = sorted(by_market.keys())
    for i, suffix in enumerate(suffixes, 1):
        want = by_market[suffix]
        if not want:
            continue
        try:
            tgt.ping(reconnect=True)
        except Exception:
            pass
        rows = exec_with_retry(
            tgt,
            lambda s=suffix: find_loans_by_market_suffix(tgt, s),
            "lookup market suffix=%s" % suffix,
        )
        for row in rows or []:
            ln = str(row["loan_no"])
            if not is_short_loan_no(ln, min_sn_len):
                continue
            if ln in plan:
                continue
            cur_app = str(row["application_no"])
            if want != cur_app:
                plan[ln] = want
                added += 1
        if i % 100 == 0:
            print(
                "enrich progress %s/%s added=%s plan=%s"
                % (i, len(suffixes), added, len(plan)),
                flush=True,
            )
    return added


def fetch_current_app_no(tgt, loan_no: str) -> str:
    with tgt.cursor() as cur:
        cur.execute(
            "SELECT application_no FROM loan WHERE loan_no=%s LIMIT 1",
            (loan_no,),
        )
        row = cur.fetchone()
    return str(row["application_no"]).strip() if row and row.get("application_no") else ""


def loan_pk_exists(tgt, app_no: str, loan_no: str) -> bool:
    with tgt.cursor() as cur:
        cur.execute(
            "SELECT period, roll_sequence FROM loan WHERE loan_no=%s LIMIT 1",
            (loan_no,),
        )
        row = cur.fetchone()
    if not row:
        return False
    with tgt.cursor() as cur:
        cur.execute(
            """
            SELECT 1 FROM loan
            WHERE application_no=%s AND period=%s AND roll_sequence=%s
              AND loan_no <> %s
            LIMIT 1
            """,
            (app_no, row["period"], row["roll_sequence"], loan_no),
        )
        return cur.fetchone() is not None


def apply_one(
    tgt, loan_no: str, want_app: str, dry_run: bool, tracker: Optional[CommitTracker]
) -> str:
    current = fetch_current_app_no(tgt, loan_no)
    if not current:
        return "skip_missing"
    if current == want_app:
        return "skip_ok"
    if loan_pk_exists(tgt, want_app, loan_no):
        print(
            "skip pk_conflict loan_no=%s want=%s current=%s"
            % (loan_no, want_app, current),
            flush=True,
        )
        return "skip_pk"
    if dry_run:
        return "ok"
    with tgt.cursor() as cur:
        cur.execute(
            """
            UPDATE loan SET application_no=%s
            WHERE loan_no=%s AND application_no=%s
            """,
            (want_app, loan_no, current),
        )
        if not cur.rowcount:
            return "skip_no_row"
    if tracker:
        tracker.note_write()
    else:
        tgt.commit()
    return "ok"


def run_apply_chunk(
    tgt,
    chunk: List[dict],
    dry_run: bool,
    commit_every: int,
    log_every: int,
    prefix: str = "",
) -> Tuple[int, int]:
    ok = skip = 0
    tracker = CommitTracker(tgt, commit_every, dry_run)
    for i, row in enumerate(chunk, 1):
        st = exec_with_retry(
            tgt,
            lambda r=row: apply_one(
                tgt,
                r["loan_no"],
                r["good_application_no"],
                dry_run,
                tracker,
            ),
            "%srestore %s" % (prefix, row["loan_no"]),
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


def split_plan_chunks(items: List[dict], workers: int) -> List[List[dict]]:
    n = max(1, int(workers))
    if not items:
        return []
    chunks: List[List[dict]] = [[] for _ in range(n)]
    for row in items:
        key = str(row.get("loan_no") or "")
        idx = int(hashlib.md5(key.encode("utf-8")).hexdigest(), 16) % n
        chunks[idx].append(row)
    return [c for c in chunks if c]


def restore_worker_run(spec: dict) -> Tuple[int, int]:
    worker_id = spec["worker_id"]
    workers = spec["workers"]
    label = "[%s/%s] " % (worker_id, workers)
    chunk = spec.get("plan_chunk") or []
    if not chunk:
        return 0, 0
    cfg = load_env(Path(spec["env"]))
    tgt = connect_target(cfg)
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
            label,
        )
        print("%sdone ok=%s skip=%s" % (label, ok, skip), flush=True)
        return ok, skip
    finally:
        tgt.close()


def run_apply_parallel(
    plan_rows: List[dict],
    workers: int,
    env_path: str,
    dry_run: bool,
    commit_every: int,
    log_every: int,
) -> Tuple[int, int]:
    chunks = split_plan_chunks(plan_rows, workers)
    specs = []
    for i, chunk in enumerate(chunks):
        if not chunk:
            continue
        specs.append(
            {
                "worker_id": i,
                "workers": workers,
                "env": env_path,
                "dry_run": dry_run,
                "commit_every": commit_every,
                "log_every": log_every,
                "plan_chunk": chunk,
            }
        )
    if not specs:
        return 0, 0
    print("parallel apply workers=%s chunks=%s" % (len(specs), len(plan_rows)), flush=True)
    ctx = multiprocessing.get_context("spawn")
    with ctx.Pool(processes=len(specs)) as pool:
        results = pool.map(restore_worker_run, specs)
    total_ok = sum(r[0] for r in results)
    total_skip = sum(r[1] for r in results)
    print(
        "parallel done workers=%s ok=%s skip=%s"
        % (len(specs), total_ok, total_skip),
        flush=True,
    )
    return total_ok, total_skip


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(
        description="Restore loan.application_no from repair_loan_status20 audit CSV"
    )
    p.add_argument("--env", default=str(HERE / "ng_migration.env"))
    p.add_argument("--audit", required=True, help="repair 主审计路径（自动找 .w0..wN 与 deleted）")
    p.add_argument("--apply", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument(
        "--use-deleted",
        action="store_true",
        help="repair 计划不足时，才用 *.deleted.csv 查库补全（默认不查库）",
    )
    p.add_argument("--min-sn-len", type=int, default=15)
    p.add_argument("--workers", type=int, default=1, help="并行写库进程数（默认 1）")
    p.add_argument("--commit-every", type=int, default=50)
    p.add_argument("--log-every", type=int, default=100)
    args = p.parse_args(argv)
    if args.apply and args.dry_run:
        p.error("use either --apply or --dry-run")
    dry_run = not args.apply

    repair_files = discover_repair_logs(args.audit)
    print("repair_files=%s" % repair_files, flush=True)

    plan, repair_stats = load_plan_from_repair_files(repair_files)
    print(
        "repair_stats=%s plan_in_memory=%s"
        % (repair_stats, len(plan)),
        flush=True,
    )

    cfg = load_env(Path(args.env))

    if args.use_deleted:
        deleted_files = discover_deleted_logs(args.audit)
        print("deleted_files=%s" % deleted_files, flush=True)
        if deleted_files:
            tgt = connect_target(cfg)
            try:
                by_market, del_stats = load_market_map_from_deleted_files(deleted_files)
                need_enrich = len(plan) < del_stats.get("delete_long", 0)
                added = 0
                if need_enrich and by_market:
                    print(
                        "enrich from deleted market_map=%s"
                        % len(by_market),
                        flush=True,
                    )
                    added = enrich_plan_from_market_map(
                        tgt, by_market, plan, args.min_sn_len
                    )
                print(
                    "deleted_stats=%s plan_added=%s total_plan=%s"
                    % (del_stats, added, len(plan)),
                    flush=True,
                )
            finally:
                tgt.close()

    items = sorted(plan.items())
    plan_rows = [
        {"loan_no": ln, "good_application_no": app} for ln, app in items
    ]
    print(
        "restore_plan=%s dry_run=%s workers=%s"
        % (len(plan_rows), dry_run, args.workers),
        flush=True,
    )
    for row in plan_rows[:20]:
        print(
            "  %s -> %s" % (row["loan_no"], row["good_application_no"]),
            flush=True,
        )
    if len(plan_rows) > 20:
        print("  ... and %s more" % (len(plan_rows) - 20), flush=True)
    if not plan_rows:
        print("nothing to restore", flush=True)
        return 1

    env_path = str(Path(args.env).resolve())
    if args.workers > 1:
        ok, skip = run_apply_parallel(
            plan_rows,
            args.workers,
            env_path,
            dry_run,
            args.commit_every,
            args.log_every,
        )
    else:
        tgt = connect_target(cfg)
        try:
            ok, skip = run_apply_chunk(
                tgt,
                plan_rows,
                dry_run,
                args.commit_every,
                args.log_every,
            )
        finally:
            tgt.close()
    print("done ok=%s skip=%s" % (ok, skip), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
