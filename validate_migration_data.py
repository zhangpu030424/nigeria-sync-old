#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Read-only validation for ng migration results.

This script does not create tables and does not write source/target business data.
It validates user/user_info existence consistency in batches and writes local
Markdown/CSV reports under reports/.
"""
import argparse
import csv
import re
import sys
import time
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Tuple

try:
    import pymysql
    from pymysql.cursors import DictCursor
except ImportError:  # pragma: no cover - exercised in real CLI use
    pymysql = None
    DictCursor = None


HERE = Path(__file__).resolve().parent
REPORT_DIR = HERE / "reports"
MUTATING_SQL_RE = re.compile(
    r"\b("
    r"insert|update|delete|replace|create|drop|alter|truncate|rename|"
    r"grant|revoke|lock|unlock|call|load|set"
    r")\b",
    re.IGNORECASE,
)
VT_TYPE_RE = re.compile(r"\bvt_type=([^\s]+)")


class SkipLogSummary:
    def __init__(self) -> None:
        self.kind_counts = Counter()
        self.vt_type_counts = Counter()


class ValidationStats:
    def __init__(self) -> None:
        self.source_user_count = 0
        self.target_user_count = 0
        self.target_user_info_count = 0
        self.source_user_missing_target_user = 0
        self.source_user_missing_target_user_info = 0
        self.user_info_without_user = 0
        self.user_without_user_info = 0
        self.details_written = 0


class RangeDiff:
    def __init__(
        self,
        source_missing_user: List[int],
        source_missing_info: List[int],
        info_without_user: List[int],
        user_without_info: List[int],
    ) -> None:
        self.source_missing_user = source_missing_user
        self.source_missing_info = source_missing_info
        self.info_without_user = info_without_user
        self.user_without_info = user_without_info


def load_env(path: Path) -> Dict[str, str]:
    env: Dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        env[k.strip()] = v.strip().strip("'\"")
    return env


def connect(cfg: Dict[str, str], kind: str):
    if pymysql is None:
        raise RuntimeError("Please install pymysql: pip install pymysql")
    kw = dict(
        charset="utf8mb4",
        cursorclass=DictCursor,
        connect_timeout=30,
        read_timeout=600,
        write_timeout=600,
        autocommit=True,
    )
    if kind == "src":
        return pymysql.connect(
            host=cfg["SOURCE_HOST"],
            port=int(cfg.get("SOURCE_PORT", "3306")),
            user=cfg["SOURCE_USER"],
            password=cfg["SOURCE_PASSWORD"],
            **kw,
        )
    return pymysql.connect(
        host=cfg["TARGET_HOST"],
        port=int(cfg.get("TARGET_PORT", "3306")),
        user=cfg["TARGET_USER"],
        password=cfg["TARGET_PASSWORD"],
        database=cfg["TARGET_DB"],
        **kw,
    )


def assert_read_only_sql(sql: str) -> None:
    stripped = sql.strip().lower()
    if not stripped.startswith("select"):
        raise ValueError(f"Only SELECT statements are allowed: {sql[:80]}")
    if MUTATING_SQL_RE.search(stripped):
        raise ValueError(f"Mutating SQL is not allowed: {sql[:80]}")


def fetch_all(conn, sql: str, params: Sequence = ()) -> List[dict]:
    assert_read_only_sql(sql)
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return list(cur.fetchall())


def fetch_one_int(conn, sql: str, params: Sequence = ()) -> int:
    rows = fetch_all(conn, sql, params)
    if not rows:
        return 0
    return int(next(iter(rows[0].values())) or 0)


def chunk_ranges(lo: int, hi: int, size: int) -> List[Tuple[int, int]]:
    if size <= 0:
        raise ValueError("batch size must be positive")
    out: List[Tuple[int, int]] = []
    cur = lo
    while cur < hi:
        nxt = min(cur + size, hi)
        out.append((cur, nxt))
        cur = nxt
    return out


def source_user_ids(src, lo: int, hi: int) -> List[int]:
    rows = fetch_all(
        src,
        """
        SELECT id AS user_id
        FROM ng_loan_market.`user`
        WHERE id > %s AND id <= %s
        ORDER BY id ASC
        """,
        (lo, hi),
    )
    return [int(r["user_id"]) for r in rows]


def target_ids_in(conn, table: str, key_col: str, ids: Sequence[int]) -> set:
    if not ids:
        return set()
    ph = ",".join(["%s"] * len(ids))
    sql = f"SELECT `{key_col}` FROM `{table}` WHERE `{key_col}` IN ({ph})"
    rows = fetch_all(conn, sql, list(ids))
    return {int(r[key_col]) for r in rows if r.get(key_col) is not None}


def find_missing_ids(conn, table: str, key_col: str, ids: Sequence[int]) -> List[int]:
    present = target_ids_in(conn, table, key_col, ids)
    return [int(i) for i in ids if int(i) not in present]


def compare_range_ids(
    source_ids: Sequence[int],
    target_user_ids: Sequence[int],
    target_info_ids: Sequence[int],
) -> RangeDiff:
    source_set = {int(i) for i in source_ids}
    user_set = {int(i) for i in target_user_ids}
    info_set = {int(i) for i in target_info_ids}
    return RangeDiff(
        source_missing_user=sorted(source_set - user_set),
        source_missing_info=sorted(source_set - info_set),
        info_without_user=sorted(info_set - user_set),
        user_without_info=sorted(user_set - info_set),
    )


def parse_skip_log(path: Path) -> SkipLogSummary:
    summary = SkipLogSummary()
    if not path.exists():
        return summary
    with path.open("r", encoding="utf-8", errors="replace") as f:
        for line in f:
            parts = line.rstrip("\n").split("\t")
            if len(parts) >= 2 and parts[1]:
                summary.kind_counts[parts[1]] += 1
            match = VT_TYPE_RE.search(line)
            if match:
                summary.vt_type_counts[match.group(1)] += 1
    return summary


def write_detail(
    writer,
    stats: ValidationStats,
    detail_limit: int,
    mismatch_type: str,
    table_name: str,
    key_name: str,
    key_value: int,
    note: str,
) -> None:
    if detail_limit >= 0 and stats.details_written >= detail_limit:
        return
    writer.writerow({
        "mismatch_type": mismatch_type,
        "table": table_name,
        "key_name": key_name,
        "key_value": key_value,
        "note": note,
    })
    stats.details_written += 1


def validate_user_tables(
    src,
    tgt,
    max_user_id: int,
    batch_size: int,
    writer,
    detail_limit: int,
) -> ValidationStats:
    stats = ValidationStats()
    stats.source_user_count = fetch_one_int(
        src,
        "SELECT COUNT(*) AS c FROM ng_loan_market.`user` WHERE id <= %s",
        (max_user_id,),
    )
    stats.target_user_count = fetch_one_int(tgt, "SELECT COUNT(*) AS c FROM `user`")
    stats.target_user_info_count = fetch_one_int(tgt, "SELECT COUNT(*) AS c FROM user_info")

    t0 = time.perf_counter()
    scanned = 0
    for lo, hi in chunk_ranges(0, max_user_id, batch_size):
        ids = source_user_ids(src, lo, hi)
        target_users = target_user_ids(tgt, lo, hi)
        target_infos = target_user_info_ids(tgt, lo, hi)
        scanned += len(ids)
        diff = compare_range_ids(ids, target_users, target_infos)
        stats.source_user_missing_target_user += len(diff.source_missing_user)
        stats.source_user_missing_target_user_info += len(diff.source_missing_info)
        stats.user_info_without_user += len(diff.info_without_user)
        stats.user_without_user_info += len(diff.user_without_info)
        for user_id in diff.source_missing_user:
            write_detail(
                writer, stats, detail_limit,
                "source_user_missing_target_user",
                "user", "user_id", user_id,
                "source user exists but target user row is missing",
            )
        for user_id in diff.source_missing_info:
            write_detail(
                writer, stats, detail_limit,
                "source_user_missing_target_user_info",
                "user_info", "user_id", user_id,
                "source user exists but target user_info row is missing",
            )
        for user_id in diff.info_without_user:
            write_detail(
                writer, stats, detail_limit,
                "target_user_info_without_user",
                "user_info", "user_id", user_id,
                "target user_info row exists but target user parent is missing",
            )
        for user_id in diff.user_without_info:
            write_detail(
                writer, stats, detail_limit,
                "target_user_without_user_info",
                "user", "user_id", user_id,
                "target user row exists but target user_info child is missing",
            )
        elapsed = time.perf_counter() - t0
        print(
            f"user validate progress scanned={scanned}/{stats.source_user_count} "
            f"range=({lo},{hi}] elapsed={elapsed:.1f}s",
            flush=True,
        )

    return stats


def target_user_info_ids(tgt, lo: int, hi: int) -> List[int]:
    rows = fetch_all(
        tgt,
        """
        SELECT user_id
        FROM user_info
        WHERE user_id > %s AND user_id <= %s
        ORDER BY user_id ASC
        """,
        (lo, hi),
    )
    return [int(r["user_id"]) for r in rows]


def target_user_ids(tgt, lo: int, hi: int) -> List[int]:
    rows = fetch_all(
        tgt,
        """
        SELECT user_id
        FROM `user`
        WHERE user_id > %s AND user_id <= %s
        ORDER BY user_id ASC
        """,
        (lo, hi),
    )
    return [int(r["user_id"]) for r in rows]


def validate_target_parent_child(
    tgt,
    max_user_id: int,
    batch_size: int,
    writer,
    detail_limit: int,
    stats: ValidationStats,
) -> None:
    for lo, hi in chunk_ranges(0, max_user_id, batch_size):
        info_ids = target_user_info_ids(tgt, lo, hi)
        if info_ids:
            missing_parent = find_missing_ids(tgt, "user", "user_id", info_ids)
            stats.user_info_without_user += len(missing_parent)
            for user_id in missing_parent:
                write_detail(
                    writer, stats, detail_limit,
                    "target_user_info_without_user",
                    "user_info", "user_id", user_id,
                    "target user_info row exists but target user parent is missing",
                )

        user_ids = target_user_ids(tgt, lo, hi)
        if user_ids:
            missing_info = find_missing_ids(tgt, "user_info", "user_id", user_ids)
            stats.user_without_user_info += len(missing_info)
            for user_id in missing_info:
                write_detail(
                    writer, stats, detail_limit,
                    "target_user_without_user_info",
                    "user", "user_id", user_id,
                    "target user row exists but target user_info child is missing",
                )


def write_summary_report(
    path: Path,
    stats: ValidationStats,
    skip_summary: SkipLogSummary,
    details_path: Path,
    started_at: str,
    elapsed: float,
) -> None:
    lines = [
        "# Migration Validation Summary",
        "",
        f"- started_at: `{started_at}`",
        f"- elapsed_seconds: `{elapsed:.1f}`",
        f"- details_csv: `{details_path}`",
        "",
        "## User Tables",
        "",
        f"- source_user_count: {stats.source_user_count}",
        f"- target_user_count: {stats.target_user_count}",
        f"- target_user_info_count: {stats.target_user_info_count}",
        f"- source_user_missing_target_user: {stats.source_user_missing_target_user}",
        f"- source_user_missing_target_user_info: {stats.source_user_missing_target_user_info}",
        f"- target_user_info_without_user: {stats.user_info_without_user}",
        f"- target_user_without_user_info: {stats.user_without_user_info}",
        f"- details_written: {stats.details_written}",
        "",
        "## Skip Log",
        "",
    ]
    if skip_summary.kind_counts:
        lines.append("### Kind Counts")
        lines.extend(f"- {k}: {v}" for k, v in skip_summary.kind_counts.most_common())
    else:
        lines.append("- skip log not found or empty")
    if skip_summary.vt_type_counts:
        lines.append("")
        lines.append("### VT Type Counts")
        lines.extend(f"- {k}: {v}" for k, v in skip_summary.vt_type_counts.most_common())
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read-only full validation for ng migration")
    parser.add_argument("--env", default=str(HERE / "ng_migration.env"), help="Path to ng_migration.env")
    parser.add_argument("--skip-log", default="", help="Path to skip log; defaults to SKIP_LOG_FILE or /tmp/ng_mig_all.skip.log")
    parser.add_argument("--max-user-id", type=int, default=0, help="Override MAX_USER_ID")
    parser.add_argument("--user-batch", type=int, default=20000, help="User id range batch size")
    parser.add_argument("--detail-limit", type=int, default=100000, help="Max CSV detail rows; -1 means unlimited")
    parser.add_argument("--reports-dir", default=str(REPORT_DIR), help="Report output directory")
    return parser.parse_args(argv)


def main(argv: Sequence[str] = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    env_path = Path(args.env)
    cfg = load_env(env_path)
    max_user_id = args.max_user_id or int(cfg.get("MAX_USER_ID", "0") or 0)
    if max_user_id <= 0:
        raise SystemExit("MAX_USER_ID is required")
    skip_path = Path(args.skip_log or cfg.get("SKIP_LOG_FILE") or "/tmp/ng_mig_all.skip.log")
    reports_dir = Path(args.reports_dir)
    reports_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    details_path = reports_dir / f"validation_mismatches_{ts}.csv"
    summary_path = reports_dir / f"validation_summary_{ts}.md"

    started = datetime.now().isoformat(timespec="seconds")
    t0 = time.perf_counter()
    skip_summary = parse_skip_log(skip_path)
    src = connect(cfg, "src")
    tgt = connect(cfg, "tgt")
    try:
        with details_path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=["mismatch_type", "table", "key_name", "key_value", "note"],
            )
            writer.writeheader()
            stats = validate_user_tables(
                src, tgt, max_user_id, args.user_batch, writer, args.detail_limit,
            )
    finally:
        src.close()
        tgt.close()

    elapsed = time.perf_counter() - t0
    write_summary_report(summary_path, stats, skip_summary, details_path, started, elapsed)
    print(f"summary: {summary_path}")
    print(f"details: {details_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
