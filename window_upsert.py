#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""One-month window incremental sync: source SELECT → assemble → target UPSERT.

Standalone path (does not modify validate_and_repair.py flow).
Source DB is read-only; target rows are merged via INSERT ... ON DUPLICATE KEY UPDATE.

Usage:
  python3 window_upsert.py --date-window last-month --dry-run
  python3 window_upsert.py --date-window last-month --apply
  python3 window_upsert.py --date-window last-month --apply --tables application,loan
  python3 window_upsert.py --date-window last-month --apply --tables loan
    # 仅 upsert loan（跳过 application 写库；源端走 loan 快速加载）
"""
import argparse
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Sequence, Tuple

import ng_migration_run as mig
import validate_and_repair as vr


HERE = Path(__file__).resolve().parent
REPORT_DIR = HERE / "reports"


def _cols_except(columns: Sequence[str], *exclude: str) -> List[str]:
    skip = set(exclude)
    return [c for c in columns if c not in skip]


def _user_update_cols() -> List[str]:
    return _cols_except(mig.USER_INSERT_COLS, "user_id")


def _upsert_table_specs() -> Tuple[Tuple[str, List[str], List[str], str], ...]:
    return (
        ("user", mig.USER_INSERT_COLS, _user_update_cols(), "rows_user"),
        ("user_info", mig.USER_INFO_COLS, _cols_except(mig.USER_INFO_COLS, "user_id"), "rows_info"),
        (
            "user_bankcard",
            mig.USER_BANKCARD_COLS,
            ["bank_code", "is_default"],
            "rows_bankcard",
        ),
        (
            "user_product",
            mig.USER_PRODUCT_COLS,
            _cols_except(mig.USER_PRODUCT_COLS, "group_user_id", "product_id"),
            "product_rows",
        ),
        (
            "application",
            mig.APPLICATION_INSERT_COLS,
            _cols_except(mig.APPLICATION_INSERT_COLS, "application_no"),
            "app_rows",
        ),
        (
            "loan",
            mig.LOAN_INSERT_COLS,
            _cols_except(mig.LOAN_INSERT_COLS, "loan_no"),
            "loan_rows",
        ),
        (
            "id_mapping",
            mig.ID_MAPPING_COLS,
            ["event_time"],
            "mapping_rows",
        ),
    )


def _row_key_tuple(row: dict, key_cols: Sequence[str]) -> Tuple[Any, ...]:
    return tuple(row[c] for c in key_cols)


def _dedupe_rows(rows: Sequence[dict], key_cols: Sequence[str]) -> List[dict]:
    out: Dict[Tuple[Any, ...], dict] = {}
    for row in rows:
        out[_row_key_tuple(row, key_cols)] = row
    return list(out.values())


def _primary_key_cols(table: str, columns: Sequence[str]) -> Tuple[str, ...]:
    if table == "user":
        return ("user_id",)
    if table == "user_info":
        return ("user_id",)
    if table == "user_bankcard":
        return ("group_user_id", "bank_account_number")
    if table == "user_product":
        return ("group_user_id", "product_id")
    if table == "application":
        return ("application_no",)
    if table == "loan":
        return ("loan_no",)
    if table == "id_mapping":
        return ("id", "app_id", "mapping_id", "type")
    raise ValueError(f"unknown table: {table}")


def _filter_specs(table_sel: set) -> List[Tuple[str, List[str], List[str], str]]:
    if "all" in table_sel:
        return list(_upsert_table_specs())
    out: List[Tuple[str, List[str], List[str], str]] = []
    for table, cols, upd, attr in _upsert_table_specs():
        if table in vr.USER_TABLES:
            if "user" not in table_sel and table not in table_sel:
                continue
        elif table in vr.APP_TABLES:
            if table not in table_sel:
                continue
        elif table not in table_sel:
            continue
        out.append((table, cols, upd, attr))
    return out


def _upsert_rows_for_table(
    cfg: Dict[str, Any],
    tgt,
    table: str,
    columns: List[str],
    update_cols: List[str],
    rows: List[dict],
    batch_size: int,
    dry_run: bool,
) -> Tuple[Any, Dict[str, int]]:
    pk_cols = _primary_key_cols(table, columns)
    deduped = _dedupe_rows(rows, pk_cols)
    stats = {
        "source_rows": len(rows),
        "deduped_rows": len(deduped),
        "batches": 0,
        "affected": 0,
    }
    if not deduped:
        return tgt, stats
    if dry_run:
        stats["batches"] = (len(deduped) + batch_size - 1) // batch_size
        return tgt, stats
    tgt, affected = mig._bulk_upsert_rows(
        tgt,
        cfg,
        "target",
        table,
        columns,
        deduped,
        batch_size,
        update_cols,
    )
    stats["affected"] = affected
    stats["batches"] = (len(deduped) + batch_size - 1) // batch_size
    tgt.commit()
    return tgt, stats


def _batch_size_for_table(cfg: Dict[str, Any], table: str) -> int:
    if table in ("application", "loan", "id_mapping"):
        if table == "id_mapping":
            return max(1, int(cfg.get("id_mapping_insert_batch", 10000)))
        return max(1, int(cfg.get("app_insert_batch", 5000)))
    return max(1, int(cfg.get("user_insert_batch", 5000)))


def execute_window_upsert(
    cfg: Dict[str, Any],
    cache: vr.WindowSourceCache,
    tgt,
    table_sel: set,
    dry_run: bool,
) -> Dict[str, Dict[str, int]]:
    results: Dict[str, Dict[str, int]] = {}
    specs = _filter_specs(table_sel)
    vr.repair_log(
        f"window upsert start dry_run={int(dry_run)} "
        f"window=[{cache.window.start_sql},{cache.window.end_sql}) tables={len(specs)}"
    )
    for table, columns, update_cols, attr in specs:
        rows = list(getattr(cache, attr, []) or [])
        batch_size = _batch_size_for_table(cfg, table)
        t0 = time.time()
        tgt, stats = _upsert_rows_for_table(
            cfg, tgt, table, columns, update_cols, rows, batch_size, dry_run,
        )
        stats["elapsed_s"] = round(time.time() - t0, 1)
        results[table] = stats
        vr.repair_log(
            f"window upsert {table} "
            f"source={stats['source_rows']} deduped={stats['deduped_rows']} "
            f"batches={stats['batches']} affected={stats['affected']} "
            f"elapsed={stats['elapsed_s']}s dry_run={int(dry_run)}"
        )
    return results


def load_window_upsert_cfg(env_path: str, args: argparse.Namespace) -> Dict[str, Any]:
    cfg = vr.load_cfg(env_path)
    cfg["app_validate_batch"] = args.app_validate_batch
    cfg["user_insert_batch"] = args.user_insert_batch
    cfg["app_insert_batch"] = args.app_insert_batch
    cfg["id_mapping_insert_batch"] = args.id_mapping_insert_batch
    cfg["loan_only_batch"] = getattr(args, "loan_only_batch", 500)
    cfg["vt_preload"] = not args.no_vt_preload
    return cfg


def _bind_cfg_log(cfg: Dict[str, Any], log_file: str) -> None:
    """Route mig_log / VT preload progress to the same file as repair_log."""
    cfg["log_file"] = log_file
    p = Path(log_file)
    cfg["skip_log_file"] = str(p.with_name(p.stem + ".skip.log"))
    mig.init_log(cfg)


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="One-month window source→target UPSERT sync")
    p.add_argument("--env", default=str(HERE / "ng_migration.env"))
    p.add_argument("--date-window", choices=["last-month"], default="last-month")
    p.add_argument("--apply", action="store_true", help="Write to target (default dry-run)")
    p.add_argument("--dry-run", action="store_true", help="Only load window + print counts")
    p.add_argument("--tables", default="all", help="all | user | application | comma list")
    p.add_argument("--log-file", default="")
    p.add_argument("--reports-dir", default=str(REPORT_DIR))
    p.add_argument("--app-validate-batch", type=int, default=20000)
    p.add_argument(
        "--loan-only-batch",
        type=int,
        default=500,
        help="--tables loan 时每批源端 app_id 数（默认 500，避免目标库大查询 2013）",
    )
    p.add_argument("--user-insert-batch", type=int, default=5000)
    p.add_argument("--app-insert-batch", type=int, default=5000)
    p.add_argument("--id-mapping-insert-batch", type=int, default=10000)
    p.add_argument(
        "--no-vt-preload",
        action="store_true",
        help="Skip full vt_token_cache preload (default: preload with progress logs)",
    )
    p.add_argument("--feishu-webhook", default="")
    p.add_argument("--report-base-url", default="")
    return p.parse_args(argv)


def _write_summary(path: Path, window: vr.DateWindow, results: Dict[str, Dict[str, int]], dry_run: bool) -> str:
    lines = [
        "# Window Upsert Summary",
        "",
        f"- window: `[{window.start_sql}, {window.end_sql})`",
        f"- dry_run: `{int(dry_run)}`",
        f"- time: `{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}`",
        "",
        "## Per Table",
        "",
        "| table | source | deduped | batches | affected | elapsed_s |",
        "|-------|--------|---------|---------|----------|-----------|",
    ]
    for table, stats in results.items():
        lines.append(
            f"| {table} | {stats.get('source_rows', 0)} | {stats.get('deduped_rows', 0)} | "
            f"{stats.get('batches', 0)} | {stats.get('affected', 0)} | {stats.get('elapsed_s', 0)} |"
        )
    text = "\n".join(lines) + "\n"
    path.write_text(text, encoding="utf-8")
    return text


def main(argv: Sequence[str] = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    dry_run = not args.apply or args.dry_run
    reports_dir = Path(args.reports_dir)
    reports_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = args.log_file or str(reports_dir / f"window_upsert_{ts}.log")
    vr.set_repair_log(log_file)
    cfg = load_window_upsert_cfg(args.env, args)
    cfg["progress_log_fn"] = vr.repair_log
    _bind_cfg_log(cfg, log_file)
    table_sel = {x.strip() for x in args.tables.split(",") if x.strip()}
    window = vr.compute_date_window(args.date_window)
    t0 = time.time()
    vr.repair_log(
        f"======== WINDOW UPSERT START dry_run={int(dry_run)} "
        f"window=[{window.start_sql},{window.end_sql}) log={log_file} ========"
    )
    if cfg.get("vt_preload"):
        mig.preload_vt_token_store(cfg)
    else:
        vr.repair_log("vt preload disabled via --no-vt-preload")
    src = mig.connect_source(cfg)
    tgt = mig.connect_target(cfg)
    mig._session_opts(tgt)
    results: Dict[str, Dict[str, int]] = {}
    try:
        cache = vr.load_window_source_cache(cfg, src, window, table_sel=table_sel, tgt=tgt)
        results = execute_window_upsert(cfg, cache, tgt, table_sel, dry_run=dry_run)
    finally:
        mig._close_mysql_conn(src)
        mig._close_mysql_conn(tgt)
    summary_path = reports_dir / f"window_upsert_summary_{ts}.md"
    summary_text = _write_summary(summary_path, window, results, dry_run)
    elapsed = round(time.time() - t0, 1)
    vr.repair_log(f"======== WINDOW UPSERT END elapsed={elapsed}s summary={summary_path} ========")
    print(summary_text)
    if args.feishu_webhook:
        vr.send_feishu(args.feishu_webhook, summary_text[:3500])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
