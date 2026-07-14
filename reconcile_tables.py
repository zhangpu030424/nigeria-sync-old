#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""逐表对账修复：源库为准，目标库逐行比对，不一致则出 plan 再修复。

设计原则（替代旧 upsert / validate_and_repair 链路）：
  - 一次只处理一张表，控制内存与 IO
  - 目标库先并行加载到内存（可落盘 cache）
  - 源库按时间窗口拉取（默认 2026-01-01 起）
  - VT 字段：命中写 token，未命中跳过并记日志
  - 一致跳过；缺失或字段不一致 → plan → apply（insert=INSERT，update=UPDATE WHERE 主键）

目标库业务表 PK（与生产 DDL 一致）：
  user          PK (mobile, app_id, closed_time)     closed_time 毫秒 ms
  user_info     PK (user_id)
  user_bankcard PK (group_user_id, bank_account_number)  另有 id 非主键
  user_product  PK (group_user_id, product_id)
  application   PK (mobile, group_user_id, sn)       partition by mobile
                idx created_time（加载筛选用 created_time 毫秒 ms）
  loan          PK (application_no, period, roll_sequence)  partition by application_no
                idx loan_no（非主键；loan_no 可随 update 修正）

  apply：insert=INSERT；update=UPDATE WHERE 表主键
        默认 15 线程、每批 500（--apply-workers / --apply-batch）

Usage:
  # user 表
  python3 reconcile_tables.py --env ./ng_migration.env --table user \\
    --phase load-target --target-cache /tmp/reconcile_user_target.jsonl

  # user_info 表（VT 预加载常驻内存，不释放）
  python3 reconcile_tables.py --env ./ng_migration.env --table user_info \\
    --phase plan --since-date 2026-01-01 \\
    --target-cache /tmp/reconcile_user_info_target.jsonl \\
    --plan-file /tmp/reconcile_user_info_plan.jsonl \\
    --log-dir /tmp/reconcile_logs
"""
# 兼容服务器 Python 3.6+（勿使用 from __future__ import annotations，3.6 不支持）
import argparse
import json
import multiprocessing
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

import ng_migration_run as mig

HERE = Path(__file__).resolve().parent

SUPPORTED_TABLES = (
    "user", "user_info", "user_bankcard", "user_product", "application", "loan",
)
DEFAULT_MAX_TARGET_USER_ID = 100_000_000
DEFAULT_SINCE_DATE = "2026-01-01"

# --- 目标库主键 / 比对列（对齐生产 DDL）---
USER_PK = ("mobile", "app_id", "closed_time")
USER_INFO_PK = ("user_id",)
BANKCARD_PK = ("group_user_id", "bank_account_number")
PRODUCT_PK = ("group_user_id", "product_id")
USER_COMPARE_COLS = [c for c in mig.USER_INSERT_COLS if c not in USER_PK]
USER_INFO_COMPARE_COLS = [c for c in mig.USER_INFO_COLS if c not in USER_INFO_PK]
# user_bankcard.id 非主键；update 保留目标库 id，仅比对/更新 bank_code、is_default
BANKCARD_COMPARE_COLS = [
    c for c in mig.USER_BANKCARD_COLS if c not in BANKCARD_PK and c != "id"
]
PRODUCT_COMPARE_COLS = [c for c in mig.USER_PRODUCT_COLS if c not in PRODUCT_PK]
# application DDL: PK(mobile, group_user_id, sn)
APPLICATION_PK = ("mobile", "group_user_id", "sn")
APPLICATION_COMPARE_COLS = [
    c for c in mig.APPLICATION_INSERT_COLS if c not in APPLICATION_PK
]
DEFAULT_EXCLUDE_APP_IDS = (567, 568, 569, 571, 572, 573)
# 测试/批量导入脏数据 created_time，默认跳过（与 audit_loan_disbursed 一致）
DEFAULT_EXCLUDE_LOAN_CREATED_MS = (1785340800000,)
# loan DDL: PK(application_no, period, roll_sequence)；loan_no 为普通索引
LOAN_PK = ("application_no", "period", "roll_sequence")
LOAN_COMPARE_COLS = [c for c in mig.LOAN_INSERT_COLS if c not in LOAN_PK]
LOAN_APPLY_UPDATE_COLS = list(LOAN_COMPARE_COLS)


def since_date_to_unix(since_date: str) -> int:
    dt = datetime.strptime(since_date, "%Y-%m-%d")
    return int(dt.timestamp())


def since_date_to_ms(since_date: str) -> int:
    return since_date_to_unix(since_date) * 1000


def parse_exclude_app_ids(raw: str) -> Tuple[int, ...]:
    if not str(raw or "").strip():
        return DEFAULT_EXCLUDE_APP_IDS
    out: List[int] = []
    for part in str(raw).split(","):
        part = part.strip()
        if part:
            out.append(int(part))
    return tuple(out) if out else DEFAULT_EXCLUDE_APP_IDS


def parse_exclude_loan_created_ms(raw: str) -> Tuple[int, ...]:
    if not str(raw or "").strip():
        return DEFAULT_EXCLUDE_LOAN_CREATED_MS
    out: List[int] = []
    for part in str(raw).split(","):
        part = part.strip()
        if part:
            out.append(int(part))
    return tuple(out) if out else DEFAULT_EXCLUDE_LOAN_CREATED_MS


def application_no_prefix_for_app_id(app_id: int) -> str:
    """目标 application_no / loan.application_no 前缀：ng{appId:04d}-"""
    return f"ng{int(app_id):04d}-"


def application_no_matches_excluded_app(
    application_no: str,
    exclude_app_ids: Tuple[int, ...],
) -> bool:
    app_no = str(application_no or "").strip().lower()
    if not app_no:
        return False
    for app_id in exclude_app_ids:
        if app_no.startswith(application_no_prefix_for_app_id(app_id).lower()):
            return True
    return False


def sql_exclude_application_no_prefixes(
    exclude_app_ids: Tuple[int, ...],
    table_alias: str = "",
) -> Tuple[str, List[str]]:
    """生成 application_no 前缀排除 SQL（loan 对账不在库内使用，仅保留供其它脚本复用）。"""
    if not exclude_app_ids:
        return "", []
    col_ref = f"`{table_alias}`.`application_no`" if table_alias else "`application_no`"
    clauses = [f"{col_ref} NOT LIKE %s" for _ in exclude_app_ids]
    params = [f"{application_no_prefix_for_app_id(aid)}%" for aid in exclude_app_ids]
    return " AND ".join(clauses), params


def loan_created_time_ms(row: dict) -> int:
    """目标库 loan/application.created_time：毫秒时间戳（BIGINT ms）。"""
    val = row.get("created_time")
    if val in (None, ""):
        return 0
    try:
        n = int(val)
    except (TypeError, ValueError):
        return 0
    return n if n >= 10**12 else n * 1000


target_created_time_ms = loan_created_time_ms


def application_row_skip_reason(
    row: dict,
    exclude_app_ids: Tuple[int, ...],
    exclude_app_set: Optional[Set[int]] = None,
) -> Optional[str]:
    """application 目标行内存过滤：app_id 排除，不在 SQL 执行。"""
    if not exclude_app_ids:
        return None
    ex_set = exclude_app_set or {int(x) for x in exclude_app_ids}
    try:
        app_id = int(row.get("app_id") or 0)
    except (TypeError, ValueError):
        app_id = 0
    if app_id in ex_set:
        return "excluded_app"
    return None


def loan_row_skip_reason(
    row: dict,
    exclude_app_ids: Tuple[int, ...],
    exclude_created_ms: Tuple[int, ...],
    exclude_created_set: Optional[Set[int]] = None,
) -> Optional[str]:
    """loan 行内存过滤：app_id 前缀 + 指定 created_time(ms)，不在 SQL 执行。"""
    if application_no_matches_excluded_app(row.get("application_no"), exclude_app_ids):
        return "excluded_app"
    if exclude_created_ms:
        ex_set = exclude_created_set or set(int(x) for x in exclude_created_ms)
        if loan_created_time_ms(row) in ex_set:
            return "excluded_created_ms"
    return None


def filter_loan_rows_in_memory(
    rows: Iterable[dict],
    exclude_app_ids: Tuple[int, ...],
    exclude_created_ms: Tuple[int, ...],
) -> Tuple[List[dict], Dict[str, int]]:
    """拉进内存后统一过滤 loan 行。"""
    stats = {"skipped_excluded_app": 0, "skipped_excluded_created_ms": 0}
    if not exclude_app_ids and not exclude_created_ms:
        return list(rows), stats
    ex_created = set(int(x) for x in exclude_created_ms) if exclude_created_ms else set()
    kept: List[dict] = []
    for row in rows:
        reason = loan_row_skip_reason(
            row, exclude_app_ids, exclude_created_ms, ex_created,
        )
        if reason == "excluded_app":
            stats["skipped_excluded_app"] += 1
            continue
        if reason == "excluded_created_ms":
            stats["skipped_excluded_created_ms"] += 1
            continue
        kept.append(row)
    return kept, stats


def filter_loan_rows_by_excluded_app(
    rows: Iterable[dict],
    exclude_app_ids: Tuple[int, ...],
) -> Tuple[List[dict], int]:
    """loan 无 app_id：拉进内存后按 application_no 前缀过滤，不在 SQL 执行。"""
    kept, stats = filter_loan_rows_in_memory(rows, exclude_app_ids, ())
    return kept, stats["skipped_excluded_app"]


def application_key(row: dict) -> Tuple[str, int, str]:
    return (
        str(row["mobile"]),
        int(row["group_user_id"]),
        str(row["sn"]),
    )


def user_key(row: dict) -> Tuple[str, int, int]:
    return (
        str(row["mobile"]),
        int(row.get("app_id") or 0),
        int(row.get("closed_time") if row.get("closed_time") is not None else 0),
    )


def loan_key(row: dict) -> Tuple[str, int, int]:
    return (
        str(row["application_no"]),
        int(row.get("period") if row.get("period") is not None else 1),
        int(row.get("roll_sequence") if row.get("roll_sequence") is not None else 0),
    )


def default_paths(table: str) -> Dict[str, str]:
    return {
        "target_cache": f"/tmp/reconcile_{table}_target.jsonl",
        "plan_file": f"/tmp/reconcile_{table}_plan.jsonl",
    }


def _worker_load_env(env_path: str) -> Dict[str, Any]:
    for line in Path(env_path).read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ[k.strip()] = v.strip().strip("'\"")
    mig.ENV_FILE = Path(env_path)
    return mig.load_env()


def _now_ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


class ReconcileLogger:
    def __init__(self, log_dir: Path, table: str) -> None:
        log_dir.mkdir(parents=True, exist_ok=True)
        self.main_path = log_dir / f"reconcile_{table}.log"
        self.vt_skip_path = log_dir / f"vt_skip_{table}.jsonl"
        self.apply_path = log_dir / f"apply_{table}.jsonl"
        self._main_fp = self.main_path.open("a", encoding="utf-8")

    def close(self) -> None:
        self._main_fp.close()

    def log(self, msg: str) -> None:
        line = f"[{_now_ts()}] {msg}"
        print(line, flush=True)
        self._main_fp.write(line + "\n")
        self._main_fp.flush()

    def vt_skip(self, record: dict) -> None:
        record = {**record, "ts": _now_ts()}
        with self.vt_skip_path.open("a", encoding="utf-8") as fp:
            fp.write(json.dumps(record, ensure_ascii=False) + "\n")

    def apply_audit(self, record: dict) -> None:
        record = {**record, "ts": _now_ts()}
        with self.apply_path.open("a", encoding="utf-8") as fp:
            fp.write(json.dumps(record, ensure_ascii=False) + "\n")


def write_jsonl(path: Path, rows: Iterable[dict]) -> int:
    n = 0
    with path.open("w", encoding="utf-8") as fp:
        for row in rows:
            fp.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")
            n += 1
    return n


def read_jsonl(path: Path) -> List[dict]:
    out: List[dict] = []
    with path.open(encoding="utf-8") as fp:
        for line in fp:
            line = line.strip()
            if line:
                out.append(json.loads(line))
    return out


def normalize_cell(col: str, val: Any) -> Any:
    if val is None:
        return None
    if col in ("info", "schemes", "product_scheme_param", "repayment_plan"):
        return normalize_info_json(val)
    if col in (
        "created_time", "submited_time", "reviewed_time", "disbursed_time",
        "last_paid_time", "paid_off_time", "lock_expire_time", "reg_time", "closed_time",
        "test_flag", "paid_time",
    ):
        try:
            return int(val) if val not in (None, "") else None
        except (TypeError, ValueError):
            return 0 if col != "paid_time" else None
    if col in (
        "is_open", "is_default", "credit_amount", "unpaid_amount",
        "locked_amount", "available_amount", "is_test", "is_first_apply",
        "is_auto_apply", "term", "periods", "repayment_method", "status",
        "loan_amount", "principal", "total_amount", "disbursed_amount",
        "period", "roll_sequence", "interest", "admin_fee", "service_fee",
        "tax_fee", "penalty_amount", "reduction_amount", "paid_amount",
    ):
        try:
            return int(val)
        except (TypeError, ValueError):
            return 0
    if col in ("app_id", "group_user_id", "info_user_id", "user_id"):
        try:
            return int(val)
        except (TypeError, ValueError):
            return None
    if isinstance(val, str):
        s = val.strip()
        if col == "password":
            return s
        return s if s else None
    return val


def normalize_info_json(val: Any) -> Optional[str]:
    if val is None or val == "":
        return None
    try:
        if isinstance(val, dict):
            obj = val
        else:
            obj = json.loads(str(val))
    except (json.JSONDecodeError, TypeError, ValueError):
        return str(val).strip() or None
    return json.dumps(obj, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def compare_rows(
    expected: dict,
    actual: Optional[dict],
    cols: Sequence[str],
) -> Tuple[bool, Dict[str, Dict[str, Any]]]:
    if actual is None:
        return False, {"__missing__": {"target": None, "expected": "row absent"}}
    diff: Dict[str, Dict[str, Any]] = {}
    for col in cols:
        ev = normalize_cell(col, expected.get(col))
        av = normalize_cell(col, actual.get(col))
        if ev != av:
            diff[col] = {"target": av, "expected": ev}
    return (not diff), diff


def _target_cols_sql(columns: Sequence[str]) -> str:
    return ", ".join(f"`{c}`" for c in columns)


def _load_target_shard(spec: dict) -> Tuple[int, List[dict], Dict[str, int]]:
    worker_id = int(spec["worker_id"])
    workers = int(spec["workers"])
    max_uid = int(spec["max_user_id"])
    page_size = int(spec["page_size"])
    env_path = str(spec["env_path"])
    table = str(spec["table"])
    columns: List[str] = list(spec["columns"])
    cols_sql = spec["cols_sql"]
    exclude_app_ids: Tuple[int, ...] = tuple(spec.get("exclude_app_ids") or ())

    label = f"[target {table} {worker_id}/{workers}]"
    stats = {"scanned": 0}
    rows: List[dict] = []

    cfg = _worker_load_env(env_path)
    tgt = mig.connect_target(cfg)
    # MOD(user_id) 分片：数据在低段也不会只压在一个 worker
    last_id = 0
    shard = worker_id - 1
    # user 表有 app_id：SQL 直接排除，少扫少传更快；user_info 无 app_id 不加此条件
    exclude_sql = ""
    exclude_params: List[Any] = []
    if table == "user" and exclude_app_ids:
        ex_ph = ",".join(["%s"] * len(exclude_app_ids))
        exclude_sql = f"AND app_id NOT IN ({ex_ph})"
        exclude_params = list(exclude_app_ids)
    t0 = time.time()
    print(
        f"{label} load start max_user_id={max_uid} "
        f"MOD(user_id,{workers})={shard} page_size={page_size} "
        f"exclude_app_ids={exclude_app_ids if table == 'user' else '()'}",
        flush=True,
    )
    try:
        while True:
            with tgt.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT {cols_sql}
                    FROM `{table}`
                    WHERE user_id > %s
                      AND user_id < %s
                      AND MOD(user_id, %s) = %s
                      {exclude_sql}
                    ORDER BY user_id ASC
                    LIMIT %s
                    """,
                    (last_id, max_uid, workers, shard, *exclude_params, page_size),
                )
                batch = cur.fetchall()
            if not batch:
                break
            for row in batch:
                rows.append({k: row[k] for k in columns})
            last_id = int(batch[-1]["user_id"])
            stats["scanned"] = len(rows)
            if stats["scanned"] % 100_000 < page_size:
                print(
                    f"{label} progress rows={stats['scanned']} last_id={last_id} "
                    f"elapsed={time.time()-t0:.1f}s",
                    flush=True,
                )
            if len(batch) < page_size:
                break
    finally:
        tgt.close()
    print(
        f"{label} done rows={len(rows)} elapsed={time.time()-t0:.1f}s",
        flush=True,
    )
    return worker_id, rows, stats


def parallel_load_target_by_user_id(
    env_path: Path,
    table: str,
    columns: Sequence[str],
    max_user_id: int,
    load_workers: int,
    page_size: int,
    logger: ReconcileLogger,
    exclude_app_ids: Tuple[int, ...] = (),
) -> Dict[int, dict]:
    workers = max(1, min(load_workers, 32))
    cols_sql = _target_cols_sql(columns)
    specs = [
        {
            "worker_id": i + 1,
            "workers": workers,
            "max_user_id": max_user_id,
            "page_size": page_size,
            "env_path": str(env_path.resolve()),
            "table": table,
            "columns": list(columns),
            "cols_sql": cols_sql,
            "exclude_app_ids": list(exclude_app_ids),
        }
        for i in range(workers)
    ]
    logger.log(
        f"load target {table}: workers={workers} max_user_id={max_user_id} "
        f"page_size={page_size} exclude_app_ids={exclude_app_ids}"
    )
    t0 = time.time()
    merged: Dict[int, dict] = {}
    if workers == 1:
        _, rows, _ = _load_target_shard(specs[0])
        for row in rows:
            merged[int(row["user_id"])] = row
    else:
        ctx = multiprocessing.get_context("spawn")
        with ctx.Pool(processes=workers) as pool:
            parts = pool.map(_load_target_shard, specs)
        for _, chunk, _ in sorted(parts, key=lambda x: x[0]):
            for row in chunk:
                merged[int(row["user_id"])] = row
    logger.log(
        f"load target {table} done rows={len(merged)} elapsed={time.time()-t0:.1f}s"
    )
    return merged


def bankcard_key(row: dict) -> Tuple[int, str]:
    return (int(row["group_user_id"]), str(row["bank_account_number"]))


def product_key(row: dict) -> Tuple[int, str]:
    return (int(row["group_user_id"]), str(row["product_id"]))


def _load_target_group_user_shard(spec: dict) -> Tuple[int, List[dict], Dict[str, int]]:
    worker_id = int(spec["worker_id"])
    workers = int(spec["workers"])
    max_gid = int(spec["max_group_user_id"])
    page_size = int(spec["page_size"])
    env_path = str(spec["env_path"])
    table = str(spec["table"])
    columns: List[str] = list(spec["columns"])
    cols_sql = spec["cols_sql"]
    order_tail = str(spec.get("order_tail") or "group_user_id ASC")

    label = f"[target {table} gid {worker_id}/{workers}]"
    stats = {"scanned": 0}
    rows: List[dict] = []

    cfg = _worker_load_env(env_path)
    tgt = mig.connect_target(cfg)
    last_gid = 0
    shard = worker_id - 1
    t0 = time.time()
    print(
        f"{label} load start max_gid={max_gid} "
        f"MOD(group_user_id,{workers})={shard} page_size={page_size}",
        flush=True,
    )
    try:
        while True:
            with tgt.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT {cols_sql}
                    FROM `{table}`
                    WHERE group_user_id > %s
                      AND group_user_id < %s
                      AND MOD(group_user_id, %s) = %s
                    ORDER BY {order_tail}
                    LIMIT %s
                    """,
                    (last_gid, max_gid, workers, shard, page_size),
                )
                batch = cur.fetchall()
            if not batch:
                break
            for row in batch:
                rows.append({k: row[k] for k in columns})
            last_gid = int(batch[-1]["group_user_id"])
            stats["scanned"] = len(rows)
            if stats["scanned"] % 100_000 < page_size:
                print(
                    f"{label} progress rows={stats['scanned']} last_gid={last_gid} "
                    f"elapsed={time.time()-t0:.1f}s",
                    flush=True,
                )
            if len(batch) < page_size:
                break
    finally:
        tgt.close()
    print(
        f"{label} done rows={len(rows)} elapsed={time.time()-t0:.1f}s",
        flush=True,
    )
    return worker_id, rows, stats


def parallel_load_target_by_group_user_id(
    env_path: Path,
    table: str,
    columns: Sequence[str],
    max_group_user_id: int,
    load_workers: int,
    page_size: int,
    logger: ReconcileLogger,
    order_tail: str = "group_user_id ASC",
) -> Dict[Tuple[Any, ...], dict]:
    workers = max(1, min(load_workers, 32))
    cols_sql = _target_cols_sql(columns)
    specs = [
        {
            "worker_id": i + 1,
            "workers": workers,
            "max_group_user_id": max_group_user_id,
            "page_size": page_size,
            "env_path": str(env_path.resolve()),
            "table": table,
            "columns": list(columns),
            "cols_sql": cols_sql,
            "order_tail": order_tail,
        }
        for i in range(workers)
    ]
    logger.log(
        f"load target {table}: workers={workers} group_user_id<{max_group_user_id} "
        f"page_size={page_size}"
    )
    t0 = time.time()
    merged: Dict[Tuple[Any, ...], dict] = {}
    if workers == 1:
        _, rows, _ = _load_target_group_user_shard(specs[0])
        key_fn = bankcard_key if table == "user_bankcard" else product_key
        for row in rows:
            merged[key_fn(row)] = row
    else:
        ctx = multiprocessing.get_context("spawn")
        with ctx.Pool(processes=workers) as pool:
            parts = pool.map(_load_target_group_user_shard, specs)
        key_fn = bankcard_key if table == "user_bankcard" else product_key
        for _, chunk, _ in sorted(parts, key=lambda x: x[0]):
            for row in chunk:
                merged[key_fn(row)] = row
    logger.log(
        f"load target {table} done rows={len(merged)} elapsed={time.time()-t0:.1f}s"
    )
    return merged


def parallel_load_target_users(
    cfg: Dict[str, Any],
    env_path: Path,
    max_user_id: int,
    load_workers: int,
    page_size: int,
    logger: ReconcileLogger,
    exclude_app_ids: Tuple[int, ...] = DEFAULT_EXCLUDE_APP_IDS,
) -> Dict[Tuple[Any, ...], dict]:
    by_uid = parallel_load_target_by_user_id(
        env_path, "user", mig.USER_INSERT_COLS, max_user_id,
        load_workers, page_size, logger, exclude_app_ids=exclude_app_ids,
    )
    merged: Dict[Tuple[Any, ...], dict] = {}
    for row in by_uid.values():
        merged[user_key(row)] = row
    logger.log(f"load target user re-keyed by PK {USER_PK} rows={len(merged)}")
    return merged


def _select_source_users_since(
    src,
    since_date: str,
    lo: int,
    hi: int,
    max_user_id: int,
    exclude_app_ids: Tuple[int, ...] = DEFAULT_EXCLUDE_APP_IDS,
) -> List[dict]:
    m = "ng_loan_market"
    ex_ph = ",".join(["%s"] * len(exclude_app_ids)) if exclude_app_ids else ""
    exclude_sql = f"AND u.`appId` NOT IN ({ex_ph})" if exclude_app_ids else ""
    sql = f"""
        SELECT
            u.id AS user_id, u.`appId` AS app_id,
            u.mobile AS mobile_raw,
            CASE WHEN u.mobile LIKE '+234%%' THEN u.mobile
                 WHEN u.mobile LIKE '234%%' THEN CONCAT('+', u.mobile)
                 WHEN u.mobile LIKE '0%%' THEN CONCAT('+234', SUBSTRING(u.mobile, 2))
                 ELSE CONCAT('+234', u.mobile) END AS mobile,
            ap.name AS app_name,
            CASE WHEN u.`isCancel` IN (1, '1') THEN UNIX_TIMESTAMP(u.updated) * 1000 ELSE 0 END AS closed_time,
            u.`deviceId` AS reg_device_id,
            IFNULL(reg_d.deviceUUID, '') AS reg_device_uuid,
            UNIX_TIMESTAMP(u.created) * 1000 AS reg_time,
            0 AS test_flag
        FROM {m}.`user` u
        LEFT JOIN {m}.app ap ON ap.id = u.`appId`
        LEFT JOIN {m}.device reg_d ON reg_d.id = u.`deviceId`
        WHERE u.created >= %s
          AND u.id > %s AND u.id <= %s
          AND u.id < %s
          {exclude_sql}
        ORDER BY u.id ASC
    """
    params: List[Any] = [since_date, lo, hi, max_user_id]
    if exclude_app_ids:
        params.extend(exclude_app_ids)
    with src.cursor() as cur:
        cur.execute(sql, params)
        return list(cur.fetchall())


def _source_user_id_bounds(
    src,
    since_date: str,
    max_user_id: int,
    exclude_app_ids: Tuple[int, ...] = DEFAULT_EXCLUDE_APP_IDS,
) -> Tuple[int, int, int]:
    m = "ng_loan_market"
    ex_ph = ",".join(["%s"] * len(exclude_app_ids)) if exclude_app_ids else ""
    exclude_sql = f"AND u.`appId` NOT IN ({ex_ph})" if exclude_app_ids else ""
    with src.cursor() as cur:
        cur.execute(
            f"""
            SELECT MIN(u.id) AS min_id, MAX(u.id) AS max_id, COUNT(*) AS cnt
            FROM {m}.`user` u
            WHERE u.created >= %s AND u.id < %s
              {exclude_sql}
            """,
            (since_date, max_user_id, *exclude_app_ids) if exclude_app_ids
            else (since_date, max_user_id),
        )
        row = cur.fetchone() or {}
    min_id = int(row.get("min_id") or 0)
    max_id = int(row.get("max_id") or 0)
    cnt = int(row.get("cnt") or 0)
    return min_id, max_id, cnt


def build_expected_user_rows(
    cfg: Dict[str, Any],
    src,
    lo: int,
    hi: int,
    since_date: str,
    max_user_id: int,
    vt: mig.VtTokenResolver,
    logger: ReconcileLogger,
) -> Tuple[List[dict], Dict[str, int]]:
    stats = {
        "source_rows": 0,
        "vt_skip": 0,
        "built": 0,
    }
    raw_rows = _select_source_users_since(src, since_date, lo, hi, max_user_id)
    stats["source_rows"] = len(raw_rows)
    if not raw_rows:
        return [], stats

    prefix = f"[plan {lo},{hi}]"
    _, lookups = mig._fetch_user_batch_lookups(src, cfg, raw_rows, lo, hi, prefix)
    mig._register_user_batch_vt(vt, raw_rows, lookups)
    vt.prefetch()

    out: List[dict] = []
    for row in raw_rows:
        mobile_raw = row.get("mobile") or ""
        user_ctx = f"user_id={row['user_id']}"
        mobile_token = vt.resolve_token(
            mig.VtTokenResolver.VT_MOBILE,
            mobile_raw,
            context=f"{user_ctx} field=mobile",
            row_data=row,
            log_miss=False,
        )
        if not mobile_token:
            stats["vt_skip"] += 1
            logger.vt_skip(
                {
                    "table": "user",
                    "user_id": int(row["user_id"]),
                    "app_id": row.get("app_id"),
                    "vt_type": "mobile",
                    "raw": mobile_raw,
                    "reason": "vt_token_cache miss",
                }
            )
            continue
        row["mobile_lookup"] = row.get("mobile")
        row["mobile"] = mobile_token
        out.append(row)

    mig._prepare_user_insert_rows(out, lookups)
    built: List[dict] = []
    for row in out:
        built.append({c: row.get(c) for c in mig.USER_INSERT_COLS})
    stats["built"] = len(built)
    return built, stats


def plan_user_table(
    cfg: Dict[str, Any],
    target_by_key: Dict[Tuple[Any, ...], dict],
    since_date: str,
    max_user_id: int,
    source_batch: int,
    vt: mig.VtTokenResolver,
    logger: ReconcileLogger,
) -> Tuple[List[dict], Dict[str, int]]:
    src = mig.connect_source(cfg)
    stats = {
        "source_total": 0,
        "skip_ok": 0,
        "plan_insert": 0,
        "plan_update": 0,
        "vt_skip": 0,
        "source_batches": 0,
    }
    plan: List[dict] = []
    try:
        min_id, max_id, cnt = _source_user_id_bounds(src, since_date, max_user_id)
        logger.log(
            f"source user since={since_date}: count={cnt} id=[{min_id},{max_id}]"
        )
        if cnt <= 0:
            return plan, stats

        lo = min_id - 1
        while lo < max_id:
            hi = min(lo + source_batch, max_id)
            stats["source_batches"] += 1
            expected_rows, batch_stats = build_expected_user_rows(
                cfg, src, lo, hi, since_date, max_user_id, vt, logger,
            )
            stats["vt_skip"] += batch_stats.get("vt_skip", 0)
            stats["source_total"] += batch_stats.get("source_rows", 0)

            for exp in expected_rows:
                pk = user_key(exp)
                actual = target_by_key.get(pk)
                ok, diff = compare_rows(exp, actual, USER_COMPARE_COLS)
                if ok:
                    stats["skip_ok"] += 1
                    continue
                action = "insert" if actual is None else "update"
                if action == "insert":
                    stats["plan_insert"] += 1
                else:
                    stats["plan_update"] += 1
                plan.append(
                    {
                        "table": "user",
                        "action": action,
                        "key": list(pk),
                        "user_id": int(exp.get("user_id") or 0),
                        "diff": diff,
                        "row": exp,
                    }
                )
            lo = hi
            if stats["source_batches"] % 20 == 0:
                logger.log(
                    f"plan progress batches={stats['source_batches']} "
                    f"ok={stats['skip_ok']} insert={stats['plan_insert']} "
                    f"update={stats['plan_update']} vt_skip={stats['vt_skip']}"
                )
    finally:
        mig._close_mysql_conn(src)
    return plan, stats


def _bulk_update_rows_by_pk(
    conn,
    cfg: Dict[str, Any],
    table: str,
    pk_cols: Sequence[str],
    update_cols: Sequence[str],
    rows: List[dict],
    batch_size: int,
) -> Tuple[Any, int]:
    """UPDATE ... SET ... WHERE 主键条件（不用 upsert）。"""
    if not rows or not update_cols:
        return conn, 0
    where_sql = " AND ".join(f"{mig._quote_col(c)}=%s" for c in pk_cols)
    set_sql = ", ".join(f"{mig._quote_col(c)}=%s" for c in update_cols)
    sql = f"UPDATE `{table}` SET {set_sql} WHERE {where_sql}"
    affected = 0
    batch_size = max(1, int(batch_size))
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        params = [
            [row.get(c) for c in update_cols] + [row.get(c) for c in pk_cols]
            for row in batch
        ]
        conn = mig._ensure_mysql_conn(conn, cfg, "target")
        with conn.cursor() as cur:
            cur.executemany(sql, params)
            affected += int(cur.rowcount or 0)
    return conn, affected


def _chunk_list(items: Sequence[Any], size: int) -> List[List[Any]]:
    n = max(1, int(size))
    return [list(items[i:i + n]) for i in range(0, len(items), n)]


def _apply_insert_batch_worker(spec: dict) -> Dict[str, Any]:
    cfg = spec["cfg"]
    table = str(spec["table"])
    columns: List[str] = list(spec["columns"])
    batch: List[dict] = list(spec["batch"])
    batch_no = int(spec["batch_no"])
    conn = mig.connect_target(cfg)
    mig._session_opts(conn)
    try:
        conn, affected = mig._bulk_insert_rows(
            conn, cfg, "target", table, columns, batch, len(batch),
        )
        conn.commit()
        return {
            "kind": "insert", "batch_no": batch_no,
            "rows": len(batch), "affected": int(affected or 0),
        }
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        mig._close_mysql_conn(conn)


def _apply_update_batch_worker(spec: dict) -> Dict[str, Any]:
    cfg = spec["cfg"]
    table = str(spec["table"])
    pk_cols: List[str] = list(spec["pk_cols"])
    update_cols: List[str] = list(spec["update_cols"])
    batch: List[dict] = list(spec["batch"])
    batch_no = int(spec["batch_no"])
    conn = mig.connect_target(cfg)
    mig._session_opts(conn)
    try:
        conn, affected = _bulk_update_rows_by_pk(
            conn, cfg, table, pk_cols, update_cols, batch, len(batch),
        )
        conn.commit()
        return {
            "kind": "update", "batch_no": batch_no,
            "rows": len(batch), "affected": int(affected or 0),
        }
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        mig._close_mysql_conn(conn)


def _parallel_apply_batches(
    cfg: Dict[str, Any],
    table: str,
    rows: List[dict],
    batch_size: int,
    apply_workers: int,
    kind: str,
    logger: ReconcileLogger,
    columns: Optional[Sequence[str]] = None,
    pk_cols: Optional[Sequence[str]] = None,
    update_cols: Optional[Sequence[str]] = None,
) -> Tuple[int, int]:
    """多线程按批 apply；每批独立连接并 commit。"""
    if not rows:
        return 0, 0
    batches = _chunk_list(rows, batch_size)
    workers = max(1, min(int(apply_workers), 32))
    specs: List[dict] = []
    for i, batch in enumerate(batches, start=1):
        spec: Dict[str, Any] = {
            "cfg": cfg, "table": table, "batch": batch, "batch_no": i,
        }
        if kind == "insert":
            spec["columns"] = list(columns or [])
        else:
            spec["pk_cols"] = list(pk_cols or [])
            spec["update_cols"] = list(update_cols or [])
        specs.append(spec)

    worker_fn = (
        _apply_insert_batch_worker if kind == "insert" else _apply_update_batch_worker
    )
    logger.log(
        f"apply {table} {kind} parallel workers={workers} "
        f"batches={len(specs)} batch_size={batch_size} rows={len(rows)}"
    )
    total_affected = 0
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(worker_fn, spec) for spec in specs]
        for fut in as_completed(futures):
            result = fut.result()
            total_affected += int(result["affected"])
            logger.log(
                f"apply {table} {kind} batch={result['batch_no']}/{len(specs)} "
                f"rows={result['rows']} affected={result['affected']}"
            )
    logger.log(
        f"apply {table} {kind} parallel done batches={len(specs)} "
        f"affected={total_affected} elapsed={time.time()-t0:.1f}s"
    )
    return total_affected, len(specs)


def _apply_reconcile_plan(
    cfg: Dict[str, Any],
    plan: List[dict],
    table: str,
    columns: Sequence[str],
    pk_cols: Sequence[str],
    update_cols: Sequence[str],
    batch_size: int,
    apply_workers: int,
    dry_run: bool,
    logger: ReconcileLogger,
) -> Dict[str, int]:
    """insert → INSERT；update → UPDATE WHERE 表主键（多线程分批）。"""
    stats = {"insert": 0, "update": 0, "applied": 0, "batches": 0}
    if not plan:
        return stats

    inserts = [p["row"] for p in plan if p.get("action") == "insert"]
    updates = [p["row"] for p in plan if p.get("action") == "update"]
    stats["insert"] = len(inserts)
    stats["update"] = len(updates)
    batch_size = max(1, int(batch_size))
    apply_workers = max(1, int(apply_workers))

    if dry_run:
        logger.log(
            f"apply dry-run table={table} insert={stats['insert']} "
            f"update={stats['update']} pk={pk_cols} "
            f"workers={apply_workers} batch_size={batch_size}"
        )
        return stats

    t0 = time.time()
    total_affected = 0
    if inserts:
        n, b = _parallel_apply_batches(
            cfg, table, inserts, batch_size, apply_workers, "insert", logger,
            columns=columns,
        )
        total_affected += n
        stats["batches"] += b
    if updates:
        n, b = _parallel_apply_batches(
            cfg, table, updates, batch_size, apply_workers, "update", logger,
            pk_cols=pk_cols, update_cols=update_cols,
        )
        total_affected += n
        stats["batches"] += b
    stats["applied"] = total_affected
    logger.log(
        f"apply {table} done insert={stats['insert']} update={stats['update']} "
        f"affected={total_affected} batches={stats['batches']} "
        f"workers={apply_workers} batch_size={batch_size} "
        f"elapsed={time.time()-t0:.1f}s"
    )
    for p in plan:
        audit: Dict[str, Any] = {
            "table": table,
            "action": p.get("action"),
            "pk": {c: p["row"].get(c) for c in pk_cols},
            "key": p.get("key"),
            "diff_keys": list(p.get("diff", {}).keys()),
        }
        for extra in ("user_id", "group_user_id", "application_no", "loan_no", "sn"):
            if extra in p:
                audit[extra] = p.get(extra)
        logger.apply_audit(audit)
    return stats


def apply_user_plan(
    cfg: Dict[str, Any],
    plan: List[dict],
    batch_size: int,
    apply_workers: int,
    dry_run: bool,
    logger: ReconcileLogger,
) -> Dict[str, int]:
    return _apply_reconcile_plan(
        cfg, plan, "user", mig.USER_INSERT_COLS, USER_PK,
        USER_COMPARE_COLS, batch_size, apply_workers, dry_run, logger,
    )


def apply_user_info_plan(
    cfg: Dict[str, Any],
    plan: List[dict],
    batch_size: int,
    apply_workers: int,
    dry_run: bool,
    logger: ReconcileLogger,
) -> Dict[str, int]:
    return _apply_reconcile_plan(
        cfg, plan, "user_info", mig.USER_INFO_COLS, USER_INFO_PK,
        USER_INFO_COMPARE_COLS, batch_size, apply_workers, dry_run, logger,
    )


def load_or_build_target_cache(
    cfg: Dict[str, Any],
    env_path: Path,
    cache_path: Path,
    table: str,
    columns: Sequence[str],
    max_user_id: int,
    load_workers: int,
    page_size: int,
    logger: ReconcileLogger,
    from_cache: bool,
    loader=None,
    key_fn=None,
) -> Dict[Any, dict]:
    if key_fn is None:
        key_fn = lambda row: int(row["user_id"])  # user_info 等按 user_id

    if from_cache and cache_path.is_file():
        logger.log(f"load target from cache {cache_path}")
        t0 = time.time()
        target_by_key: Dict[Any, dict] = {}
        skipped = 0
        exclude_set = set(DEFAULT_EXCLUDE_APP_IDS) if table == "user" else set()
        for row in read_jsonl(cache_path):
            if exclude_set:
                try:
                    if int(row.get("app_id") or 0) in exclude_set:
                        skipped += 1
                        continue
                except (TypeError, ValueError):
                    pass
            target_by_key[key_fn(row)] = row
        logger.log(
            f"cache loaded rows={len(target_by_key)} skipped_excluded_app={skipped} "
            f"elapsed={time.time()-t0:.1f}s"
        )
        return target_by_key

    if loader is None:
        target_by_key = parallel_load_target_by_user_id(
            env_path, table, columns, max_user_id, load_workers, page_size, logger,
            exclude_app_ids=DEFAULT_EXCLUDE_APP_IDS if table == "user" else (),
        )
        if table == "user":
            target_by_key = {user_key(row): row for row in target_by_key.values()}
    else:
        target_by_key = loader(
            cfg, env_path, max_user_id, load_workers, page_size, logger,
        )
    logger.log(f"write target cache {cache_path}")
    t0 = time.time()
    n = write_jsonl(cache_path, target_by_key.values())
    logger.log(f"cache written rows={n} elapsed={time.time()-t0:.1f}s")
    return target_by_key


def load_or_build_target_cache_by_key(
    env_path: Path,
    cache_path: Path,
    table: str,
    columns: Sequence[str],
    max_group_user_id: int,
    load_workers: int,
    page_size: int,
    logger: ReconcileLogger,
    from_cache: bool,
    key_fn,
    order_tail: str = "group_user_id ASC",
) -> Dict[Tuple[Any, ...], dict]:
    if from_cache and cache_path.is_file():
        logger.log(f"load target from cache {cache_path}")
        t0 = time.time()
        target_by_key: Dict[Tuple[Any, ...], dict] = {}
        for row in read_jsonl(cache_path):
            target_by_key[key_fn(row)] = row
        logger.log(
            f"cache loaded rows={len(target_by_key)} elapsed={time.time()-t0:.1f}s"
        )
        return target_by_key

    target_by_key = parallel_load_target_by_group_user_id(
        env_path, table, columns, max_group_user_id,
        load_workers, page_size, logger, order_tail=order_tail,
    )
    logger.log(f"write target cache {cache_path}")
    t0 = time.time()
    n = write_jsonl(cache_path, target_by_key.values())
    logger.log(f"cache written rows={n} elapsed={time.time()-t0:.1f}s")
    return target_by_key


def setup_preloads(cfg: Dict[str, Any], logger: ReconcileLogger, vt_preload: bool) -> None:
    """VT / LUP 预加载进进程内存，plan 阶段全程保留不释放。"""
    if vt_preload and cfg.get("vt_token_enable", True):
        logger.log("VT preload start (kept in memory for whole run) ...")
        vt_n = mig.preload_vt_token_store(cfg)
        logger.log(f"VT preload done rows={vt_n}")
    elif not vt_preload:
        logger.log("VT preload skipped (--no-vt-preload); batch lookup per source chunk")
    if cfg.get("lup_preload", True):
        lup_n = mig.preload_lup_store(cfg)
        logger.log(f"LUP preload done rows={lup_n}")


def _emergency_contact_mobile_raw(item: Any) -> str:
    if isinstance(item, (list, tuple)) and len(item) >= 3:
        mobile = item[2]
    elif isinstance(item, dict):
        mobile = (
            item.get("mobile") or item.get("contactNumber") or item.get("contact_number")
        )
    else:
        mobile = None
    return str(mobile).strip() if mobile is not None else ""


def _check_user_info_vt_requirements(
    ud: Optional[dict],
    vt: mig.VtTokenResolver,
    user_id: int,
    vt_enabled: bool,
    logger: ReconcileLogger,
) -> Optional[str]:
    """需 VT 的字段未命中则返回 skip 原因；否则 None。"""
    if not vt_enabled:
        return None
    ud = ud or {}
    user_ctx = f"user_id={user_id}"
    bvn_raw = (ud.get("bvn") or "").strip()
    if bvn_raw:
        token = vt.resolve_token(
            mig.VtTokenResolver.VT_ID_NUMBER,
            bvn_raw,
            context=f"{user_ctx} field=id_number",
            row_data={"user_id": user_id, "bvn": bvn_raw},
            log_miss=False,
        )
        if not token:
            logger.vt_skip(
                {
                    "table": "user_info",
                    "user_id": user_id,
                    "vt_type": "id_number",
                    "raw": bvn_raw,
                    "reason": "vt_token_cache miss",
                }
            )
            return "id_number"

    parsed = mig._parse_emergency_contact(ud.get("emergencyContact"))
    if isinstance(parsed, list):
        for item in parsed:
            mobile_raw = _emergency_contact_mobile_raw(item)
            if not mobile_raw:
                continue
            name = None
            relation = None
            if isinstance(item, (list, tuple)):
                if len(item) >= 1:
                    name = item[0]
                if len(item) >= 2:
                    relation = item[1]
            elif isinstance(item, dict):
                name = item.get("name")
                relation = item.get("relation")
            token = mig._resolve_contact_mobile(
                vt, mobile_raw, context=f"{user_ctx} field=emergency_contact",
                name=name, relation=relation,
            )
            if not token:
                logger.vt_skip(
                    {
                        "table": "user_info",
                        "user_id": user_id,
                        "vt_type": "emergency_contact",
                        "raw": mobile_raw,
                        "reason": "vt_token_cache miss",
                    }
                )
                return "emergency_contact"
    return None


def build_expected_user_info_rows(
    cfg: Dict[str, Any],
    src,
    lo: int,
    hi: int,
    since_date: str,
    max_user_id: int,
    vt: mig.VtTokenResolver,
    logger: ReconcileLogger,
) -> Tuple[List[dict], Dict[str, int]]:
    stats = {"source_rows": 0, "vt_skip": 0, "built": 0}
    raw_rows = _select_source_users_since(src, since_date, lo, hi, max_user_id)
    stats["source_rows"] = len(raw_rows)
    if not raw_rows:
        return [], stats

    prefix = f"[user_info plan {lo},{hi}]"
    _, lookups = mig._fetch_user_batch_lookups(src, cfg, raw_rows, lo, hi, prefix)
    mig._register_user_batch_vt(vt, raw_rows, lookups)
    vt.prefetch()

    vt_enabled = cfg.get("vt_token_enable", True)
    ud_by_user = lookups.get("ud_by_user") or {}
    ok_users: List[dict] = []
    for row in raw_rows:
        user_id = int(row["user_id"])
        skip_reason = _check_user_info_vt_requirements(
            ud_by_user.get(user_id), vt, user_id, vt_enabled, logger,
        )
        if skip_reason:
            stats["vt_skip"] += 1
            continue
        ok_users.append(row)

    info_rows = mig._build_user_info_rows(ok_users, lookups, vt if ok_users else None)
    built = [{c: row.get(c) for c in mig.USER_INFO_COLS} for row in info_rows]
    stats["built"] = len(built)
    return built, stats


def plan_user_info_table(
    cfg: Dict[str, Any],
    target_by_id: Dict[int, dict],
    since_date: str,
    max_user_id: int,
    source_batch: int,
    vt: mig.VtTokenResolver,
    logger: ReconcileLogger,
) -> Tuple[List[dict], Dict[str, int]]:
    src = mig.connect_source(cfg)
    stats = {
        "source_total": 0,
        "skip_ok": 0,
        "plan_insert": 0,
        "plan_update": 0,
        "vt_skip": 0,
        "source_batches": 0,
    }
    plan: List[dict] = []
    try:
        min_id, max_id, cnt = _source_user_id_bounds(src, since_date, max_user_id)
        logger.log(
            f"source user_info (via user.created>={since_date}): count={cnt} "
            f"id=[{min_id},{max_id}]"
        )
        if cnt <= 0:
            return plan, stats

        lo = min_id - 1
        while lo < max_id:
            hi = min(lo + source_batch, max_id)
            stats["source_batches"] += 1
            expected_rows, batch_stats = build_expected_user_info_rows(
                cfg, src, lo, hi, since_date, max_user_id, vt, logger,
            )
            stats["vt_skip"] += batch_stats.get("vt_skip", 0)
            stats["source_total"] += batch_stats.get("source_rows", 0)

            for exp in expected_rows:
                uid = int(exp["user_id"])
                actual = target_by_id.get(uid)
                ok, diff = compare_rows(exp, actual, USER_INFO_COMPARE_COLS)
                if ok:
                    stats["skip_ok"] += 1
                    continue
                action = "insert" if actual is None else "update"
                if action == "insert":
                    stats["plan_insert"] += 1
                else:
                    stats["plan_update"] += 1
                plan.append(
                    {
                        "table": "user_info",
                        "action": action,
                        "key": [uid],
                        "user_id": uid,
                        "diff": diff,
                        "row": exp,
                    }
                )
            lo = hi
            if stats["source_batches"] % 20 == 0:
                logger.log(
                    f"plan progress batches={stats['source_batches']} "
                    f"ok={stats['skip_ok']} insert={stats['plan_insert']} "
                    f"update={stats['plan_update']} vt_skip={stats['vt_skip']}"
                )
    finally:
        mig._close_mysql_conn(src)
    return plan, stats


def _run_reconcile_phases(
    args: argparse.Namespace,
    cfg: Dict[str, Any],
    table: str,
    columns: Sequence[str],
    plan_fn,
    apply_fn,
    loader=None,
) -> int:
    env_path = Path(args.env).resolve()
    mig.ENV_FILE = env_path
    log_dir = Path(args.log_dir)
    logger = ReconcileLogger(log_dir, table)
    cache_path = Path(args.target_cache)
    plan_path = Path(args.plan_file)
    phase = args.phase
    dry_run = not args.apply

    if phase in ("load-target", "plan", "all"):
        setup_preloads(cfg, logger, args.vt_preload)

    target_by_key: Dict[Any, dict] = {}
    if phase in ("load-target", "plan", "all"):
        cache_key_fn = user_key if table == "user" else None
        target_by_key = load_or_build_target_cache(
            cfg,
            env_path,
            cache_path,
            table,
            columns,
            args.max_target_user_id,
            args.load_workers,
            args.page_size,
            logger,
            from_cache=args.from_cache and phase != "load-target",
            loader=loader,
            key_fn=cache_key_fn,
        )
        if phase == "load-target":
            logger.log("phase load-target done")
            logger.close()
            return 0

    plan: List[dict] = []
    if phase in ("plan", "all"):
        vt = mig.VtTokenResolver(
            mig.connect_source(cfg),
            enabled=cfg.get("vt_token_enable", True),
            chunk=cfg.get("vt_token_chunk", 2000),
            vt_db=cfg.get("vt_token_db", "ng_loan_market"),
        )
        logger.log(f"plan start table={table} since={args.since_date} dry_run={dry_run}")
        t0 = time.time()
        plan, plan_stats = plan_fn(
            cfg, target_by_key, args.since_date, args.max_target_user_id,
            args.source_batch, vt, logger,
        )
        mig._close_mysql_conn(vt.conn)
        n = write_jsonl(plan_path, plan)
        logger.log(
            f"plan done file={plan_path} rows={n} stats={plan_stats} "
            f"elapsed={time.time()-t0:.1f}s"
        )

    if phase == "apply":
        if not plan_path.is_file():
            logger.log(f"ERROR plan file missing: {plan_path}")
            logger.close()
            return 1
        plan = read_jsonl(plan_path)
        logger.log(f"loaded plan rows={len(plan)}")

    if phase in ("apply", "all"):
        if phase == "all" and not plan:
            logger.log("apply skipped: empty plan")
        else:
            apply_stats = apply_fn(
                cfg, plan, args.apply_batch, args.apply_workers, dry_run, logger,
            )
            logger.log(f"apply stats={apply_stats}")

    logger.close()
    return 0


def _source_users_batch_stats(
    cfg: Dict[str, Any],
    src,
    lo: int,
    hi: int,
    since_date: str,
    max_user_id: int,
    prefix: str,
) -> Tuple[List[dict], Dict[str, Any], set]:
    raw_rows = _select_source_users_since(src, since_date, lo, hi, max_user_id)
    if not raw_rows:
        return [], {}, set()
    _, lookups = mig._fetch_user_batch_lookups(src, cfg, raw_rows, lo, hi, prefix)
    user_ids = {int(r["user_id"]) for r in raw_rows}
    return raw_rows, lookups, user_ids


def build_expected_bankcard_rows(
    cfg: Dict[str, Any],
    src,
    lo: int,
    hi: int,
    since_date: str,
    max_user_id: int,
    vt: mig.VtTokenResolver,
    logger: ReconcileLogger,
) -> Tuple[List[dict], Dict[str, int]]:
    stats = {"source_users": 0, "vt_skip": 0, "built": 0}
    prefix = f"[bankcard plan {lo},{hi}]"
    _, lookups, user_ids = _source_users_batch_stats(
        cfg, src, lo, hi, since_date, max_user_id, prefix,
    )
    stats["source_users"] = len(user_ids)
    if not user_ids:
        return [], stats

    ud_by_user = lookups.get("ud_by_user") or {}
    for uid in user_ids:
        ud = ud_by_user.get(uid)
        if ud:
            vt.register(mig.VtTokenResolver.VT_BANK, ud.get("bankAccount"))
    vt.prefetch()

    vt_enabled = cfg.get("vt_token_enable", True)
    skip_users: set = set()
    for uid in user_ids:
        ud = ud_by_user.get(uid)
        if not ud:
            continue
        bank_account = (ud.get("bankAccount") or "").strip()
        if not bank_account:
            continue
        if not vt_enabled:
            continue
        token = vt.resolve_token(
            mig.VtTokenResolver.VT_BANK,
            bank_account,
            context=f"user_id={uid} field=bank",
            row_data={"user_id": uid, "bank_account": bank_account},
            log_miss=False,
        )
        if not token:
            stats["vt_skip"] += 1
            skip_users.add(uid)
            logger.vt_skip(
                {
                    "table": "user_bankcard",
                    "group_user_id": uid,
                    "vt_type": "bank_account",
                    "raw": bank_account,
                    "reason": "vt_token_cache miss",
                }
            )

    allowed = user_ids - skip_users
    rows = mig._build_bankcard_rows(
        lookups, vt, allowed_user_ids=allowed, cfg=cfg,
    )
    built = [{c: row.get(c) for c in mig.USER_BANKCARD_COLS} for row in rows]
    stats["built"] = len(built)
    return built, stats


def build_expected_user_product_rows(
    cfg: Dict[str, Any],
    src,
    lo: int,
    hi: int,
    since_date: str,
    max_user_id: int,
    vt: mig.VtTokenResolver,
    logger: ReconcileLogger,
) -> Tuple[List[dict], Dict[str, int]]:
    del vt  # user_product 无 VT 字段
    stats = {"source_users": 0, "built": 0}
    prefix = f"[user_product plan {lo},{hi}]"
    _, lookups, user_ids = _source_users_batch_stats(
        cfg, src, lo, hi, since_date, max_user_id, prefix,
    )
    stats["source_users"] = len(user_ids)
    if not user_ids:
        return [], stats
    prod_src = [
        p for p in (lookups.get("prod_rows") or [])
        if int(p["userId"]) in user_ids
    ]
    rows = mig._build_user_product_rows(prod_src)
    built = [{c: row.get(c) for c in mig.USER_PRODUCT_COLS} for row in rows]
    stats["built"] = len(built)
    return built, stats


def _plan_composite_table(
    cfg: Dict[str, Any],
    target_by_key: Dict[Tuple[Any, ...], dict],
    since_date: str,
    max_user_id: int,
    source_batch: int,
    table: str,
    compare_cols: Sequence[str],
    key_fn,
    build_fn,
    logger: ReconcileLogger,
    vt: mig.VtTokenResolver,
) -> Tuple[List[dict], Dict[str, int]]:
    src = mig.connect_source(cfg)
    stats = {
        "source_total": 0,
        "skip_ok": 0,
        "plan_insert": 0,
        "plan_update": 0,
        "vt_skip": 0,
        "source_batches": 0,
    }
    plan: List[dict] = []
    try:
        min_id, max_id, cnt = _source_user_id_bounds(src, since_date, max_user_id)
        logger.log(
            f"source {table} (user.created>={since_date}): users={cnt} id=[{min_id},{max_id}]"
        )
        if cnt <= 0:
            return plan, stats

        lo = min_id - 1
        while lo < max_id:
            hi = min(lo + source_batch, max_id)
            stats["source_batches"] += 1
            expected_rows, batch_stats = build_fn(
                cfg, src, lo, hi, since_date, max_user_id, vt, logger,
            )
            stats["vt_skip"] += batch_stats.get("vt_skip", 0)
            stats["source_total"] += batch_stats.get("source_users", 0)

            for exp in expected_rows:
                pk = key_fn(exp)
                actual = target_by_key.get(pk)
                ok, diff = compare_rows(exp, actual, compare_cols)
                if ok:
                    stats["skip_ok"] += 1
                    continue
                action = "insert" if actual is None else "update"
                row_out = dict(exp)
                if actual is not None and "id" in actual and "id" in row_out:
                    row_out["id"] = actual["id"]
                if action == "insert":
                    stats["plan_insert"] += 1
                else:
                    stats["plan_update"] += 1
                plan.append(
                    {
                        "table": table,
                        "action": action,
                        "key": list(pk),
                        "group_user_id": int(exp["group_user_id"]),
                        "diff": diff,
                        "row": row_out,
                    }
                )
            lo = hi
            if stats["source_batches"] % 20 == 0:
                logger.log(
                    f"plan progress batches={stats['source_batches']} "
                    f"ok={stats['skip_ok']} insert={stats['plan_insert']} "
                    f"update={stats['plan_update']} vt_skip={stats['vt_skip']}"
                )
    finally:
        mig._close_mysql_conn(src)
    return plan, stats


def plan_user_bankcard_table(
    cfg: Dict[str, Any],
    target_by_key: Dict[Tuple[Any, ...], dict],
    since_date: str,
    max_user_id: int,
    source_batch: int,
    vt: mig.VtTokenResolver,
    logger: ReconcileLogger,
) -> Tuple[List[dict], Dict[str, int]]:
    return _plan_composite_table(
        cfg, target_by_key, since_date, max_user_id, source_batch,
        "user_bankcard", BANKCARD_COMPARE_COLS, bankcard_key,
        build_expected_bankcard_rows, logger, vt,
    )


def plan_user_product_table(
    cfg: Dict[str, Any],
    target_by_key: Dict[Tuple[Any, ...], dict],
    since_date: str,
    max_user_id: int,
    source_batch: int,
    vt: mig.VtTokenResolver,
    logger: ReconcileLogger,
) -> Tuple[List[dict], Dict[str, int]]:
    return _plan_composite_table(
        cfg, target_by_key, since_date, max_user_id, source_batch,
        "user_product", PRODUCT_COMPARE_COLS, product_key,
        build_expected_user_product_rows, logger, vt,
    )


def _apply_composite_plan(
    cfg: Dict[str, Any],
    plan: List[dict],
    table: str,
    columns: Sequence[str],
    pk_cols: Sequence[str],
    update_cols: Sequence[str],
    batch_size: int,
    apply_workers: int,
    dry_run: bool,
    logger: ReconcileLogger,
) -> Dict[str, int]:
    return _apply_reconcile_plan(
        cfg, plan, table, columns, pk_cols, update_cols,
        batch_size, apply_workers, dry_run, logger,
    )


def apply_user_bankcard_plan(
    cfg: Dict[str, Any],
    plan: List[dict],
    batch_size: int,
    apply_workers: int,
    dry_run: bool,
    logger: ReconcileLogger,
) -> Dict[str, int]:
    return _apply_composite_plan(
        cfg, plan, "user_bankcard", mig.USER_BANKCARD_COLS,
        BANKCARD_PK, BANKCARD_COMPARE_COLS,
        batch_size, apply_workers, dry_run, logger,
    )


def apply_user_product_plan(
    cfg: Dict[str, Any],
    plan: List[dict],
    batch_size: int,
    apply_workers: int,
    dry_run: bool,
    logger: ReconcileLogger,
) -> Dict[str, int]:
    return _apply_composite_plan(
        cfg, plan, "user_product", mig.USER_PRODUCT_COLS,
        PRODUCT_PK, PRODUCT_COMPARE_COLS,
        batch_size, apply_workers, dry_run, logger,
    )


def _run_reconcile_phases_composite(
    args: argparse.Namespace,
    cfg: Dict[str, Any],
    table: str,
    columns: Sequence[str],
    key_fn,
    plan_fn,
    apply_fn,
    order_tail: str = "group_user_id ASC",
    need_vt: bool = True,
) -> int:
    env_path = Path(args.env).resolve()
    mig.ENV_FILE = env_path
    log_dir = Path(args.log_dir)
    logger = ReconcileLogger(log_dir, table)
    cache_path = Path(args.target_cache)
    plan_path = Path(args.plan_file)
    phase = args.phase
    dry_run = not args.apply

    if phase in ("load-target", "plan", "all") and need_vt:
        setup_preloads(cfg, logger, args.vt_preload)

    target_by_key: Dict[Tuple[Any, ...], dict] = {}
    if phase in ("load-target", "plan", "all"):
        target_by_key = load_or_build_target_cache_by_key(
            env_path,
            cache_path,
            table,
            columns,
            args.max_target_user_id,
            args.load_workers,
            args.page_size,
            logger,
            from_cache=args.from_cache and phase != "load-target",
            key_fn=key_fn,
            order_tail=order_tail,
        )
        if phase == "load-target":
            logger.log("phase load-target done")
            logger.close()
            return 0

    plan: List[dict] = []
    if phase in ("plan", "all"):
        vt = mig.VtTokenResolver(
            mig.connect_source(cfg),
            enabled=cfg.get("vt_token_enable", True),
            chunk=cfg.get("vt_token_chunk", 2000),
            vt_db=cfg.get("vt_token_db", "ng_loan_market"),
        )
        logger.log(f"plan start table={table} since={args.since_date} dry_run={dry_run}")
        t0 = time.time()
        plan, plan_stats = plan_fn(
            cfg, target_by_key, args.since_date, args.max_target_user_id,
            args.source_batch, vt, logger,
        )
        mig._close_mysql_conn(vt.conn)
        n = write_jsonl(plan_path, plan)
        logger.log(
            f"plan done file={plan_path} rows={n} stats={plan_stats} "
            f"elapsed={time.time()-t0:.1f}s"
        )

    if phase == "apply":
        if not plan_path.is_file():
            logger.log(f"ERROR plan file missing: {plan_path}")
            logger.close()
            return 1
        plan = read_jsonl(plan_path)
        logger.log(f"loaded plan rows={len(plan)}")

    if phase in ("apply", "all"):
        if phase == "all" and not plan:
            logger.log("apply skipped: empty plan")
        else:
            apply_stats = apply_fn(
                cfg, plan, args.apply_batch, args.apply_workers, dry_run, logger,
            )
            logger.log(f"apply stats={apply_stats}")

    logger.close()
    return 0


# ---------------------------------------------------------------------------
# application
# ---------------------------------------------------------------------------

def _load_target_application_shard(spec: dict) -> Tuple[int, List[dict], Dict[str, int]]:
    worker_id = int(spec["worker_id"])
    workers = int(spec["workers"])
    since_ms = int(spec["since_ms"])
    page_size = int(spec["page_size"])
    env_path = str(spec["env_path"])
    columns: List[str] = list(spec["columns"])
    cols_sql = spec["cols_sql"]
    exclude_app_ids: Tuple[int, ...] = tuple(spec["exclude_app_ids"])

    label = f"[target application {worker_id}/{workers}]"
    stats = {"scanned": 0}
    rows: List[dict] = []
    cfg = _worker_load_env(env_path)
    tgt = mig.connect_target(cfg)
    last_app_no = ""
    ex_ph = ",".join(["%s"] * len(exclude_app_ids)) if exclude_app_ids else ""
    exclude_sql = f"AND app_id NOT IN ({ex_ph})" if exclude_app_ids else ""
    t0 = time.time()
    print(
        f"{label} load start created_time_ms>={since_ms} "
        f"exclude_app_ids={exclude_app_ids} (SQL NOT IN)",
        flush=True,
    )
    try:
        while True:
            with tgt.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT {cols_sql}
                    FROM `application`
                    WHERE created_time >= %s
                      {exclude_sql}
                      AND application_no IS NOT NULL AND application_no <> ''
                      AND MOD(CRC32(application_no), %s) = %s
                      AND application_no > %s
                    ORDER BY application_no ASC
                    LIMIT %s
                    """,
                    (
                        since_ms,
                        *exclude_app_ids,
                        workers,
                        worker_id - 1,
                        last_app_no,
                        page_size,
                    ),
                )
                batch = cur.fetchall()
            if not batch:
                break
            for row in batch:
                rows.append({k: row[k] for k in columns})
            last_app_no = str(batch[-1]["application_no"])
            stats["scanned"] = len(rows)
            if stats["scanned"] % 100_000 < page_size:
                print(
                    f"{label} progress rows={stats['scanned']} last={last_app_no} "
                    f"elapsed={time.time()-t0:.1f}s",
                    flush=True,
                )
            if len(batch) < page_size:
                break
    finally:
        tgt.close()
    print(
        f"{label} done rows={len(rows)} elapsed={time.time()-t0:.1f}s",
        flush=True,
    )
    return worker_id, rows, stats


def parallel_load_target_applications(
    env_path: Path,
    columns: Sequence[str],
    since_ms: int,
    exclude_app_ids: Tuple[int, ...],
    load_workers: int,
    page_size: int,
    logger: ReconcileLogger,
) -> Dict[Tuple[Any, ...], dict]:
    workers = max(1, min(load_workers, 32))
    cols_sql = _target_cols_sql(columns)
    specs = [
        {
            "worker_id": i + 1,
            "workers": workers,
            "since_ms": since_ms,
            "page_size": page_size,
            "env_path": str(env_path.resolve()),
            "columns": list(columns),
            "cols_sql": cols_sql,
            "exclude_app_ids": list(exclude_app_ids),
        }
        for i in range(workers)
    ]
    logger.log(
        f"load target application: workers={workers} created_time_ms>={since_ms} "
        f"exclude_app_ids={exclude_app_ids} (SQL NOT IN) page_size={page_size}"
    )
    t0 = time.time()
    merged: Dict[Tuple[Any, ...], dict] = {}
    if workers == 1:
        _, chunk, _ = _load_target_application_shard(specs[0])
        for row in chunk:
            merged[application_key(row)] = row
    else:
        ctx = multiprocessing.get_context("spawn")
        with ctx.Pool(processes=workers) as pool:
            parts = pool.map(_load_target_application_shard, specs)
        for _, chunk, _ in sorted(parts, key=lambda x: x[0]):
            for row in chunk:
                merged[application_key(row)] = row
    logger.log(
        f"load target application done rows={len(merged)} "
        f"elapsed={time.time()-t0:.1f}s"
    )
    return merged


def load_or_build_target_application_cache(
    env_path: Path,
    cache_path: Path,
    since_ms: int,
    exclude_app_ids: Tuple[int, ...],
    load_workers: int,
    page_size: int,
    logger: ReconcileLogger,
    from_cache: bool,
) -> Dict[Tuple[Any, ...], dict]:
    if from_cache and cache_path.is_file():
        logger.log(f"load target from cache {cache_path}")
        t0 = time.time()
        target_by_key: Dict[Tuple[Any, ...], dict] = {}
        skipped = 0
        exclude_app_set = {int(x) for x in exclude_app_ids} if exclude_app_ids else set()
        for row in read_jsonl(cache_path):
            if application_row_skip_reason(row, exclude_app_ids, exclude_app_set):
                skipped += 1
                continue
            target_by_key[application_key(row)] = row
        logger.log(
            f"cache loaded rows={len(target_by_key)} skipped_excluded_app={skipped} "
            f"elapsed={time.time()-t0:.1f}s"
        )
        return target_by_key

    target_by_key = parallel_load_target_applications(
        env_path, mig.APPLICATION_INSERT_COLS, since_ms,
        exclude_app_ids, load_workers, page_size, logger,
    )
    logger.log(f"write target cache {cache_path}")
    t0 = time.time()
    n = write_jsonl(cache_path, target_by_key.values())
    logger.log(f"cache written rows={n} elapsed={time.time()-t0:.1f}s")
    return target_by_key


def _select_application_source_since(
    src,
    lo: int,
    hi: int,
    since_unix: int,
    exclude_app_ids: Tuple[int, ...],
) -> List[dict]:
    m, c = "ng_loan_market", "ng_loan_core"
    ex_ph = ",".join(["%s"] * len(exclude_app_ids))
    sql = f"""
        SELECT
            a.applicationNo AS application_no,
            CASE WHEN a.mobile LIKE '+234%%' THEN a.mobile
                 WHEN a.mobile LIKE '234%%' THEN CONCAT('+', a.mobile)
                 WHEN a.mobile LIKE '0%%' THEN CONCAT('+234', SUBSTRING(a.mobile, 2))
                 ELSE CONCAT('+234', a.mobile) END AS mobile,
            'ng01' AS bid, a.`appId` AS app_id, '1.0.0' AS app_version,
            a.`userId` AS user_id, a.applicationNo AS sn,
            CASE WHEN a.`repeatLoan` = 0 THEN 1 ELSE 0 END AS is_first_apply,
            IFNULL(NULLIF(a.gaid, ''), NULL) AS gaid_idfa,
            IFNULL(CAST(a.`deviceDataId` AS CHAR), '') AS device_uuid,
            IFNULL(a.bankCode, '') AS bank_code,
            IFNULL(a.bankAccount, '') AS bank_account_number,
            CAST(a.`productId` AS CHAR) AS product_id,
            a.term, a.shouldLoanAmount AS should_loan_amount,
            a.amount AS amount, a.repayment AS repayment,
            a.disburseAmount AS disburse_amount,
            a.applyDate AS apply_date, a.dueDate AS due_date,
            ca.sn AS core_sn,
            IFNULL(ca.apply_time, 0) AS core_apply_time,
            IFNULL(ca.audit_time, 0) AS core_audit_time,
            IFNULL(ca.orig_fee, 0) AS core_orig_fee,
            a.disburseTime AS disburse_time, a.paidTime AS paid_time,
            a.`status` AS src_status,
            IFNULL(u.credentialNo, '') AS id2,
            CAST(UNIX_TIMESTAMP(a.created) AS UNSIGNED) * 1000 AS event_time
        FROM {m}.application a
        LEFT JOIN {m}.`user` u ON u.id = a.`userId`
        LEFT JOIN {c}.application ca ON ca.ext_sn = a.applicationNo
        WHERE a.applicationNo IS NOT NULL AND a.applicationNo <> ''
          AND a.`appId` NOT IN ({ex_ph})
          AND a.applyDate >= %s
          AND a.id > %s AND a.id <= %s
        ORDER BY a.id ASC
    """
    params: List[Any] = list(exclude_app_ids) + [since_unix, lo, hi]
    with src.cursor() as cur:
        cur.execute(sql, params)
        return list(cur.fetchall())


def _source_application_id_bounds(
    src,
    since_unix: int,
    exclude_app_ids: Tuple[int, ...],
) -> Tuple[int, int, int]:
    m = "ng_loan_market"
    ex_ph = ",".join(["%s"] * len(exclude_app_ids))
    with src.cursor() as cur:
        cur.execute(
            f"""
            SELECT MIN(a.id) AS min_id, MAX(a.id) AS max_id, COUNT(*) AS cnt
            FROM {m}.application a
            WHERE a.applicationNo IS NOT NULL AND a.applicationNo <> ''
              AND a.`appId` NOT IN ({ex_ph})
              AND a.applyDate >= %s
            """,
            (*exclude_app_ids, since_unix),
        )
        row = cur.fetchone() or {}
    return (
        int(row.get("min_id") or 0),
        int(row.get("max_id") or 0),
        int(row.get("cnt") or 0),
    )


def _application_vt_skip_reason(
    row: dict,
    bvn_raw: str,
    vt: mig.VtTokenResolver,
    vt_enabled: bool,
    logger: ReconcileLogger,
    table_name: str = "application",
) -> Optional[str]:
    if not vt_enabled:
        return None
    app_no = row.get("application_no") or row.get("sn")
    user_id = row.get("user_id")
    app_ctx = f"application_no={app_no} user_id={user_id}"
    mobile_raw = row.get("mobile") or ""
    if not vt.resolve_token(
        mig.VtTokenResolver.VT_MOBILE, mobile_raw,
        context=f"{app_ctx} field=mobile", row_data=row, log_miss=False,
    ):
        logger.vt_skip({
            "table": table_name, "application_no": app_no, "user_id": user_id,
            "vt_type": "mobile", "raw": mobile_raw, "reason": "vt_token_cache miss",
        })
        return "mobile"
    bank_raw = row.get("bank_account_number") or ""
    if bank_raw and not vt.resolve_token(
        mig.VtTokenResolver.VT_BANK, bank_raw,
        context=f"{app_ctx} field=bank", row_data=row, log_miss=False,
    ):
        logger.vt_skip({
            "table": table_name, "application_no": app_no, "user_id": user_id,
            "vt_type": "bank_account", "raw": bank_raw, "reason": "vt_token_cache miss",
        })
        return "bank_account"
    if bvn_raw and not vt.resolve_token(
        mig.VtTokenResolver.VT_ID_NUMBER, bvn_raw,
        context=f"{app_ctx} field=id_number",
        row_data={**row, "bvn": bvn_raw}, log_miss=False,
    ):
        logger.vt_skip({
            "table": table_name, "application_no": app_no, "user_id": user_id,
            "vt_type": "id_number", "raw": bvn_raw, "reason": "vt_token_cache miss",
        })
        return "id_number"
    gaid_raw = row.get("gaid_idfa")
    if gaid_raw not in (None, "") and not vt.resolve_token(
        mig.VtTokenResolver.VT_GAID, gaid_raw,
        context=f"{app_ctx} field=gaid", row_data=row, log_miss=False,
    ):
        logger.vt_skip({
            "table": table_name, "application_no": app_no, "user_id": user_id,
            "vt_type": "gaid_idfa", "raw": gaid_raw, "reason": "vt_token_cache miss",
        })
        return "gaid_idfa"
    return None


def build_expected_application_rows(
    cfg: Dict[str, Any],
    src,
    lo: int,
    hi: int,
    since_unix: int,
    exclude_app_ids: Tuple[int, ...],
    vt: mig.VtTokenResolver,
    logger: ReconcileLogger,
) -> Tuple[List[dict], Dict[str, int]]:
    stats = {"source_rows": 0, "vt_skip": 0, "no_core_sn": 0, "built": 0}
    raw_rows = _select_application_source_since(
        src, lo, hi, since_unix, exclude_app_ids,
    )
    stats["source_rows"] = len(raw_rows)
    if not raw_rows:
        return [], stats

    user_ids = [int(r["user_id"]) for r in raw_rows]
    sns = [str(r["application_no"]) for r in raw_rows if r.get("application_no")]
    bvn_map, repay_map = mig._fetch_app_lookup_maps(cfg, src, user_ids, sns)
    mig._register_app_batch_vt(vt, raw_rows, bvn_map)
    vt.prefetch()

    vt_enabled = cfg.get("vt_token_enable", True)
    ok_rows: List[dict] = []
    for row in raw_rows:
        if not str(row.get("core_sn") or "").strip():
            stats["no_core_sn"] += 1
            continue
        bvn_raw = bvn_map.get(int(row["user_id"]), "") or ""
        if _application_vt_skip_reason(row, bvn_raw, vt, vt_enabled, logger):
            stats["vt_skip"] += 1
            continue
        ok_rows.append(row)

    built_raw = mig._build_application_rows(ok_rows, bvn_map, repay_map, vt)
    built = [{c: row.get(c) for c in mig.APPLICATION_INSERT_COLS} for row in built_raw]
    stats["built"] = len(built)
    return built, stats


def plan_application_table(
    cfg: Dict[str, Any],
    target_by_key: Dict[Tuple[Any, ...], dict],
    since_date: str,
    exclude_app_ids: Tuple[int, ...],
    source_batch: int,
    vt: mig.VtTokenResolver,
    logger: ReconcileLogger,
) -> Tuple[List[dict], Dict[str, int]]:
    since_unix = since_date_to_unix(since_date)
    src = mig.connect_source(cfg)
    stats = {
        "source_total": 0,
        "skip_ok": 0,
        "plan_insert": 0,
        "plan_update": 0,
        "vt_skip": 0,
        "no_core_sn": 0,
        "source_batches": 0,
    }
    plan: List[dict] = []
    try:
        min_id, max_id, cnt = _source_application_id_bounds(
            src, since_unix, exclude_app_ids,
        )
        logger.log(
            f"source application applyDate>={since_date}: count={cnt} "
            f"id=[{min_id},{max_id}] exclude_app_ids={exclude_app_ids}"
        )
        if cnt <= 0:
            return plan, stats

        lo = min_id - 1
        while lo < max_id:
            hi = min(lo + source_batch, max_id)
            stats["source_batches"] += 1
            expected_rows, batch_stats = build_expected_application_rows(
                cfg, src, lo, hi, since_unix, exclude_app_ids, vt, logger,
            )
            stats["vt_skip"] += batch_stats.get("vt_skip", 0)
            stats["no_core_sn"] += batch_stats.get("no_core_sn", 0)
            stats["source_total"] += batch_stats.get("source_rows", 0)

            for exp in expected_rows:
                pk = application_key(exp)
                actual = target_by_key.get(pk)
                ok, diff = compare_rows(exp, actual, APPLICATION_COMPARE_COLS)
                if ok:
                    stats["skip_ok"] += 1
                    continue
                action = "insert" if actual is None else "update"
                if action == "insert":
                    stats["plan_insert"] += 1
                else:
                    stats["plan_update"] += 1
                plan.append({
                    "table": "application",
                    "action": action,
                    "key": list(pk),
                    "application_no": exp.get("application_no"),
                    "user_id": exp.get("user_id"),
                    "sn": exp.get("sn"),
                    "diff": diff,
                    "row": exp,
                })
            lo = hi
            if stats["source_batches"] % 20 == 0:
                logger.log(
                    f"plan progress batches={stats['source_batches']} "
                    f"ok={stats['skip_ok']} insert={stats['plan_insert']} "
                    f"update={stats['plan_update']} vt_skip={stats['vt_skip']} "
                    f"no_core_sn={stats['no_core_sn']}"
                )
    finally:
        mig._close_mysql_conn(src)
    return plan, stats


def apply_application_plan(
    cfg: Dict[str, Any],
    plan: List[dict],
    batch_size: int,
    apply_workers: int,
    dry_run: bool,
    logger: ReconcileLogger,
) -> Dict[str, int]:
    return _apply_composite_plan(
        cfg, plan, "application", mig.APPLICATION_INSERT_COLS,
        APPLICATION_PK, APPLICATION_COMPARE_COLS,
        batch_size, apply_workers, dry_run, logger,
    )


def run_application_reconcile(args: argparse.Namespace, cfg: Dict[str, Any]) -> int:
    env_path = Path(args.env).resolve()
    mig.ENV_FILE = env_path
    log_dir = Path(args.log_dir)
    logger = ReconcileLogger(log_dir, "application")
    cache_path = Path(args.target_cache)
    plan_path = Path(args.plan_file)
    phase = args.phase
    dry_run = not args.apply
    exclude_app_ids = parse_exclude_app_ids(args.exclude_app_ids)
    since_ms = (
        int(args.target_created_since_ms)
        if args.target_created_since_ms
        else since_date_to_ms(args.since_date)
    )

    if phase in ("load-target", "plan", "all"):
        setup_preloads(cfg, logger, args.vt_preload)

    target_by_key: Dict[Tuple[Any, ...], dict] = {}
    if phase in ("load-target", "plan", "all"):
        target_by_key = load_or_build_target_application_cache(
            env_path,
            cache_path,
            since_ms,
            exclude_app_ids,
            args.load_workers,
            args.page_size,
            logger,
            from_cache=args.from_cache and phase != "load-target",
        )
        if phase == "load-target":
            logger.log("phase load-target done")
            logger.close()
            return 0

    plan: List[dict] = []
    if phase in ("plan", "all"):
        vt = mig.VtTokenResolver(
            mig.connect_source(cfg),
            enabled=cfg.get("vt_token_enable", True),
            chunk=cfg.get("vt_token_chunk", 2000),
            vt_db=cfg.get("vt_token_db", "ng_loan_market"),
        )
        logger.log(
            f"plan start since={args.since_date} target_created_time_ms>={since_ms} "
            f"dry_run={dry_run}"
        )
        t0 = time.time()
        plan, plan_stats = plan_application_table(
            cfg, target_by_key, args.since_date, exclude_app_ids,
            args.source_batch, vt, logger,
        )
        mig._close_mysql_conn(vt.conn)
        n = write_jsonl(plan_path, plan)
        logger.log(
            f"plan done file={plan_path} rows={n} stats={plan_stats} "
            f"elapsed={time.time()-t0:.1f}s"
        )

    if phase == "apply":
        if not plan_path.is_file():
            logger.log(f"ERROR plan file missing: {plan_path}")
            logger.close()
            return 1
        plan = read_jsonl(plan_path)
        logger.log(f"loaded plan rows={len(plan)}")

    if phase in ("apply", "all"):
        if phase == "all" and not plan:
            logger.log("apply skipped: empty plan")
        else:
            apply_stats = apply_application_plan(
                cfg, plan, args.apply_batch, args.apply_workers, dry_run, logger,
            )
            logger.log(f"apply stats={apply_stats}")

    logger.close()
    return 0


# ---------------------------------------------------------------------------
# loan（已放款 application → 有且仅一条 loan；loan_no 中间段 = core repay_plan.sn）
# ---------------------------------------------------------------------------

def _load_target_loan_shard(spec: dict) -> Tuple[int, List[dict], Dict[str, int]]:
    worker_id = int(spec["worker_id"])
    workers = int(spec["workers"])
    since_ms = int(spec["since_ms"])
    page_size = int(spec["page_size"])
    env_path = str(spec["env_path"])
    columns: List[str] = list(spec["columns"])
    cols_sql = spec["cols_sql"]
    exclude_created_ms: Tuple[int, ...] = tuple(spec["exclude_created_ms"])
    exclude_app_ids: Tuple[int, ...] = tuple(spec["exclude_app_ids"])

    label = f"[target loan {worker_id}/{workers}]"
    stats = {"scanned": 0, "skipped_excluded_app": 0, "skipped_excluded_created_ms": 0}
    rows: List[dict] = []
    cfg = _worker_load_env(env_path)
    tgt = mig.connect_target(cfg)
    last_app_no = ""
    exclude_created_set = set(int(x) for x in exclude_created_ms) if exclude_created_ms else set()
    t0 = time.time()
    print(
        f"{label} load start created_time_ms>={since_ms} exclude_app_ids={exclude_app_ids} "
        f"exclude_created_ms={exclude_created_ms} (post-filter in memory)",
        flush=True,
    )
    try:
        while True:
            with tgt.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT {cols_sql}
                    FROM `loan`
                    WHERE created_time >= %s
                      AND application_no IS NOT NULL AND application_no <> ''
                      AND MOD(CRC32(application_no), %s) = %s
                      AND application_no > %s
                    ORDER BY application_no ASC, period ASC, roll_sequence ASC
                    LIMIT %s
                    """,
                    (
                        since_ms,
                        workers,
                        worker_id - 1,
                        last_app_no,
                        page_size,
                    ),
                )
                batch = cur.fetchall()
            if not batch:
                break
            for row in batch:
                item = {k: row[k] for k in columns}
                reason = loan_row_skip_reason(
                    item, exclude_app_ids, exclude_created_ms, exclude_created_set,
                )
                if reason == "excluded_app":
                    stats["skipped_excluded_app"] += 1
                    continue
                if reason == "excluded_created_ms":
                    stats["skipped_excluded_created_ms"] += 1
                    continue
                rows.append(item)
            last_app_no = str(batch[-1]["application_no"])
            stats["scanned"] = len(rows)
            if stats["scanned"] % 100_000 < page_size:
                print(
                    f"{label} progress rows={stats['scanned']} "
                    f"skipped_app={stats['skipped_excluded_app']} "
                    f"skipped_created={stats['skipped_excluded_created_ms']} "
                    f"last={last_app_no} elapsed={time.time()-t0:.1f}s",
                    flush=True,
                )
            if len(batch) < page_size:
                break
    finally:
        tgt.close()
    print(
        f"{label} done rows={len(rows)} skipped_app={stats['skipped_excluded_app']} "
        f"skipped_created={stats['skipped_excluded_created_ms']} "
        f"elapsed={time.time()-t0:.1f}s",
        flush=True,
    )
    return worker_id, rows, stats


def parallel_load_target_loans(
    env_path: Path,
    columns: Sequence[str],
    since_ms: int,
    exclude_app_ids: Tuple[int, ...],
    exclude_created_ms: Tuple[int, ...],
    load_workers: int,
    page_size: int,
    logger: ReconcileLogger,
) -> Dict[Tuple[Any, ...], dict]:
    workers = max(1, min(load_workers, 32))
    cols_sql = _target_cols_sql(columns)
    specs = [
        {
            "worker_id": i + 1,
            "workers": workers,
            "since_ms": since_ms,
            "page_size": page_size,
            "env_path": str(env_path.resolve()),
            "columns": list(columns),
            "cols_sql": cols_sql,
            "exclude_app_ids": list(exclude_app_ids),
            "exclude_created_ms": list(exclude_created_ms),
        }
        for i in range(workers)
    ]
    logger.log(
        f"load target loan: workers={workers} created_time_ms>={since_ms} "
        f"exclude_app_ids={exclude_app_ids} exclude_created_ms={exclude_created_ms} "
        f"(memory filter) page_size={page_size}"
    )
    t0 = time.time()
    merged: Dict[Tuple[Any, ...], dict] = {}
    if workers == 1:
        _, chunk, _ = _load_target_loan_shard(specs[0])
        for row in chunk:
            merged[loan_key(row)] = row
    else:
        ctx = multiprocessing.get_context("spawn")
        with ctx.Pool(processes=workers) as pool:
            parts = pool.map(_load_target_loan_shard, specs)
        for _, chunk, _ in sorted(parts, key=lambda x: x[0]):
            for row in chunk:
                merged[loan_key(row)] = row
    logger.log(
        f"load target loan done rows={len(merged)} elapsed={time.time()-t0:.1f}s"
    )
    return merged


def load_or_build_target_loan_cache(
    env_path: Path,
    cache_path: Path,
    since_ms: int,
    exclude_app_ids: Tuple[int, ...],
    exclude_created_ms: Tuple[int, ...],
    load_workers: int,
    page_size: int,
    logger: ReconcileLogger,
    from_cache: bool,
) -> Dict[Tuple[Any, ...], dict]:
    if from_cache and cache_path.is_file():
        logger.log(f"load target from cache {cache_path}")
        t0 = time.time()
        target_by_key: Dict[Tuple[Any, ...], dict] = {}
        skip_stats = {"skipped_excluded_app": 0, "skipped_excluded_created_ms": 0}
        exclude_created_set = (
            set(int(x) for x in exclude_created_ms) if exclude_created_ms else set()
        )
        for row in read_jsonl(cache_path):
            reason = loan_row_skip_reason(
                row, exclude_app_ids, exclude_created_ms, exclude_created_set,
            )
            if reason == "excluded_app":
                skip_stats["skipped_excluded_app"] += 1
                continue
            if reason == "excluded_created_ms":
                skip_stats["skipped_excluded_created_ms"] += 1
                continue
            target_by_key[loan_key(row)] = row
        logger.log(
            f"cache loaded rows={len(target_by_key)} skip={skip_stats} "
            f"elapsed={time.time()-t0:.1f}s"
        )
        return target_by_key

    target_by_key = parallel_load_target_loans(
        env_path, mig.LOAN_INSERT_COLS, since_ms,
        exclude_app_ids, exclude_created_ms, load_workers, page_size, logger,
    )
    logger.log(f"write target cache {cache_path}")
    t0 = time.time()
    n = write_jsonl(cache_path, target_by_key.values())
    logger.log(f"cache written rows={n} elapsed={time.time()-t0:.1f}s")
    return target_by_key


def _select_disbursed_application_source_since(
    src,
    lo: int,
    hi: int,
    since_unix: int,
    exclude_app_ids: Tuple[int, ...],
) -> List[dict]:
    """源库已放款单（disburseTime>0），applyDate >= since_unix。"""
    m, c = "ng_loan_market", "ng_loan_core"
    ex_ph = ",".join(["%s"] * len(exclude_app_ids))
    sql = f"""
        SELECT
            a.applicationNo AS application_no,
            CASE WHEN a.mobile LIKE '+234%%' THEN a.mobile
                 WHEN a.mobile LIKE '234%%' THEN CONCAT('+', a.mobile)
                 WHEN a.mobile LIKE '0%%' THEN CONCAT('+234', SUBSTRING(a.mobile, 2))
                 ELSE CONCAT('+234', a.mobile) END AS mobile,
            'ng01' AS bid, a.`appId` AS app_id, '1.0.0' AS app_version,
            a.`userId` AS user_id, a.applicationNo AS sn,
            CASE WHEN a.`repeatLoan` = 0 THEN 1 ELSE 0 END AS is_first_apply,
            IFNULL(NULLIF(a.gaid, ''), NULL) AS gaid_idfa,
            IFNULL(CAST(a.`deviceDataId` AS CHAR), '') AS device_uuid,
            IFNULL(a.bankCode, '') AS bank_code,
            IFNULL(a.bankAccount, '') AS bank_account_number,
            CAST(a.`productId` AS CHAR) AS product_id,
            a.term, a.shouldLoanAmount AS should_loan_amount,
            a.amount AS amount, a.repayment AS repayment,
            a.disburseAmount AS disburse_amount,
            a.applyDate AS apply_date, a.dueDate AS due_date,
            ca.sn AS core_sn,
            IFNULL(ca.apply_time, 0) AS core_apply_time,
            IFNULL(ca.audit_time, 0) AS core_audit_time,
            IFNULL(ca.orig_fee, 0) AS core_orig_fee,
            a.disburseTime AS disburse_time, a.paidTime AS paid_time,
            a.`status` AS src_status,
            IFNULL(u.credentialNo, '') AS id2,
            CAST(UNIX_TIMESTAMP(a.created) AS UNSIGNED) * 1000 AS event_time
        FROM {m}.application a
        LEFT JOIN {m}.`user` u ON u.id = a.`userId`
        LEFT JOIN {c}.application ca ON ca.ext_sn = a.applicationNo
        WHERE a.applicationNo IS NOT NULL AND a.applicationNo <> ''
          AND a.`appId` NOT IN ({ex_ph})
          AND a.applyDate >= %s
          AND a.disburseTime > 0
          AND a.id > %s AND a.id <= %s
        ORDER BY a.id ASC
    """
    params: List[Any] = list(exclude_app_ids) + [since_unix, lo, hi]
    with src.cursor() as cur:
        cur.execute(sql, params)
        return list(cur.fetchall())


def _source_disbursed_application_id_bounds(
    src,
    since_unix: int,
    exclude_app_ids: Tuple[int, ...],
) -> Tuple[int, int, int]:
    m = "ng_loan_market"
    ex_ph = ",".join(["%s"] * len(exclude_app_ids))
    with src.cursor() as cur:
        cur.execute(
            f"""
            SELECT MIN(a.id) AS min_id, MAX(a.id) AS max_id, COUNT(*) AS cnt
            FROM {m}.application a
            WHERE a.applicationNo IS NOT NULL AND a.applicationNo <> ''
              AND a.`appId` NOT IN ({ex_ph})
              AND a.applyDate >= %s
              AND a.disburseTime > 0
            """,
            (*exclude_app_ids, since_unix),
        )
        row = cur.fetchone() or {}
    return (
        int(row.get("min_id") or 0),
        int(row.get("max_id") or 0),
        int(row.get("cnt") or 0),
    )


def build_expected_loan_rows(
    cfg: Dict[str, Any],
    src,
    lo: int,
    hi: int,
    since_unix: int,
    exclude_app_ids: Tuple[int, ...],
    vt: mig.VtTokenResolver,
    logger: ReconcileLogger,
) -> Tuple[List[dict], Dict[str, int]]:
    stats = {
        "source_rows": 0, "vt_skip": 0, "no_core_sn": 0,
        "no_application_no": 0, "built": 0,
    }
    raw_rows = _select_disbursed_application_source_since(
        src, lo, hi, since_unix, exclude_app_ids,
    )
    stats["source_rows"] = len(raw_rows)
    if not raw_rows:
        return [], stats

    user_ids = [int(r["user_id"]) for r in raw_rows]
    sns = [str(r["application_no"]) for r in raw_rows if r.get("application_no")]
    bvn_map, _repay_map = mig._fetch_app_lookup_maps(cfg, src, user_ids, sns)
    mig._register_app_batch_vt(vt, raw_rows, bvn_map)
    vt.prefetch()

    vt_enabled = cfg.get("vt_token_enable", True)
    sn_to_app_no: Dict[str, str] = {}
    for row in raw_rows:
        if not str(row.get("core_sn") or "").strip():
            stats["no_core_sn"] += 1
            continue
        bvn_raw = bvn_map.get(int(row["user_id"]), "") or ""
        if _application_vt_skip_reason(
            row, bvn_raw, vt, vt_enabled, logger, table_name="loan",
        ):
            stats["vt_skip"] += 1
            continue
        app_no = mig.format_application_no(row.get("app_id"), row.get("application_no"))
        if not app_no:
            stats["no_application_no"] += 1
            continue
        if application_no_matches_excluded_app(app_no, exclude_app_ids):
            continue
        sn_to_app_no[str(row["core_sn"]).strip()] = app_no

    if not sn_to_app_no:
        return [], stats

    loans_raw = mig._fetch_loan_rows_from_source(src, sn_to_app_no)
    built = [{c: row.get(c) for c in mig.LOAN_INSERT_COLS} for row in loans_raw]
    stats["built"] = len(built)
    return built, stats


def plan_loan_table(
    cfg: Dict[str, Any],
    target_by_key: Dict[Tuple[Any, ...], dict],
    since_date: str,
    exclude_app_ids: Tuple[int, ...],
    source_batch: int,
    vt: mig.VtTokenResolver,
    logger: ReconcileLogger,
) -> Tuple[List[dict], Dict[str, int]]:
    since_unix = since_date_to_unix(since_date)
    src = mig.connect_source(cfg)
    stats = {
        "source_total": 0,
        "skip_ok": 0,
        "plan_insert": 0,
        "plan_update": 0,
        "vt_skip": 0,
        "no_core_sn": 0,
        "no_application_no": 0,
        "source_batches": 0,
    }
    plan: List[dict] = []
    try:
        min_id, max_id, cnt = _source_disbursed_application_id_bounds(
            src, since_unix, exclude_app_ids,
        )
        logger.log(
            f"source loan (disbursed applyDate>={since_date}): count={cnt} "
            f"id=[{min_id},{max_id}] exclude_app_ids={exclude_app_ids}"
        )
        if cnt <= 0:
            return plan, stats

        lo = min_id - 1
        while lo < max_id:
            hi = min(lo + source_batch, max_id)
            stats["source_batches"] += 1
            expected_rows, batch_stats = build_expected_loan_rows(
                cfg, src, lo, hi, since_unix, exclude_app_ids, vt, logger,
            )
            stats["vt_skip"] += batch_stats.get("vt_skip", 0)
            stats["no_core_sn"] += batch_stats.get("no_core_sn", 0)
            stats["no_application_no"] += batch_stats.get("no_application_no", 0)
            stats["source_total"] += batch_stats.get("source_rows", 0)

            for exp in expected_rows:
                pk = loan_key(exp)
                actual = target_by_key.get(pk)
                ok, diff = compare_rows(exp, actual, LOAN_COMPARE_COLS)
                if ok:
                    stats["skip_ok"] += 1
                    continue
                action = "insert" if actual is None else "update"
                if action == "insert":
                    stats["plan_insert"] += 1
                else:
                    stats["plan_update"] += 1
                plan.append({
                    "table": "loan",
                    "action": action,
                    "key": list(pk),
                    "loan_no": exp.get("loan_no"),
                    "application_no": exp.get("application_no"),
                    "diff": diff,
                    "row": exp,
                })
            lo = hi
            if stats["source_batches"] % 20 == 0:
                logger.log(
                    f"plan progress batches={stats['source_batches']} "
                    f"ok={stats['skip_ok']} insert={stats['plan_insert']} "
                    f"update={stats['plan_update']} vt_skip={stats['vt_skip']} "
                    f"no_core_sn={stats['no_core_sn']}"
                )
    finally:
        mig._close_mysql_conn(src)
    return plan, stats


def apply_loan_plan(
    cfg: Dict[str, Any],
    plan: List[dict],
    batch_size: int,
    apply_workers: int,
    dry_run: bool,
    logger: ReconcileLogger,
) -> Dict[str, int]:
    return _apply_composite_plan(
        cfg, plan, "loan", mig.LOAN_INSERT_COLS,
        LOAN_PK, LOAN_APPLY_UPDATE_COLS,
        batch_size, apply_workers, dry_run, logger,
    )


def run_loan_reconcile(args: argparse.Namespace, cfg: Dict[str, Any]) -> int:
    env_path = Path(args.env).resolve()
    mig.ENV_FILE = env_path
    log_dir = Path(args.log_dir)
    logger = ReconcileLogger(log_dir, "loan")
    cache_path = Path(args.target_cache)
    plan_path = Path(args.plan_file)
    phase = args.phase
    dry_run = not args.apply
    exclude_app_ids = parse_exclude_app_ids(args.exclude_app_ids)
    exclude_loan_created_ms = parse_exclude_loan_created_ms(args.exclude_loan_created_ms)
    since_ms = (
        int(args.target_created_since_ms)
        if args.target_created_since_ms
        else since_date_to_ms(args.since_date)
    )

    if phase in ("load-target", "plan", "all"):
        setup_preloads(cfg, logger, args.vt_preload)

    target_by_key: Dict[Tuple[Any, ...], dict] = {}
    if phase in ("load-target", "plan", "all"):
        target_by_key = load_or_build_target_loan_cache(
            env_path,
            cache_path,
            since_ms,
            exclude_app_ids,
            exclude_loan_created_ms,
            args.load_workers,
            args.page_size,
            logger,
            from_cache=args.from_cache and phase != "load-target",
        )
        if phase == "load-target":
            logger.log("phase load-target done")
            logger.close()
            return 0

    plan: List[dict] = []
    if phase in ("plan", "all"):
        vt = mig.VtTokenResolver(
            mig.connect_source(cfg),
            enabled=cfg.get("vt_token_enable", True),
            chunk=cfg.get("vt_token_chunk", 2000),
            vt_db=cfg.get("vt_token_db", "ng_loan_market"),
        )
        logger.log(
            f"plan start since={args.since_date} target_created_time_ms>={since_ms} "
            f"dry_run={dry_run}"
        )
        t0 = time.time()
        plan, plan_stats = plan_loan_table(
            cfg, target_by_key, args.since_date, exclude_app_ids,
            args.source_batch, vt, logger,
        )
        mig._close_mysql_conn(vt.conn)
        n = write_jsonl(plan_path, plan)
        logger.log(
            f"plan done file={plan_path} rows={n} stats={plan_stats} "
            f"elapsed={time.time()-t0:.1f}s"
        )

    if phase == "apply":
        if not plan_path.is_file():
            logger.log(f"ERROR plan file missing: {plan_path}")
            logger.close()
            return 1
        plan = read_jsonl(plan_path)
        logger.log(f"loaded plan rows={len(plan)}")

    if phase in ("apply", "all"):
        if phase == "all" and not plan:
            logger.log("apply skipped: empty plan")
        else:
            apply_stats = apply_loan_plan(
                cfg, plan, args.apply_batch, args.apply_workers, dry_run, logger,
            )
            logger.log(f"apply stats={apply_stats}")

    logger.close()
    return 0


def run_user_reconcile(args: argparse.Namespace, cfg: Dict[str, Any]) -> int:
    return _run_reconcile_phases(
        args, cfg, "user", mig.USER_INSERT_COLS,
        plan_user_table, apply_user_plan,
        loader=parallel_load_target_users,
    )


def run_user_info_reconcile(args: argparse.Namespace, cfg: Dict[str, Any]) -> int:
    return _run_reconcile_phases(
        args, cfg, "user_info", mig.USER_INFO_COLS,
        plan_user_info_table, apply_user_info_plan,
    )


def run_user_bankcard_reconcile(args: argparse.Namespace, cfg: Dict[str, Any]) -> int:
    return _run_reconcile_phases_composite(
        args, cfg, "user_bankcard", mig.USER_BANKCARD_COLS, bankcard_key,
        plan_user_bankcard_table, apply_user_bankcard_plan,
        order_tail="group_user_id ASC, bank_account_number ASC",
        need_vt=True,
    )


def run_user_product_reconcile(args: argparse.Namespace, cfg: Dict[str, Any]) -> int:
    return _run_reconcile_phases_composite(
        args, cfg, "user_product", mig.USER_PRODUCT_COLS, product_key,
        plan_user_product_table, apply_user_product_plan,
        order_tail="group_user_id ASC, product_id ASC",
        need_vt=False,
    )


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(description="Table-by-table source-target reconcile")
    p.add_argument("--env", default=str(HERE / "ng_migration.env"))
    p.add_argument(
        "--table",
        required=True,
        choices=SUPPORTED_TABLES,
        help="目标表（已实现: user, user_info, user_bankcard, user_product, application, loan）",
    )
    p.add_argument(
        "--phase",
        default="plan",
        choices=("load-target", "plan", "apply", "all"),
    )
    p.add_argument("--apply", action="store_true", help="apply 阶段写库（默认 plan 仅出文件）")
    p.add_argument("--since-date", default=DEFAULT_SINCE_DATE)
    p.add_argument("--max-target-user-id", type=int, default=DEFAULT_MAX_TARGET_USER_ID)
    p.add_argument("--target-cache", default="")
    p.add_argument("--plan-file", default="")
    p.add_argument("--log-dir", default="/tmp/reconcile_logs")
    p.add_argument("--from-cache", action="store_true", help="plan 阶段复用已有 target cache")
    p.add_argument("--load-workers", type=int, default=8)
    p.add_argument("--source-workers", type=int, default=4)
    p.add_argument("--page-size", type=int, default=50000)
    p.add_argument("--source-batch", type=int, default=5000)
    p.add_argument("--apply-batch", type=int, default=500, help="apply 每批行数")
    p.add_argument("--apply-workers", type=int, default=15, help="apply 并行线程数")
    p.add_argument("--vt-preload", dest="vt_preload", action="store_true", default=True)
    p.add_argument("--no-vt-preload", dest="vt_preload", action="store_false")
    p.add_argument(
        "--exclude-app-ids",
        default="",
        help=(
            f"application/loan 排除 app_id（application 按 app_id 字段、loan 按 application_no 前缀；"
            f"均在内存过滤；默认 {','.join(map(str, DEFAULT_EXCLUDE_APP_IDS))}）"
        ),
    )
    p.add_argument(
        "--target-created-since-ms",
        default="",
        help="application/loan 目标库 created_time 下限（毫秒时间戳 ms，默认 2026-01-01 → since_date*1000）",
    )
    p.add_argument(
        "--exclude-loan-created-ms",
        default="",
        help=(
            f"loan 内存排除 created_time（毫秒 ms，默认 {','.join(map(str, DEFAULT_EXCLUDE_LOAN_CREATED_MS))}；"
            "不在 SQL 执行"
        ),
    )
    args = p.parse_args(argv)

    env_path = Path(args.env)
    if not env_path.is_file():
        print(f"env not found: {env_path}", file=sys.stderr)
        return 1

    mig.ENV_FILE = env_path
    cfg = mig.load_env()

    paths = default_paths(args.table)
    if not args.target_cache:
        args.target_cache = paths["target_cache"]
    if not args.plan_file:
        args.plan_file = paths["plan_file"]

    if args.table == "user":
        return run_user_reconcile(args, cfg)
    if args.table == "user_info":
        return run_user_info_reconcile(args, cfg)
    if args.table == "user_bankcard":
        return run_user_bankcard_reconcile(args, cfg)
    if args.table == "user_product":
        return run_user_product_reconcile(args, cfg)
    if args.table == "application":
        return run_application_reconcile(args, cfg)
    if args.table == "loan":
        return run_loan_reconcile(args, cfg)

    print(
        f"table={args.table} 尚未实现；已实现: {', '.join(SUPPORTED_TABLES)}",
        file=sys.stderr,
    )
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
