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
        默认 load=16 / source-workers=8 / source-batch=20000；apply 24 线程、每批 1000

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
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

import pymysql
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
        self._lock = threading.Lock()

    def close(self) -> None:
        with self._lock:
            self._main_fp.close()

    def log(self, msg: str) -> None:
        line = f"[{_now_ts()}] {msg}"
        print(line, flush=True)
        with self._lock:
            self._main_fp.write(line + "\n")
            self._main_fp.flush()

    def vt_skip(self, record: dict) -> None:
        record = dict(record)
        record["ts"] = _now_ts()
        line = json.dumps(record, ensure_ascii=False) + "\n"
        with self._lock:
            with self.vt_skip_path.open("a", encoding="utf-8") as fp:
                fp.write(line)

    def apply_audit(self, record: dict) -> None:
        record = dict(record)
        record["ts"] = _now_ts()
        line = json.dumps(record, ensure_ascii=False) + "\n"
        with self._lock:
            with self.apply_path.open("a", encoding="utf-8") as fp:
                fp.write(line)


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




def _partition_id_span(min_id: int, max_id: int, workers: int) -> List[Tuple[int, int]]:
    """将 (min_id-1, max_id] 切成连续 (lo, hi] 区间，供 plan 并行。"""
    workers = max(1, int(workers or 1))
    lo0 = int(min_id) - 1
    span = int(max_id) - lo0
    if span <= 0:
        return []
    workers = min(workers, span)
    chunk = (span + workers - 1) // workers
    out: List[Tuple[int, int]] = []
    lo = lo0
    while lo < max_id:
        hi = min(lo + chunk, max_id)
        out.append((lo, hi))
        lo = hi
    return out


def _make_vt_resolver(cfg: Dict[str, Any], conn=None):
    if conn is None:
        conn = mig.connect_source(cfg)
    return mig.VtTokenResolver(
        conn,
        enabled=cfg.get("vt_token_enable", True),
        chunk=cfg.get("vt_token_chunk", 2000),
        vt_db=cfg.get("vt_token_db", "ng_loan_market"),
    )


def _merge_stats(parts: List[Dict[str, int]]) -> Dict[str, int]:
    if not parts:
        return {}
    out: Dict[str, int] = {}
    for part in parts:
        for k, v in part.items():
            if isinstance(v, int):
                out[k] = out.get(k, 0) + int(v)
            else:
                out[k] = v  # type: ignore
    return out


def _run_parallel_id_spans(
    spans: List[Tuple[int, int]],
    workers: int,
    shard_fn,
    logger: "ReconcileLogger",
    label: str,
) -> Tuple[List[dict], Dict[str, int]]:
    """对多个 id 区间并行执行 shard_fn(lo, hi) -> (plan, stats)。"""
    if not spans:
        return [], {}
    workers = max(1, min(int(workers or 1), len(spans)))
    if workers == 1 or len(spans) == 1:
        plan, stats = shard_fn(spans[0][0], spans[0][1])
        return plan, stats

    logger.log(
        "{0} parallel spans={1} workers={2}".format(label, len(spans), workers)
    )
    plans: List[dict] = []
    stats_parts: List[Dict[str, int]] = []
    done = 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futs = [pool.submit(shard_fn, lo, hi) for lo, hi in spans]
        for fut in as_completed(futs):
            part_plan, part_stats = fut.result()
            plans.extend(part_plan)
            stats_parts.append(part_stats)
            done += 1
            logger.log(
                "{0} shard {1}/{2} plan_rows={3} stats={4}".format(
                    label, done, len(spans), len(part_plan), part_stats,
                )
            )
    return plans, _merge_stats(stats_parts)

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


def _is_mysql_lost_conn(exc: BaseException) -> bool:
    if isinstance(exc, (pymysql.err.OperationalError, pymysql.err.InterfaceError)):
        code = exc.args[0] if exc.args else None
        if code in (0, 2006, 2013, 2014, 2055):
            return True
        msg = str(exc).lower()
        return "lost connection" in msg or "gone away" in msg
    return False


def _probe_max_user_id(
    env_path: Path,
    table: str,
    max_user_id: int,
    exclude_app_ids: Tuple[int, ...] = (),
) -> int:
    """查实际 MAX(user_id)，避免按 1 亿空段切分。"""
    cfg = _worker_load_env(str(env_path))
    tgt = mig.connect_target(cfg)
    try:
        exclude_sql = ""
        params = [max_user_id]  # type: List[Any]
        if table == "user" and exclude_app_ids:
            ex_ph = ",".join(["%s"] * len(exclude_app_ids))
            exclude_sql = "AND app_id NOT IN ({0})".format(ex_ph)
            params.extend(exclude_app_ids)
        with tgt.cursor() as cur:
            cur.execute(
                """
                SELECT MAX(user_id) AS max_id
                FROM `{table}`
                WHERE user_id < %s
                  {exclude_sql}
                """.format(table=table, exclude_sql=exclude_sql),
                params,
            )
            row = cur.fetchone() or {}
        actual = int(row.get("max_id") or 0)
        return max(0, min(actual + 1, max_user_id))
    finally:
        tgt.close()


def _load_target_shard(spec: dict) -> Tuple[int, List[dict], Dict[str, int]]:
    worker_id = int(spec["worker_id"])
    workers = int(spec["workers"])
    max_uid = int(spec["max_user_id"])
    page_size = int(spec["page_size"])
    env_path = str(spec["env_path"])
    table = str(spec["table"])
    columns = list(spec["columns"])  # type: List[str]
    cols_sql = spec["cols_sql"]
    exclude_app_ids = tuple(spec.get("exclude_app_ids") or ())  # type: Tuple[int, ...]
    lo = int(spec.get("range_lo", 0))
    hi = int(spec.get("range_hi", max_uid))

    label = "[target {0} {1}/{2}]".format(table, worker_id, workers)
    stats = {"scanned": 0}
    rows = []  # type: List[dict]

    cfg = _worker_load_env(env_path)
    tgt = mig.connect_target(cfg)
    last_id = max(0, lo - 1)
    exclude_sql = ""
    exclude_params = []  # type: List[Any]
    if table == "user" and exclude_app_ids:
        ex_ph = ",".join(["%s"] * len(exclude_app_ids))
        exclude_sql = "AND app_id NOT IN ({0})".format(ex_ph)
        exclude_params = list(exclude_app_ids)
    max_retries = max(3, int(cfg.get("mysql_batch_retries") or 6))
    t0 = time.time()
    print(
        "{0} load start range=[{1},{2}) page_size={3} exclude_app_ids={4}".format(
            label, lo, hi, page_size,
            exclude_app_ids if table == "user" else (),
        ),
        flush=True,
    )
    try:
        while True:
            batch = None
            last_exc = None  # type: Optional[BaseException]
            for attempt in range(max_retries):
                try:
                    try:
                        tgt.ping(reconnect=True)
                    except Exception:
                        mig._close_mysql_conn(tgt)
                        tgt = mig.connect_target(cfg)
                    with tgt.cursor() as cur:
                        cur.execute(
                            """
                            SELECT {cols_sql}
                            FROM `{table}`
                            WHERE user_id > %s
                              AND user_id >= %s AND user_id < %s
                              {exclude_sql}
                            ORDER BY user_id ASC
                            LIMIT %s
                            """.format(
                                cols_sql=cols_sql, table=table, exclude_sql=exclude_sql,
                            ),
                            (last_id, lo, hi) + tuple(exclude_params) + (page_size,),
                        )
                        batch = cur.fetchall()
                    break
                except Exception as exc:
                    last_exc = exc
                    if not _is_mysql_lost_conn(exc) or attempt >= max_retries - 1:
                        raise
                    delay = min(8.0, 0.5 * (2 ** attempt))
                    print(
                        "{0} mysql lost conn retry {1}/{2} sleep={3:.1f}s err={4}".format(
                            label, attempt + 1, max_retries, delay, exc,
                        ),
                        flush=True,
                    )
                    mig._close_mysql_conn(tgt)
                    time.sleep(delay)
                    tgt = mig.connect_target(cfg)
            if batch is None:
                if last_exc is not None:
                    raise last_exc
                break
            if not batch:
                break
            for row in batch:
                rows.append({k: row[k] for k in columns})
            last_id = int(batch[-1]["user_id"])
            stats["scanned"] = len(rows)
            if stats["scanned"] % 100_000 < page_size:
                print(
                    "{0} progress rows={1} last_id={2} elapsed={3:.1f}s".format(
                        label, stats["scanned"], last_id, time.time() - t0,
                    ),
                    flush=True,
                )
            if len(batch) < page_size:
                break
    finally:
        mig._close_mysql_conn(tgt)
    print(
        "{0} done rows={1} elapsed={2:.1f}s".format(label, len(rows), time.time() - t0),
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
    logger.log(
        "probe max user_id table={0} cap={1} exclude_app_ids={2}".format(
            table, max_user_id, exclude_app_ids,
        )
    )
    effective_max = _probe_max_user_id(
        env_path, table, max_user_id, exclude_app_ids=exclude_app_ids,
    )
    if effective_max <= 0:
        logger.log("load target {0}: no rows (max_user_id probe=0)".format(table))
        return {}
    logger.log(
        "load target {0}: workers={1} id_range=[0,{2}) page_size={3} exclude_app_ids={4}".format(
            table, workers, effective_max, page_size, exclude_app_ids,
        )
    )
    specs = []
    for i in range(workers):
        lo = (effective_max * i) // workers
        hi = (effective_max * (i + 1)) // workers
        specs.append(
            {
                "worker_id": i + 1,
                "workers": workers,
                "max_user_id": max_user_id,
                "range_lo": lo,
                "range_hi": hi,
                "page_size": page_size,
                "env_path": str(env_path.resolve()),
                "table": table,
                "columns": list(columns),
                "cols_sql": cols_sql,
                "exclude_app_ids": list(exclude_app_ids),
            }
        )
    t0 = time.time()
    merged = {}  # type: Dict[int, dict]
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
        "load target {0} done rows={1} elapsed={2:.1f}s".format(
            table, len(merged), time.time() - t0,
        )
    )
    return merged


def bankcard_key(row: dict) -> Tuple[int, str]:
    return (int(row["group_user_id"]), str(row["bank_account_number"]))


def product_key(row: dict) -> Tuple[int, str]:
    return (int(row["group_user_id"]), str(row["product_id"]))


def _probe_max_group_user_id(env_path: Path, table: str, max_gid: int) -> int:
    cfg = _worker_load_env(str(env_path))
    tgt = mig.connect_target(cfg)
    try:
        with tgt.cursor() as cur:
            cur.execute(
                """
                SELECT MAX(group_user_id) AS max_id
                FROM `{0}`
                WHERE group_user_id < %s
                """.format(table),
                (max_gid,),
            )
            row = cur.fetchone() or {}
        actual = int(row.get("max_id") or 0)
        return max(0, min(actual + 1, max_gid))
    finally:
        tgt.close()


def _load_target_group_user_shard(spec: dict) -> Tuple[int, List[dict], Dict[str, int]]:
    worker_id = int(spec["worker_id"])
    workers = int(spec["workers"])
    max_gid = int(spec["max_group_user_id"])
    page_size = int(spec["page_size"])
    env_path = str(spec["env_path"])
    table = str(spec["table"])
    columns = list(spec["columns"])  # type: List[str]
    cols_sql = spec["cols_sql"]
    order_tail = str(spec.get("order_tail") or "group_user_id ASC")
    lo = int(spec.get("range_lo", 0))
    hi = int(spec.get("range_hi", max_gid))

    label = "[target {0} gid {1}/{2}]".format(table, worker_id, workers)
    stats = {"scanned": 0}
    rows = []  # type: List[dict]

    cfg = _worker_load_env(env_path)
    tgt = mig.connect_target(cfg)
    last_gid = max(0, lo - 1)
    max_retries = max(3, int(cfg.get("mysql_batch_retries") or 6))
    t0 = time.time()
    print(
        "{0} load start range=[{1},{2}) page_size={3}".format(label, lo, hi, page_size),
        flush=True,
    )
    try:
        while True:
            batch = None
            last_exc = None  # type: Optional[BaseException]
            for attempt in range(max_retries):
                try:
                    try:
                        tgt.ping(reconnect=True)
                    except Exception:
                        mig._close_mysql_conn(tgt)
                        tgt = mig.connect_target(cfg)
                    with tgt.cursor() as cur:
                        cur.execute(
                            """
                            SELECT {cols_sql}
                            FROM `{table}`
                            WHERE group_user_id > %s
                              AND group_user_id >= %s AND group_user_id < %s
                            ORDER BY {order_tail}
                            LIMIT %s
                            """.format(
                                cols_sql=cols_sql, table=table, order_tail=order_tail,
                            ),
                            (last_gid, lo, hi, page_size),
                        )
                        batch = cur.fetchall()
                    break
                except Exception as exc:
                    last_exc = exc
                    if not _is_mysql_lost_conn(exc) or attempt >= max_retries - 1:
                        raise
                    delay = min(8.0, 0.5 * (2 ** attempt))
                    print(
                        "{0} mysql lost conn retry {1}/{2} sleep={3:.1f}s err={4}".format(
                            label, attempt + 1, max_retries, delay, exc,
                        ),
                        flush=True,
                    )
                    mig._close_mysql_conn(tgt)
                    time.sleep(delay)
                    tgt = mig.connect_target(cfg)
            if batch is None:
                if last_exc is not None:
                    raise last_exc
                break
            if not batch:
                break
            for row in batch:
                rows.append({k: row[k] for k in columns})
            last_gid = int(batch[-1]["group_user_id"])
            stats["scanned"] = len(rows)
            if stats["scanned"] % 100_000 < page_size:
                print(
                    "{0} progress rows={1} last_gid={2} elapsed={3:.1f}s".format(
                        label, stats["scanned"], last_gid, time.time() - t0,
                    ),
                    flush=True,
                )
            if len(batch) < page_size:
                break
    finally:
        mig._close_mysql_conn(tgt)
    print(
        "{0} done rows={1} elapsed={2:.1f}s".format(label, len(rows), time.time() - t0),
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
    logger.log("probe max group_user_id table={0} cap={1}".format(table, max_group_user_id))
    effective_max = _probe_max_group_user_id(env_path, table, max_group_user_id)
    if effective_max <= 0:
        logger.log("load target {0}: no rows".format(table))
        return {}
    logger.log(
        "load target {0}: workers={1} group_user_id_range=[0,{2}) page_size={3}".format(
            table, workers, effective_max, page_size,
        )
    )
    specs = []
    for i in range(workers):
        lo = (effective_max * i) // workers
        hi = (effective_max * (i + 1)) // workers
        specs.append(
            {
                "worker_id": i + 1,
                "workers": workers,
                "max_group_user_id": max_group_user_id,
                "range_lo": lo,
                "range_hi": hi,
                "page_size": page_size,
                "env_path": str(env_path.resolve()),
                "table": table,
                "columns": list(columns),
                "cols_sql": cols_sql,
                "order_tail": order_tail,
            }
        )
    t0 = time.time()
    merged = {}  # type: Dict[Tuple[Any, ...], dict]
    key_fn = bankcard_key if table == "user_bankcard" else product_key
    if workers == 1:
        _, chunk, _ = _load_target_group_user_shard(specs[0])
        for row in chunk:
            merged[key_fn(row)] = row
    else:
        ctx = multiprocessing.get_context("spawn")
        with ctx.Pool(processes=workers) as pool:
            parts = pool.map(_load_target_group_user_shard, specs)
        for _, chunk, _ in sorted(parts, key=lambda x: x[0]):
            for row in chunk:
                merged[key_fn(row)] = row
    logger.log(
        "load target {0} done rows={1} elapsed={2:.1f}s".format(
            table, len(merged), time.time() - t0,
        )
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
    limit: Optional[int] = None,
) -> List[dict]:
    """源 user：id>(lo,hi] 且 created>=since；limit 时 keyset 推进，跳过空 id 窗。"""
    m = "ng_loan_market"
    ex_ph = ",".join(["%s"] * len(exclude_app_ids)) if exclude_app_ids else ""
    exclude_sql = "AND u.`appId` NOT IN ({0})".format(ex_ph) if exclude_app_ids else ""
    limit_sql = "LIMIT %s" if limit else ""
    sql = """
        SELECT
            u.id AS user_id, u.`appId` AS app_id,
            u.mobile AS mobile_raw,
            CASE WHEN u.mobile LIKE '+234%' THEN u.mobile
                 WHEN u.mobile LIKE '234%' THEN CONCAT('+', u.mobile)
                 WHEN u.mobile LIKE '0%' THEN CONCAT('+234', SUBSTRING(u.mobile, 2))
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
        {limit_sql}
    """.format(m=m, exclude_sql=exclude_sql, limit_sql=limit_sql)
    params = [since_date, lo, hi, max_user_id]  # type: List[Any]
    if exclude_app_ids:
        params.extend(exclude_app_ids)
    if limit:
        params.append(int(limit))
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
    limit: Optional[int] = None,
) -> Tuple[List[dict], Dict[str, int]]:
    stats = {
        "source_rows": 0,
        "vt_skip": 0,
        "built": 0,
        "last_id": lo,
    }
    raw_rows = _select_source_users_since(
        src, since_date, lo, hi, max_user_id, limit=limit,
    )
    stats["source_rows"] = len(raw_rows)
    if not raw_rows:
        return [], stats
    stats["last_id"] = int(raw_rows[-1]["user_id"])

    prefix = "[plan {0},{1}]".format(lo, hi)
    # user 表只需 lup+dac；mobile VT 来自 user 行本身
    _, lookups = mig._fetch_user_batch_lookups(
        src, cfg, raw_rows, lo, hi, prefix, needed=("lup", "dac"),
    )
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


def _plan_user_span(
    cfg: Dict[str, Any],
    target_by_key: Dict[Tuple[Any, ...], dict],
    since_date: str,
    max_user_id: int,
    source_batch: int,
    logger: ReconcileLogger,
    span_lo: int,
    span_hi: int,
) -> Tuple[List[dict], Dict[str, int]]:
    src = mig.connect_source(cfg)
    vt = _make_vt_resolver(cfg, src)
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
        last_id = span_lo
        while last_id < span_hi:
            stats["source_batches"] += 1
            expected_rows, batch_stats = build_expected_user_rows(
                cfg, src, last_id, span_hi, since_date, max_user_id, vt, logger,
                limit=source_batch,
            )
            nsrc = int(batch_stats.get("source_rows") or 0)
            if nsrc <= 0:
                break
            stats["vt_skip"] += batch_stats.get("vt_skip", 0)
            stats["source_total"] += nsrc
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
            last_id = int(batch_stats.get("last_id") or last_id)
            if nsrc < source_batch:
                break
    finally:
        mig._close_mysql_conn(src)
    return plan, stats


def plan_user_table(
    cfg: Dict[str, Any],
    target_by_key: Dict[Tuple[Any, ...], dict],
    since_date: str,
    max_user_id: int,
    source_batch: int,
    vt: mig.VtTokenResolver,
    logger: ReconcileLogger,
    source_workers: int = 1,
) -> Tuple[List[dict], Dict[str, int]]:
    src = mig.connect_source(cfg)
    try:
        min_id, max_id, cnt = _source_user_id_bounds(src, since_date, max_user_id)
        logger.log(
            "source user since={0}: count={1} id=[{2},{3}]".format(
                since_date, cnt, min_id, max_id,
            )
        )
        if cnt <= 0:
            return [], {
                "source_total": 0, "skip_ok": 0, "plan_insert": 0,
                "plan_update": 0, "vt_skip": 0, "source_batches": 0,
            }
    finally:
        mig._close_mysql_conn(src)

    workers = max(1, int(source_workers or 1))
    spans = _partition_id_span(min_id, max_id, workers)
    logger.log(
        "plan user workers={0} batch={1} spans={2}".format(
            workers, source_batch, len(spans),
        )
    )

    def _shard(lo, hi):
        return _plan_user_span(
            cfg, target_by_key, since_date, max_user_id, source_batch, logger, lo, hi,
        )

    return _run_parallel_id_spans(spans, workers, _shard, logger, "plan user")


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



def tune_source_lookup_parallel(cfg: Dict[str, Any], source_workers: int, logger=None) -> None:
    """source_workers 高时压低 LOOKUP_PARALLEL，避免连接风暴。"""
    sw = max(1, int(source_workers or 1))
    cur = max(1, int(cfg.get("lookup_parallel") or 1))
    if sw >= 6 and cur > 2:
        cfg["lookup_parallel"] = 2
    elif sw >= 4 and cur > 3:
        cfg["lookup_parallel"] = 3
    if logger is not None and int(cfg.get("lookup_parallel") or 1) != cur:
        logger.log(
            "tune LOOKUP_PARALLEL {0} -> {1} (source_workers={2})".format(
                cur, cfg["lookup_parallel"], sw,
            )
        )


def setup_preloads(cfg: Dict[str, Any], logger: ReconcileLogger, vt_preload: bool) -> None:
    """VT / LUP 预加载进进程内存；仅 plan/all 需要（load-target 不翻 VT）。

    同进程内再次调用会复用已加载的全局 store，不重新扫库。
    """
    if vt_preload and cfg.get("vt_token_enable", True):
        if mig._vt_global_store is not None:
            logger.log(
                "VT preload reuse rows={0} (kept in memory)".format(
                    len(mig._vt_global_store)
                )
            )
        else:
            logger.log("VT preload start (kept in memory for whole run) ...")
            vt_n = mig.preload_vt_token_store(cfg)
            logger.log("VT preload done rows={0}".format(vt_n))
    elif not vt_preload:
        logger.log("VT preload skipped (--no-vt-preload); batch lookup per source chunk")
    if cfg.get("lup_preload", True):
        if mig._lup_global_store is not None:
            logger.log(
                "LUP preload reuse rows={0} (kept in memory)".format(
                    len(mig._lup_global_store)
                )
            )
        else:
            lup_n = mig.preload_lup_store(cfg)
            logger.log("LUP preload done rows={0}".format(lup_n))


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
    limit: Optional[int] = None,
) -> Tuple[List[dict], Dict[str, int]]:
    stats = {"source_rows": 0, "vt_skip": 0, "built": 0, "last_id": lo}
    raw_rows = _select_source_users_since(
        src, since_date, lo, hi, max_user_id, limit=limit,
    )
    stats["source_rows"] = len(raw_rows)
    if not raw_rows:
        return [], stats
    stats["last_id"] = int(raw_rows[-1]["user_id"])

    prefix = "[user_info plan {0},{1}]".format(lo, hi)
    _, lookups = mig._fetch_user_batch_lookups(
        src, cfg, raw_rows, lo, hi, prefix, needed=("ud", "lup", "uri", "dac"),
    )
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


def _plan_user_info_span(
    cfg: Dict[str, Any],
    target_by_id: Dict[int, dict],
    since_date: str,
    max_user_id: int,
    source_batch: int,
    logger: ReconcileLogger,
    span_lo: int,
    span_hi: int,
) -> Tuple[List[dict], Dict[str, int]]:
    src = mig.connect_source(cfg)
    vt = _make_vt_resolver(cfg, src)
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
        last_id = span_lo
        while last_id < span_hi:
            stats["source_batches"] += 1
            expected_rows, batch_stats = build_expected_user_info_rows(
                cfg, src, last_id, span_hi, since_date, max_user_id, vt, logger,
                limit=source_batch,
            )
            nsrc = int(batch_stats.get("source_rows") or 0)
            if nsrc <= 0:
                break
            stats["vt_skip"] += batch_stats.get("vt_skip", 0)
            stats["source_total"] += nsrc
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
            last_id = int(batch_stats.get("last_id") or last_id)
            if nsrc < source_batch:
                break
    finally:
        mig._close_mysql_conn(src)
    return plan, stats


def plan_user_info_table(
    cfg: Dict[str, Any],
    target_by_id: Dict[int, dict],
    since_date: str,
    max_user_id: int,
    source_batch: int,
    vt: mig.VtTokenResolver,
    logger: ReconcileLogger,
    source_workers: int = 1,
) -> Tuple[List[dict], Dict[str, int]]:
    src = mig.connect_source(cfg)
    try:
        min_id, max_id, cnt = _source_user_id_bounds(src, since_date, max_user_id)
        logger.log(
            "source user_info (via user.created>={0}): count={1} id=[{2},{3}]".format(
                since_date, cnt, min_id, max_id,
            )
        )
        if cnt <= 0:
            return [], {
                "source_total": 0, "skip_ok": 0, "plan_insert": 0,
                "plan_update": 0, "vt_skip": 0, "source_batches": 0,
            }
    finally:
        mig._close_mysql_conn(src)

    workers = max(1, int(source_workers or 1))
    spans = _partition_id_span(min_id, max_id, workers)
    logger.log(
        "plan user_info workers={0} batch={1} spans={2}".format(
            workers, source_batch, len(spans),
        )
    )

    def _shard(lo, hi):
        return _plan_user_info_span(
            cfg, target_by_id, since_date, max_user_id, source_batch, logger, lo, hi,
        )

    return _run_parallel_id_spans(spans, workers, _shard, logger, "plan user_info")


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

    if phase in ("plan", "all"):
        tune_source_lookup_parallel(cfg, getattr(args, "source_workers", 8), logger)
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
            source_workers=args.source_workers,
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

    # phase=all 仅在显式 --apply 时写库；无 --apply 只出 plan（DRY_RUN）
    if phase == "apply" or (phase == "all" and args.apply):
        if not plan:
            logger.log("apply skipped: empty plan")
        else:
            apply_stats = apply_fn(
                cfg, plan, args.apply_batch, args.apply_workers, dry_run, logger,
            )
            logger.log(f"apply stats={apply_stats}")
    elif phase == "all" and not args.apply:
        logger.log("apply skipped (no --apply / DRY_RUN)")

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
    needed=None,
    limit: Optional[int] = None,
) -> Tuple[List[dict], Dict[str, Any], set]:
    raw_rows = _select_source_users_since(
        src, since_date, lo, hi, max_user_id, limit=limit,
    )
    if not raw_rows:
        return [], {}, set()
    _, lookups = mig._fetch_user_batch_lookups(
        src, cfg, raw_rows, lo, hi, prefix, needed=needed,
    )
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
    limit: Optional[int] = None,
) -> Tuple[List[dict], Dict[str, int]]:
    stats = {"source_users": 0, "vt_skip": 0, "built": 0, "last_id": lo}
    prefix = "[bankcard plan {0},{1}]".format(lo, hi)
    raw_rows, lookups, user_ids = _source_users_batch_stats(
        cfg, src, lo, hi, since_date, max_user_id, prefix,
        needed=("ud",), limit=limit,
    )
    stats["source_users"] = len(user_ids)
    if not user_ids:
        return [], stats
    stats["last_id"] = int(raw_rows[-1]["user_id"])

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
    limit: Optional[int] = None,
) -> Tuple[List[dict], Dict[str, int]]:
    del vt  # user_product 无 VT 字段
    stats = {"source_users": 0, "built": 0, "last_id": lo}
    prefix = "[user_product plan {0},{1}]".format(lo, hi)
    raw_rows, lookups, user_ids = _source_users_batch_stats(
        cfg, src, lo, hi, since_date, max_user_id, prefix,
        needed=("prod",), limit=limit,
    )
    stats["source_users"] = len(user_ids)
    if not user_ids:
        return [], stats
    stats["last_id"] = int(raw_rows[-1]["user_id"])
    prod_src = [
        p for p in (lookups.get("prod_rows") or [])
        if int(p["userId"]) in user_ids
    ]
    rows = mig._build_user_product_rows(prod_src)
    built = [{c: row.get(c) for c in mig.USER_PRODUCT_COLS} for row in rows]
    stats["built"] = len(built)
    return built, stats


def _plan_composite_span(
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
    span_lo: int,
    span_hi: int,
) -> Tuple[List[dict], Dict[str, int]]:
    src = mig.connect_source(cfg)
    vt = _make_vt_resolver(cfg, src)
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
        last_id = span_lo
        while last_id < span_hi:
            stats["source_batches"] += 1
            expected_rows, batch_stats = build_fn(
                cfg, src, last_id, span_hi, since_date, max_user_id, vt, logger,
                limit=source_batch,
            )
            nsrc = int(batch_stats.get("source_users") or 0)
            if nsrc <= 0:
                break
            stats["vt_skip"] += batch_stats.get("vt_skip", 0)
            stats["source_total"] += nsrc
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
            last_id = int(batch_stats.get("last_id") or last_id)
            if nsrc < source_batch:
                break
    finally:
        mig._close_mysql_conn(src)
    return plan, stats


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
    source_workers: int = 1,
) -> Tuple[List[dict], Dict[str, int]]:
    src = mig.connect_source(cfg)
    try:
        min_id, max_id, cnt = _source_user_id_bounds(src, since_date, max_user_id)
        logger.log(
            "source {0} (user.created>={1}): users={2} id=[{3},{4}]".format(
                table, since_date, cnt, min_id, max_id,
            )
        )
        if cnt <= 0:
            return [], {
                "source_total": 0, "skip_ok": 0, "plan_insert": 0,
                "plan_update": 0, "vt_skip": 0, "source_batches": 0,
            }
    finally:
        mig._close_mysql_conn(src)

    workers = max(1, int(source_workers or 1))
    spans = _partition_id_span(min_id, max_id, workers)
    logger.log(
        "plan {0} workers={1} batch={2} spans={3}".format(
            table, workers, source_batch, len(spans),
        )
    )

    def _shard(lo, hi):
        return _plan_composite_span(
            cfg, target_by_key, since_date, max_user_id, source_batch,
            table, compare_cols, key_fn, build_fn, logger, lo, hi,
        )

    return _run_parallel_id_spans(
        spans, workers, _shard, logger, "plan {0}".format(table),
    )


def plan_user_bankcard_table(
    cfg: Dict[str, Any],
    target_by_key: Dict[Tuple[Any, ...], dict],
    since_date: str,
    max_user_id: int,
    source_batch: int,
    vt: mig.VtTokenResolver,
    logger: ReconcileLogger,
    source_workers: int = 1,
) -> Tuple[List[dict], Dict[str, int]]:
    return _plan_composite_table(
        cfg, target_by_key, since_date, max_user_id, source_batch,
        "user_bankcard", BANKCARD_COMPARE_COLS, bankcard_key,
        build_expected_bankcard_rows, logger, vt,
        source_workers=source_workers,
    )


def plan_user_product_table(
    cfg: Dict[str, Any],
    target_by_key: Dict[Tuple[Any, ...], dict],
    since_date: str,
    max_user_id: int,
    source_batch: int,
    vt: mig.VtTokenResolver,
    logger: ReconcileLogger,
    source_workers: int = 1,
) -> Tuple[List[dict], Dict[str, int]]:
    return _plan_composite_table(
        cfg, target_by_key, since_date, max_user_id, source_batch,
        "user_product", PRODUCT_COMPARE_COLS, product_key,
        build_expected_user_product_rows, logger, vt,
        source_workers=source_workers,
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

    if phase in ("plan", "all"):
        tune_source_lookup_parallel(cfg, getattr(args, "source_workers", 8), logger)
        if need_vt:
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
            source_workers=args.source_workers,
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

    if phase == "apply" or (phase == "all" and args.apply):
        if not plan:
            logger.log("apply skipped: empty plan")
        else:
            apply_stats = apply_fn(
                cfg, plan, args.apply_batch, args.apply_workers, dry_run, logger,
            )
            logger.log(f"apply stats={apply_stats}")
    elif phase == "all" and not args.apply:
        logger.log("apply skipped (no --apply / DRY_RUN)")

    logger.close()
    return 0


# ---------------------------------------------------------------------------
# application
# ---------------------------------------------------------------------------

def _probe_created_time_bounds(
    env_path: Path,
    table: str,
    since_ms: int,
    exclude_app_ids: Tuple[int, ...] = (),
) -> Tuple[int, int]:
    """可选探测；大表 MIN/MAX 易 2013，加载路径请优先用 _created_time_load_span。"""
    cfg = _worker_load_env(str(env_path))
    tgt = mig.connect_target(cfg)
    max_retries = max(3, int(cfg.get("mysql_batch_retries") or 6))
    try:
        exclude_sql = ""
        params = [since_ms]  # type: List[Any]
        if table == "application" and exclude_app_ids:
            ex_ph = ",".join(["%s"] * len(exclude_app_ids))
            exclude_sql = "AND app_id NOT IN ({0})".format(ex_ph)
            params.extend(exclude_app_ids)

        def _exec(cur):
            cur.execute(
                """
                SELECT MIN(created_time) AS min_t, MAX(created_time) AS max_t
                FROM `{table}`
                WHERE created_time >= %s
                  {exclude_sql}
                """.format(table=table, exclude_sql=exclude_sql),
                params,
            )
            return cur.fetchone() or {}

        tgt, row = _fetch_page_with_mysql_retry(
            "[probe {0} created_time]".format(table),
            cfg,
            tgt,
            max_retries,
            _exec,
        )
        min_t = int((row or {}).get("min_t") or 0)
        max_t = int((row or {}).get("max_t") or 0)
        if min_t <= 0 or max_t <= 0 or max_t < min_t:
            return 0, 0
        return min_t, max_t
    finally:
        mig._close_mysql_conn(tgt)


def _created_time_load_span(since_ms: int) -> Tuple[int, int]:
    """不查 MIN/MAX：用 since_ms ~ now+1天 做时间分片（避免探测拖垮目标库）。"""
    min_t = max(0, int(since_ms or 0))
    max_t = max(min_t, int(time.time() * 1000) + 86400000)
    return min_t, max_t


def _partition_created_time_span(
    min_t: int, max_t: int, workers: int,
) -> List[Tuple[int, int]]:
    """将 [min_t, max_t] 切成连续 [lo, hi) 区间（最后一段 hi=max_t+1）。"""
    workers = max(1, int(workers or 1))
    if max_t < min_t:
        return []
    span = (max_t - min_t) + 1
    workers = min(workers, span)
    chunk = (span + workers - 1) // workers
    out = []  # type: List[Tuple[int, int]]
    lo = min_t
    while lo <= max_t:
        hi = min(lo + chunk, max_t + 1)
        out.append((lo, hi))
        lo = hi
    return out


def _fetch_page_with_mysql_retry(label, cfg, tgt, max_retries, execute_fn):
    """execute_fn(cursor) -> batch；遇 2013/2006 重连重试。返回 (tgt, batch)。"""
    batch = None
    last_exc = None  # type: Optional[BaseException]
    for attempt in range(max_retries):
        try:
            try:
                tgt.ping(reconnect=True)
            except Exception:
                mig._close_mysql_conn(tgt)
                tgt = mig.connect_target(cfg)
            with tgt.cursor() as cur:
                batch = execute_fn(cur)
            return tgt, batch
        except Exception as exc:
            last_exc = exc
            if not _is_mysql_lost_conn(exc) or attempt >= max_retries - 1:
                raise
            delay = min(8.0, 0.5 * (2 ** attempt))
            print(
                "{0} mysql lost conn retry {1}/{2} sleep={3:.1f}s err={4}".format(
                    label, attempt + 1, max_retries, delay, exc,
                ),
                flush=True,
            )
            mig._close_mysql_conn(tgt)
            time.sleep(delay)
            tgt = mig.connect_target(cfg)
    if last_exc is not None:
        raise last_exc
    return tgt, []


def _load_target_application_shard(spec: dict) -> Tuple[int, List[dict], Dict[str, int]]:
    worker_id = int(spec["worker_id"])
    workers = int(spec["workers"])
    range_lo = int(spec["range_lo"])
    range_hi = int(spec["range_hi"])
    page_size = int(spec["page_size"])
    env_path = str(spec["env_path"])
    columns = list(spec["columns"])  # type: List[str]
    cols_sql = spec["cols_sql"]
    exclude_app_ids = tuple(spec["exclude_app_ids"])  # type: Tuple[int, ...]

    label = "[target application {0}/{1}]".format(worker_id, workers)
    stats = {"scanned": 0}
    rows = []  # type: List[dict]
    cfg = _worker_load_env(env_path)
    tgt = mig.connect_target(cfg)
    ex_ph = ",".join(["%s"] * len(exclude_app_ids)) if exclude_app_ids else ""
    exclude_sql = "AND app_id NOT IN ({0})".format(ex_ph) if exclude_app_ids else ""
    max_retries = max(3, int(cfg.get("mysql_batch_retries") or 6))
    last_ct = None  # type: Optional[int]
    last_app_no = ""
    t0 = time.time()
    print(
        "{0} load start created_time=[{1},{2}) page_size={3} exclude_app_ids={4}".format(
            label, range_lo, range_hi, page_size, exclude_app_ids,
        ),
        flush=True,
    )
    try:
        while True:
            def _exec(cur, _last_ct=last_ct, _last_app_no=last_app_no):
                if _last_ct is None:
                    cur.execute(
                        """
                        SELECT {cols_sql}
                        FROM `application`
                        WHERE created_time >= %s AND created_time < %s
                          {exclude_sql}
                          AND application_no IS NOT NULL AND application_no <> ''
                        ORDER BY created_time ASC, application_no ASC
                        LIMIT %s
                        """.format(cols_sql=cols_sql, exclude_sql=exclude_sql),
                        (range_lo, range_hi) + tuple(exclude_app_ids) + (page_size,),
                    )
                else:
                    cur.execute(
                        """
                        SELECT {cols_sql}
                        FROM `application`
                        WHERE created_time >= %s AND created_time < %s
                          {exclude_sql}
                          AND application_no IS NOT NULL AND application_no <> ''
                          AND (
                            created_time > %s
                            OR (created_time = %s AND application_no > %s)
                          )
                        ORDER BY created_time ASC, application_no ASC
                        LIMIT %s
                        """.format(cols_sql=cols_sql, exclude_sql=exclude_sql),
                        (range_lo, range_hi)
                        + tuple(exclude_app_ids)
                        + (_last_ct, _last_ct, _last_app_no, page_size),
                    )
                return cur.fetchall()

            tgt, batch = _fetch_page_with_mysql_retry(
                label, cfg, tgt, max_retries, _exec,
            )
            if not batch:
                break
            for row in batch:
                rows.append({k: row[k] for k in columns})
            last_ct = int(batch[-1]["created_time"] or 0)
            last_app_no = str(batch[-1]["application_no"] or "")
            stats["scanned"] = len(rows)
            if stats["scanned"] % 100_000 < page_size:
                print(
                    "{0} progress rows={1} last_ct={2} last={3} elapsed={4:.1f}s".format(
                        label, stats["scanned"], last_ct, last_app_no, time.time() - t0,
                    ),
                    flush=True,
                )
            if len(batch) < page_size:
                break
    finally:
        mig._close_mysql_conn(tgt)
    print(
        "{0} done rows={1} elapsed={2:.1f}s".format(label, len(rows), time.time() - t0),
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
    # 不探测 MIN/MAX（大表聚合易 2013）；since~now 时间分片 + 走 created_time 索引
    workers = max(1, min(int(load_workers or 1), 6))
    page_size = max(1000, min(int(page_size or 50000), 30000))
    min_t, max_t = _created_time_load_span(since_ms)
    spans = _partition_created_time_span(min_t, max_t, workers)
    workers = len(spans)
    cols_sql = _target_cols_sql(columns)
    specs = [
        {
            "worker_id": i + 1,
            "workers": workers,
            "range_lo": lo,
            "range_hi": hi,
            "page_size": page_size,
            "env_path": str(env_path.resolve()),
            "columns": list(columns),
            "cols_sql": cols_sql,
            "exclude_app_ids": list(exclude_app_ids),
        }
        for i, (lo, hi) in enumerate(spans)
    ]
    logger.log(
        "load target application: workers={0} created_time=[{1},{2}] "
        "exclude_app_ids={3} page_size={4} (time-range, no MIN/MAX probe)".format(
            workers, min_t, max_t, exclude_app_ids, page_size,
        )
    )
    t0 = time.time()
    merged = {}  # type: Dict[Tuple[Any, ...], dict]
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
        "load target application done rows={0} elapsed={1:.1f}s".format(
            len(merged), time.time() - t0,
        )
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
    limit: Optional[int] = None,
) -> List[dict]:
    m, c = "ng_loan_market", "ng_loan_core"
    ex_ph = ",".join(["%s"] * len(exclude_app_ids))
    sql = f"""
        SELECT
            a.id AS src_id,
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
    if limit:
        sql = sql.rstrip() + "\n        LIMIT %s\n    "
    params: List[Any] = list(exclude_app_ids) + [since_unix, lo, hi]
    if limit:
        params.append(int(limit))
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
    limit: Optional[int] = None,
) -> Tuple[List[dict], Dict[str, int]]:
    stats = {
        "source_rows": 0, "vt_skip": 0, "no_core_sn": 0, "built": 0, "last_id": lo,
    }
    raw_rows = _select_application_source_since(
        src, lo, hi, since_unix, exclude_app_ids, limit=limit,
    )
    stats["source_rows"] = len(raw_rows)
    if not raw_rows:
        return [], stats
    stats["last_id"] = int(raw_rows[-1].get("src_id") or lo)

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


def _plan_application_span(
    cfg: Dict[str, Any],
    target_by_key: Dict[Tuple[Any, ...], dict],
    since_unix: int,
    exclude_app_ids: Tuple[int, ...],
    source_batch: int,
    logger: ReconcileLogger,
    span_lo: int,
    span_hi: int,
) -> Tuple[List[dict], Dict[str, int]]:
    src = mig.connect_source(cfg)
    vt = _make_vt_resolver(cfg, src)
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
        last_id = span_lo
        while last_id < span_hi:
            stats["source_batches"] += 1
            expected_rows, batch_stats = build_expected_application_rows(
                cfg, src, last_id, span_hi, since_unix, exclude_app_ids, vt, logger,
                limit=source_batch,
            )
            nsrc = int(batch_stats.get("source_rows") or 0)
            if nsrc <= 0:
                break
            stats["vt_skip"] += batch_stats.get("vt_skip", 0)
            stats["no_core_sn"] += batch_stats.get("no_core_sn", 0)
            stats["source_total"] += nsrc
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
            last_id = int(batch_stats.get("last_id") or last_id)
            if nsrc < source_batch:
                break
    finally:
        mig._close_mysql_conn(src)
    return plan, stats


def plan_application_table(
    cfg: Dict[str, Any],
    target_by_key: Dict[Tuple[Any, ...], dict],
    since_date: str,
    exclude_app_ids: Tuple[int, ...],
    source_batch: int,
    vt: mig.VtTokenResolver,
    logger: ReconcileLogger,
    source_workers: int = 1,
) -> Tuple[List[dict], Dict[str, int]]:
    since_unix = since_date_to_unix(since_date)
    src = mig.connect_source(cfg)
    try:
        min_id, max_id, cnt = _source_application_id_bounds(
            src, since_unix, exclude_app_ids,
        )
        logger.log(
            "source application applyDate>={0}: count={1} id=[{2},{3}] exclude_app_ids={4}".format(
                since_date, cnt, min_id, max_id, exclude_app_ids,
            )
        )
        if cnt <= 0:
            return [], {
                "source_total": 0, "skip_ok": 0, "plan_insert": 0,
                "plan_update": 0, "vt_skip": 0, "no_core_sn": 0, "source_batches": 0,
            }
    finally:
        mig._close_mysql_conn(src)

    workers = max(1, int(source_workers or 1))
    spans = _partition_id_span(min_id, max_id, workers)
    logger.log(
        "plan application workers={0} batch={1} spans={2}".format(
            workers, source_batch, len(spans),
        )
    )

    def _shard(lo, hi):
        return _plan_application_span(
            cfg, target_by_key, since_unix, exclude_app_ids,
            source_batch, logger, lo, hi,
        )

    return _run_parallel_id_spans(spans, workers, _shard, logger, "plan application")


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

    if phase in ("plan", "all"):
        tune_source_lookup_parallel(cfg, getattr(args, "source_workers", 8), logger)
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
            source_workers=args.source_workers,
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

    if phase == "apply" or (phase == "all" and args.apply):
        if not plan:
            logger.log("apply skipped: empty plan")
        else:
            apply_stats = apply_application_plan(
                cfg, plan, args.apply_batch, args.apply_workers, dry_run, logger,
            )
            logger.log(f"apply stats={apply_stats}")
    elif phase == "all" and not args.apply:
        logger.log("apply skipped (no --apply / DRY_RUN)")

    logger.close()
    return 0


# ---------------------------------------------------------------------------
# loan（已放款 application → 有且仅一条 loan；loan_no 中间段 = core repay_plan.sn）
# ---------------------------------------------------------------------------

def _load_target_loan_shard(spec: dict) -> Tuple[int, List[dict], Dict[str, int]]:
    worker_id = int(spec["worker_id"])
    workers = int(spec["workers"])
    range_lo = int(spec["range_lo"])
    range_hi = int(spec["range_hi"])
    page_size = int(spec["page_size"])
    env_path = str(spec["env_path"])
    columns = list(spec["columns"])  # type: List[str]
    cols_sql = spec["cols_sql"]
    exclude_created_ms = tuple(spec["exclude_created_ms"])  # type: Tuple[int, ...]
    exclude_app_ids = tuple(spec["exclude_app_ids"])  # type: Tuple[int, ...]

    label = "[target loan {0}/{1}]".format(worker_id, workers)
    stats = {
        "scanned": 0,
        "skipped_excluded_app": 0,
        "skipped_excluded_created_ms": 0,
    }
    rows = []  # type: List[dict]
    cfg = _worker_load_env(env_path)
    tgt = mig.connect_target(cfg)
    exclude_created_set = (
        set(int(x) for x in exclude_created_ms) if exclude_created_ms else set()
    )
    max_retries = max(3, int(cfg.get("mysql_batch_retries") or 6))
    last_ct = None  # type: Optional[int]
    last_app_no = ""
    last_period = -1
    last_roll = -1
    t0 = time.time()
    print(
        "{0} load start created_time=[{1},{2}) page_size={3} exclude_app_ids={4} "
        "exclude_created_ms={5}".format(
            label, range_lo, range_hi, page_size, exclude_app_ids, exclude_created_ms,
        ),
        flush=True,
    )
    try:
        while True:
            def _exec(
                cur,
                _last_ct=last_ct,
                _last_app_no=last_app_no,
                _last_period=last_period,
                _last_roll=last_roll,
            ):
                if _last_ct is None:
                    cur.execute(
                        """
                        SELECT {cols_sql}
                        FROM `loan`
                        WHERE created_time >= %s AND created_time < %s
                          AND application_no IS NOT NULL AND application_no <> ''
                        ORDER BY created_time ASC, application_no ASC,
                                 period ASC, roll_sequence ASC
                        LIMIT %s
                        """.format(cols_sql=cols_sql),
                        (range_lo, range_hi, page_size),
                    )
                else:
                    cur.execute(
                        """
                        SELECT {cols_sql}
                        FROM `loan`
                        WHERE created_time >= %s AND created_time < %s
                          AND application_no IS NOT NULL AND application_no <> ''
                          AND (
                            created_time > %s
                            OR (created_time = %s AND application_no > %s)
                            OR (
                              created_time = %s AND application_no = %s
                              AND period > %s
                            )
                            OR (
                              created_time = %s AND application_no = %s
                              AND period = %s AND roll_sequence > %s
                            )
                          )
                        ORDER BY created_time ASC, application_no ASC,
                                 period ASC, roll_sequence ASC
                        LIMIT %s
                        """.format(cols_sql=cols_sql),
                        (
                            range_lo, range_hi,
                            _last_ct,
                            _last_ct, _last_app_no,
                            _last_ct, _last_app_no, _last_period,
                            _last_ct, _last_app_no, _last_period, _last_roll,
                            page_size,
                        ),
                    )
                return cur.fetchall()

            tgt, batch = _fetch_page_with_mysql_retry(
                label, cfg, tgt, max_retries, _exec,
            )
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
            last = batch[-1]
            last_ct = int(last.get("created_time") or 0)
            last_app_no = str(last.get("application_no") or "")
            last_period = int(last.get("period") if last.get("period") is not None else 1)
            last_roll = int(
                last.get("roll_sequence") if last.get("roll_sequence") is not None else 0
            )
            stats["scanned"] = len(rows)
            if stats["scanned"] % 100_000 < page_size:
                print(
                    "{0} progress rows={1} skipped_app={2} skipped_created={3} "
                    "last={4} elapsed={5:.1f}s".format(
                        label,
                        stats["scanned"],
                        stats["skipped_excluded_app"],
                        stats["skipped_excluded_created_ms"],
                        last_app_no,
                        time.time() - t0,
                    ),
                    flush=True,
                )
            if len(batch) < page_size:
                break
    finally:
        mig._close_mysql_conn(tgt)
    print(
        "{0} done rows={1} skipped_app={2} skipped_created={3} elapsed={4:.1f}s".format(
            label,
            len(rows),
            stats["skipped_excluded_app"],
            stats["skipped_excluded_created_ms"],
            time.time() - t0,
        ),
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
    workers = max(1, min(int(load_workers or 1), 6))
    page_size = max(1000, min(int(page_size or 50000), 30000))
    min_t, max_t = _created_time_load_span(since_ms)
    spans = _partition_created_time_span(min_t, max_t, workers)
    workers = len(spans)
    cols_sql = _target_cols_sql(columns)
    specs = [
        {
            "worker_id": i + 1,
            "workers": workers,
            "range_lo": lo,
            "range_hi": hi,
            "page_size": page_size,
            "env_path": str(env_path.resolve()),
            "columns": list(columns),
            "cols_sql": cols_sql,
            "exclude_app_ids": list(exclude_app_ids),
            "exclude_created_ms": list(exclude_created_ms),
        }
        for i, (lo, hi) in enumerate(spans)
    ]
    logger.log(
        "load target loan: workers={0} created_time=[{1},{2}] "
        "exclude_app_ids={3} exclude_created_ms={4} page_size={5} "
        "(time-range, no MIN/MAX probe)".format(
            workers, min_t, max_t, exclude_app_ids, exclude_created_ms, page_size,
        )
    )
    t0 = time.time()
    merged = {}  # type: Dict[Tuple[Any, ...], dict]
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
        "load target loan done rows={0} elapsed={1:.1f}s".format(
            len(merged), time.time() - t0,
        )
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
    limit: Optional[int] = None,
) -> List[dict]:
    """源库已放款单（disburseTime>0），applyDate >= since_unix。"""
    m, c = "ng_loan_market", "ng_loan_core"
    ex_ph = ",".join(["%s"] * len(exclude_app_ids))
    sql = f"""
        SELECT
            a.id AS src_id,
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
    if limit:
        sql = sql.rstrip() + "\n        LIMIT %s\n    "
    params: List[Any] = list(exclude_app_ids) + [since_unix, lo, hi]
    if limit:
        params.append(int(limit))
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
    limit: Optional[int] = None,
) -> Tuple[List[dict], Dict[str, int]]:
    stats = {
        "source_rows": 0, "vt_skip": 0, "no_core_sn": 0,
        "no_application_no": 0, "built": 0, "last_id": lo,
    }
    raw_rows = _select_disbursed_application_source_since(
        src, lo, hi, since_unix, exclude_app_ids, limit=limit,
    )
    stats["source_rows"] = len(raw_rows)
    if not raw_rows:
        return [], stats
    stats["last_id"] = int(raw_rows[-1].get("src_id") or lo)

    user_ids = [int(r["user_id"]) for r in raw_rows]
    # loan 只要 bvn（VT 过滤）；repay_map 无用，跳过源库 repay 查询
    bvn_map, _repay_map = mig._fetch_app_lookup_maps(
        cfg, src, user_ids, [], need_repay=False,
    )
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


def _plan_loan_span(
    cfg: Dict[str, Any],
    target_by_key: Dict[Tuple[Any, ...], dict],
    since_unix: int,
    exclude_app_ids: Tuple[int, ...],
    source_batch: int,
    logger: ReconcileLogger,
    span_lo: int,
    span_hi: int,
) -> Tuple[List[dict], Dict[str, int]]:
    src = mig.connect_source(cfg)
    vt = _make_vt_resolver(cfg, src)
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
        last_id = span_lo
        while last_id < span_hi:
            stats["source_batches"] += 1
            expected_rows, batch_stats = build_expected_loan_rows(
                cfg, src, last_id, span_hi, since_unix, exclude_app_ids, vt, logger,
                limit=source_batch,
            )
            nsrc = int(batch_stats.get("source_rows") or 0)
            if nsrc <= 0:
                break
            stats["vt_skip"] += batch_stats.get("vt_skip", 0)
            stats["no_core_sn"] += batch_stats.get("no_core_sn", 0)
            stats["no_application_no"] += batch_stats.get("no_application_no", 0)
            stats["source_total"] += nsrc
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
            last_id = int(batch_stats.get("last_id") or last_id)
            if nsrc < source_batch:
                break
    finally:
        mig._close_mysql_conn(src)
    return plan, stats


def plan_loan_table(
    cfg: Dict[str, Any],
    target_by_key: Dict[Tuple[Any, ...], dict],
    since_date: str,
    exclude_app_ids: Tuple[int, ...],
    source_batch: int,
    vt: mig.VtTokenResolver,
    logger: ReconcileLogger,
    source_workers: int = 1,
) -> Tuple[List[dict], Dict[str, int]]:
    since_unix = since_date_to_unix(since_date)
    src = mig.connect_source(cfg)
    try:
        min_id, max_id, cnt = _source_disbursed_application_id_bounds(
            src, since_unix, exclude_app_ids,
        )
        logger.log(
            "source loan (disbursed applyDate>={0}): count={1} id=[{2},{3}] exclude_app_ids={4}".format(
                since_date, cnt, min_id, max_id, exclude_app_ids,
            )
        )
        if cnt <= 0:
            return [], {
                "source_total": 0, "skip_ok": 0, "plan_insert": 0,
                "plan_update": 0, "vt_skip": 0, "no_core_sn": 0,
                "no_application_no": 0, "source_batches": 0,
            }
    finally:
        mig._close_mysql_conn(src)

    workers = max(1, int(source_workers or 1))
    spans = _partition_id_span(min_id, max_id, workers)
    logger.log(
        "plan loan workers={0} batch={1} spans={2}".format(
            workers, source_batch, len(spans),
        )
    )

    def _shard(lo, hi):
        return _plan_loan_span(
            cfg, target_by_key, since_unix, exclude_app_ids,
            source_batch, logger, lo, hi,
        )

    return _run_parallel_id_spans(spans, workers, _shard, logger, "plan loan")


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

    if phase in ("plan", "all"):
        tune_source_lookup_parallel(cfg, getattr(args, "source_workers", 8), logger)
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
            source_workers=args.source_workers,
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

    if phase == "apply" or (phase == "all" and args.apply):
        if not plan:
            logger.log("apply skipped: empty plan")
        else:
            apply_stats = apply_loan_plan(
                cfg, plan, args.apply_batch, args.apply_workers, dry_run, logger,
            )
            logger.log(f"apply stats={apply_stats}")
    elif phase == "all" and not args.apply:
        logger.log("apply skipped (no --apply / DRY_RUN)")

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


def dispatch_table(args: argparse.Namespace, cfg: Dict[str, Any], table: str) -> int:
    if table == "user":
        return run_user_reconcile(args, cfg)
    if table == "user_info":
        return run_user_info_reconcile(args, cfg)
    if table == "user_bankcard":
        return run_user_bankcard_reconcile(args, cfg)
    if table == "user_product":
        return run_user_product_reconcile(args, cfg)
    if table == "application":
        return run_application_reconcile(args, cfg)
    if table == "loan":
        return run_loan_reconcile(args, cfg)
    print(
        "table={0} 尚未实现；已实现: {1}".format(table, ", ".join(SUPPORTED_TABLES)),
        file=sys.stderr,
    )
    return 2


def run_all_tables(args: argparse.Namespace, cfg: Dict[str, Any]) -> int:
    """单进程顺序跑多表；VT/LUP 只加载一次，后续表复用内存。"""
    start = getattr(args, "start_table", None) or SUPPORTED_TABLES[0]
    if start not in SUPPORTED_TABLES:
        print("unknown START_TABLE/start-table: {0}".format(start), file=sys.stderr)
        return 2

    Path(args.log_dir).mkdir(parents=True, exist_ok=True)
    master = ReconcileLogger(Path(args.log_dir), "all")
    started = False
    overall_t0 = time.time()
    master.log(
        "reconcile_all start since={0} apply={1} start_table={2}".format(
            args.since_date, bool(args.apply), start,
        )
    )
    # 先一次性预加载，后续各表 setup_preloads 走 reuse
    tune_source_lookup_parallel(cfg, getattr(args, "source_workers", 8), master)
    if args.vt_preload or cfg.get("lup_preload", True):
        setup_preloads(cfg, master, args.vt_preload)

    for table in SUPPORTED_TABLES:
        if not started:
            if table != start:
                master.log("skip table={0} (before start_table={1})".format(table, start))
                continue
            started = True
        t0 = time.time()
        master.log("========== BEGIN table={0} ==========".format(table))
        table_args = argparse.Namespace(**vars(args))
        table_args.table = table
        table_args.phase = "all"
        paths = default_paths(table)
        table_args.target_cache = paths["target_cache"]
        table_args.plan_file = paths["plan_file"]
        table_args.from_cache = bool(getattr(args, "from_cache", False))
        rc = dispatch_table(table_args, cfg, table)
        master.log(
            "========== DONE table={0} rc={1} elapsed={2}s ==========".format(
                table, rc, int(time.time() - t0),
            )
        )
        if rc != 0:
            master.log("reconcile_all aborted at table={0} rc={1}".format(table, rc))
            master.close()
            return rc

    master.log(
        "reconcile_all finished OK elapsed={0}s".format(int(time.time() - overall_t0))
    )
    master.close()
    return 0


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(description="Table-by-table source-target reconcile")
    p.add_argument("--env", default=str(HERE / "ng_migration.env"))
    p.add_argument(
        "--table",
        default="",
        choices=SUPPORTED_TABLES + ("",),
        help="目标表；与 --all-tables 二选一",
    )
    p.add_argument(
        "--all-tables",
        action="store_true",
        help="单进程按顺序跑全部表；VT/LUP 只预加载一次并常驻",
    )
    p.add_argument(
        "--start-table",
        default="user",
        choices=SUPPORTED_TABLES,
        help="--all-tables 时从该表开始（含）",
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
    p.add_argument("--load-workers", type=int, default=16)
    p.add_argument("--source-workers", type=int, default=8,
                    help="plan 源库 id 区间并行线程数（吃空闲 CPU）")
    p.add_argument("--page-size", type=int, default=100000)
    p.add_argument("--source-batch", type=int, default=20000)
    p.add_argument("--apply-batch", type=int, default=1000, help="apply 每批行数")
    p.add_argument("--apply-workers", type=int, default=24, help="apply 并行线程数")
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

    if args.all_tables:
        return run_all_tables(args, cfg)

    if not args.table:
        print("需要 --table 或 --all-tables", file=sys.stderr)
        return 2

    paths = default_paths(args.table)
    if not args.target_cache:
        args.target_cache = paths["target_cache"]
    if not args.plan_file:
        args.plan_file = paths["plan_file"]

    return dispatch_table(args, cfg, args.table)


if __name__ == "__main__":
    raise SystemExit(main())
