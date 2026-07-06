#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
尼日老库 → id 库 跨机迁移脚本（源库与目标库可不在同一 MySQL 实例）

源库：10.52.x ng_loan_market / ng_loan_core（只读）
目标库：由 ng_migration.env 的 TARGET_* 配置（仅写正式表，不使用 dt_mig_* 物化表）

group_user_id = user_id（写入时直接用 user_id，不再单独查 user 表）
每批从源库 SELECT，Python 组装后 bulk 写正式表。

依赖：pip install pymysql（可选 pip install orjson 加速 JSON 组装）

用法：
  python3 ng_migration_run.py full         # 推荐：user + application 全量（支持断点续跑）
  python3 ng_migration_run.py user         # user + user_info + user_bankcard + user_product
  python3 ng_migration_run.py user_info    # 仅 user_info
  python3 ng_migration_run.py application  # application + loan + id_mapping
  python3 ng_migration_run.py verify

断点续跑：DROP_MAT_ON_START=0，保留 PROGRESS_FILE（含 full_user_done / user_lo.W* / app_lo.W*）
全新全量：DROP_MAT_ON_START=1 或删除 PROGRESS_FILE
  python3 ng_migration_run.py drop_staging # 手动删除目标库遗留 dt_mig_* 表
"""
import argparse
import json
import os
import random
import sys
import threading
import time
from datetime import date, datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

try:
    import pymysql
    from pymysql.cursors import DictCursor
except ImportError:
    print("请先安装: pip install pymysql", file=sys.stderr)
    sys.exit(1)

try:
    import orjson as _orjson

    def _json_dumps(obj: Any) -> str:
        return _orjson.dumps(obj, default=str).decode("utf-8")

    _USING_ORJSON = True
except ImportError:
    def _json_dumps(obj: Any) -> str:
        return json.dumps(obj, ensure_ascii=False, default=str)

    _USING_ORJSON = False

HERE = Path(__file__).resolve().parent
ENV_FILE = HERE / "ng_migration.env"

def _user_product_schemes_json(credit_amount: int) -> str:
    return _json_dumps([
        {
            "schemeId": "PROD-001-D7",
            "amountRange": [int(credit_amount)],
        },
    ])


APPLICATION_SCHEME_PARAM_JSON = _json_dumps({
    "penalty_rate": 0.05,
    "upfront_rate": 0.35,
    "interest_rate": 0,
    "post_paid_rate": 0.05,
})


def _strip_env_value(v: str) -> str:
    v = v.strip()
    if len(v) >= 2 and v[0] == v[-1] and v[0] in ("'", '"'):
        return v[1:-1]
    return v


def load_env() -> Dict[str, str]:
    if ENV_FILE.exists():
        for line in ENV_FILE.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), _strip_env_value(v))
    cfg = {
        "source_host": os.environ.get("SOURCE_HOST", "127.0.0.1"),
        "source_port": int(os.environ.get("SOURCE_PORT", "3306")),
        "source_user": os.environ.get("SOURCE_USER", "root"),
        "source_password": os.environ.get("SOURCE_PASSWORD", ""),
        "target_host": os.environ.get("TARGET_HOST", "127.0.0.1"),
        "target_port": int(os.environ.get("TARGET_PORT", "3306")),
        "target_user": os.environ.get("TARGET_USER", "id"),
        "target_password": os.environ.get("TARGET_PASSWORD", ""),
        "target_db": os.environ.get("TARGET_DB", "id"),
        "max_user_id": int(os.environ.get("MAX_USER_ID", "9153604")),
        "max_app_id": os.environ.get("MAX_APP_ID", "").strip(),
        "user_batch": int(os.environ.get("USER_BATCH", "20000")),
        "user_insert_batch": int(os.environ.get("USER_INSERT_BATCH", "20000")),
        "app_batch": int(os.environ.get("APP_BATCH", "100000")),
        "app_insert_batch": int(os.environ.get("APP_INSERT_BATCH", "10000")),
        "id_mapping_insert_batch": int(
            os.environ.get("ID_MAPPING_INSERT_BATCH", "25000"),
        ),
        "app_worker_balance": os.environ.get("APP_WORKER_BALANCE", "count").strip().lower(),
        "workers": int(os.environ.get("WORKERS", "10")),
        "app_workers": int(os.environ.get("APP_WORKERS", "8")),
        "app_active_workers": int(os.environ.get("APP_ACTIVE_WORKERS", "0")),
        "max_worker_slots": int(os.environ.get("MAX_WORKER_SLOTS", "64")),
        "lookup_parallel": int(os.environ.get("LOOKUP_PARALLEL", "4")),
        "lup_pair_chunk": int(os.environ.get("LUP_PAIR_CHUNK", "400")),
        "progress_file": os.environ.get("PROGRESS_FILE", "/tmp/ng_mig_progress.env"),
        "progress_save_every": max(1, int(os.environ.get("PROGRESS_SAVE_EVERY", "3"))),
        "log_file": os.environ.get("LOG_FILE", "").strip(),
        "skip_log_file": os.environ.get("SKIP_LOG_FILE", "").strip(),
        "log_every": int(os.environ.get("LOG_EVERY", "20")),
        "lo": os.environ.get("LO", "").strip(),
        "hi": os.environ.get("HI", "").strip(),
        "drop_mat_on_start": os.environ.get("DROP_MAT_ON_START", "1").strip().lower() in (
            "1", "true", "yes",
        ),
        "deadlock_max_retries": int(os.environ.get("DEADLOCK_MAX_RETRIES", "8")),
        "insert_row_retries": int(os.environ.get("INSERT_ROW_RETRIES", "3")),
        "vt_token_enable": os.environ.get("VT_TOKEN_ENABLE", "1").strip().lower() in (
            "1", "true", "yes",
        ),
        "vt_token_chunk": int(os.environ.get("VT_TOKEN_CHUNK", "2000")),
        "vt_token_db": os.environ.get("VT_TOKEN_DB", "ng_loan_market").strip(),
        "vt_preload": os.environ.get("VT_PRELOAD", "1").strip().lower() in (
            "1", "true", "yes",
        ),
        "lup_preload": os.environ.get("LUP_PRELOAD", "1").strip().lower() in (
            "1", "true", "yes",
        ),
        "snowflake_worker_id": int(os.environ.get("SNOWFLAKE_WORKER_ID", "1")),
        "snowflake_epoch_ms": int(os.environ.get("SNOWFLAKE_EPOCH_MS", "1577836800000")),
    }
    return cfg


_vt_global_store: Optional[Dict[Tuple[str, str], str]] = None
_lup_global_store: Optional[Dict[Tuple[Any, str], str]] = None
_vt_preload_lock = threading.Lock()
_lup_preload_lock = threading.Lock()


class SnowflakeIdGenerator:
    """64-bit snowflake IDs: timestamp, worker id, sequence."""

    def __init__(
        self,
        worker_id: int = 1,
        epoch_ms: int = 1577836800000,
        sequence_bits: int = 12,
        worker_bits: int = 10,
    ):
        if worker_id < 0 or worker_id >= (1 << worker_bits):
            raise ValueError(f"worker_id must be between 0 and {(1 << worker_bits) - 1}")
        self.worker_id = int(worker_id)
        self.epoch_ms = int(epoch_ms)
        self.sequence_bits = int(sequence_bits)
        self.worker_bits = int(worker_bits)
        self.sequence_mask = (1 << self.sequence_bits) - 1
        self.last_ms = -1
        self.sequence = 0
        self._lock = threading.Lock()

    def next_id(self) -> int:
        with self._lock:
            now_ms = int(time.time() * 1000)
            if now_ms < self.last_ms:
                now_ms = self.last_ms
            if now_ms == self.last_ms:
                self.sequence = (self.sequence + 1) & self.sequence_mask
                if self.sequence == 0:
                    while now_ms <= self.last_ms:
                        time.sleep(0.001)
                        now_ms = int(time.time() * 1000)
            else:
                self.sequence = 0
            self.last_ms = now_ms
            timestamp_part = now_ms - self.epoch_ms
            if timestamp_part < 0:
                raise ValueError("current time is before snowflake epoch")
            return (
                (timestamp_part << (self.worker_bits + self.sequence_bits))
                | (self.worker_id << self.sequence_bits)
                | self.sequence
            )


_snowflake_global: Optional[SnowflakeIdGenerator] = None
_snowflake_global_key: Optional[Tuple[int, int]] = None
_snowflake_global_lock = threading.Lock()


def get_snowflake_generator(cfg: Optional[Dict[str, Any]] = None) -> SnowflakeIdGenerator:
    cfg = cfg or {}
    worker_id = int(cfg.get("snowflake_worker_id") or os.environ.get("SNOWFLAKE_WORKER_ID", "1"))
    epoch_ms = int(cfg.get("snowflake_epoch_ms") or os.environ.get("SNOWFLAKE_EPOCH_MS", "1577836800000"))
    key = (worker_id, epoch_ms)
    global _snowflake_global, _snowflake_global_key
    with _snowflake_global_lock:
        if _snowflake_global is None or _snowflake_global_key != key:
            _snowflake_global = SnowflakeIdGenerator(worker_id=worker_id, epoch_ms=epoch_ms)
            _snowflake_global_key = key
        return _snowflake_global


def next_snowflake_id(cfg: Optional[Dict[str, Any]] = None) -> int:
    return get_snowflake_generator(cfg).next_id()


def connect_source(cfg: Dict[str, Any]):
    return pymysql.connect(
        host=cfg["source_host"],
        port=cfg["source_port"],
        user=cfg["source_user"],
        password=cfg["source_password"],
        charset="utf8mb4",
        autocommit=False,
        cursorclass=DictCursor,
    )


def connect_target(cfg: Dict[str, Any]):
    return pymysql.connect(
        host=cfg["target_host"],
        port=cfg["target_port"],
        user=cfg["target_user"],
        password=cfg["target_password"],
        database=cfg["target_db"],
        charset="utf8mb4",
        autocommit=False,
        cursorclass=DictCursor,
        connect_timeout=int(cfg.get("mysql_connect_timeout") or 60),
        read_timeout=int(cfg.get("mysql_read_timeout") or 3600),
        write_timeout=int(cfg.get("mysql_write_timeout") or 3600),
    )


def _close_mysql_conn(conn) -> None:
    """释放 MySQL 连接（忽略关闭异常）。"""
    if conn is None:
        return
    try:
        conn.close()
    except Exception:
        pass


def _new_mysql_conn(cfg: Dict[str, Any], kind: str):
    if kind == "target":
        conn = connect_target(cfg)
        _session_opts(conn)
        return conn
    return connect_source(cfg)


def _ping_mysql_conn(
    conn,
    cfg: Optional[Dict[str, Any]] = None,
    kind: str = "target",
):
    """批内写入前保活；断线则 rollback 后抛出，由外层整批重试。"""
    try:
        conn.ping(reconnect=False)
        return conn
    except Exception as exc:
        if cfg is None:
            raise
        try:
            conn.rollback()
        except Exception:
            pass
        _close_mysql_conn(conn)
        if isinstance(exc, pymysql.err.Error):
            raise
        raise pymysql.err.OperationalError(2013, str(exc)) from exc


def _is_batch_retryable_error(exc: BaseException) -> bool:
    """整批重试：死锁 1213 或连接/通信类瞬时错误（含 2013）。"""
    return _is_deadlock_error(exc) or _is_transient_insert_error(exc)


def _batch_retry_backoff(attempt: int) -> float:
    return min(3.0, 0.15 * (2 ** attempt)) + random.uniform(0, 0.2)


def _log_batch_retry(
    prefix: str,
    exc: BaseException,
    attempt: int,
    max_retries: int,
    delay: float,
) -> None:
    if _is_deadlock_error(exc):
        reason = "deadlock 1213"
    elif isinstance(exc, pymysql.err.OperationalError) and exc.args:
        reason = f"mysql errno={exc.args[0]}"
    elif isinstance(exc, pymysql.err.InterfaceError):
        reason = "interface error"
    else:
        reason = type(exc).__name__
    mig_log(
        f"{prefix} {reason}, batch retry {attempt + 1}/"
        f"{max_retries - 1} sleep={delay:.2f}s"
    )


def _prepare_batch_retry_tgt(out_tgt, cfg: Dict[str, Any], kind: str = "target"):
    """批次失败后 rollback 并换新连接，供下一轮重试。"""
    try:
        out_tgt.rollback()
    except Exception:
        pass
    return _reconnect_mysql(out_tgt, cfg, kind)


def _ensure_mysql_conn(conn, cfg: Dict[str, Any], kind: str):
    """长查询后、批写入开始前保活；断线则重连（仅在尚未写入目标库时调用）。"""
    try:
        conn.ping(reconnect=True)
        return conn
    except Exception:
        _close_mysql_conn(conn)
        return _new_mysql_conn(cfg, kind)


def _reconnect_mysql(conn, cfg: Dict[str, Any], kind: str):
    """主动断开并新建连接（批量失败后降级逐行前调用）。"""
    _close_mysql_conn(conn)
    return _new_mysql_conn(cfg, kind)


def preload_vt_token_store(cfg: Dict[str, Any]) -> int:
    """一次性将源库 vt_token_cache(status=1) 载入进程内存，批内 O(1) 查 token。"""
    global _vt_global_store
    if not cfg.get("vt_token_enable", True) or not cfg.get("vt_preload", True):
        return 0
    with _vt_preload_lock:
        if _vt_global_store is not None:
            return len(_vt_global_store)
        vt_db = cfg.get("vt_token_db", "ng_loan_market")
        sql = f"""
            SELECT vt_type, raw_value, token
            FROM `{vt_db}`.vt_token_cache
            WHERE status = 1
              AND token IS NOT NULL AND token <> ''
        """
        t0 = time.perf_counter()
        mig_log(f"== VT preload start db={vt_db} ==")
        progress_fn = cfg.get("progress_log_fn")
        if progress_fn:
            progress_fn("vt preload start (full vt_token_cache, may take a while)...")
        src = connect_source(cfg)
        store: Dict[Tuple[str, str], str] = {}
        try:
            with src.cursor() as cur:
                cur.execute(sql)
                chunk_no = 0
                while True:
                    chunk = cur.fetchmany(50000)
                    if not chunk:
                        break
                    chunk_no += 1
                    for row in chunk:
                        store[(row["vt_type"], row["raw_value"])] = row["token"]
                    if chunk_no == 1 or chunk_no % 10 == 0:
                        el = time.perf_counter() - t0
                        msg = (
                            f"vt preload progress chunks={chunk_no} rows={len(store)} "
                            f"elapsed={el:.1f}s"
                        )
                        mig_log(f"== {msg} ==")
                        if progress_fn:
                            progress_fn(msg)
        finally:
            _close_mysql_conn(src)
        _vt_global_store = store
        el = time.perf_counter() - t0
        rate = (len(store) / el) if el > 0 else 0.0
        done_msg = (
            f"vt preload done rows={len(store)} elapsed={el:.1f}s ({rate:.0f} rows/s)"
        )
        mig_log(f"== {done_msg} ==")
        if progress_fn:
            progress_fn(done_msg)
        return len(store)


def preload_lup_store(cfg: Dict[str, Any]) -> int:
    """一次性聚合 log_user_password，批内本地查 password（避免每批 GROUP BY）。"""
    global _lup_global_store
    if not cfg.get("lup_preload", True):
        return 0
    with _lup_preload_lock:
        if _lup_global_store is not None:
            return len(_lup_global_store)
        m = "ng_loan_market"
        sql = f"""
            SELECT l1.`appId`, l1.mobile, l1.password
            FROM {m}.log_user_password l1
            INNER JOIN (
                SELECT `appId`, mobile, MAX(id) AS max_id
                FROM {m}.log_user_password
                GROUP BY `appId`, mobile
            ) l2 ON l1.`appId` = l2.`appId` AND l1.mobile = l2.mobile AND l1.id = l2.max_id
        """
        t0 = time.perf_counter()
        src = connect_source(cfg)
        store: Dict[Tuple[Any, str], str] = {}
        try:
            with src.cursor() as cur:
                cur.execute(sql)
                while True:
                    chunk = cur.fetchmany(50000)
                    if not chunk:
                        break
                    for row in chunk:
                        password = row["password"] or ""
                        for key in _lup_keys_for_pair(row["appId"], row["mobile"]):
                            store[key] = password
        finally:
            _close_mysql_conn(src)
        _lup_global_store = store
        el = time.perf_counter() - t0
        rate = (len(store) / el) if el > 0 else 0.0
        mig_log(
            f"== LUP preload done rows={len(store)} elapsed={el:.1f}s "
            f"({rate:.0f} rows/s) =="
        )
        return len(store)


def _effective_workers(requested: int, cfg: Dict[str, Any]) -> int:
    cap = max(1, cfg.get("max_worker_slots", 64))
    return max(1, min(requested, cap))


def _application_worker_ranges_by_id(
    lo_start: int, max_id: int, workers: int,
) -> List[Tuple[int, int, int]]:
    """按 application.id 等分（旧逻辑）。"""
    if workers <= 1 or max_id <= lo_start:
        return [(lo_start, max_id, 0)]
    span = (max_id - lo_start + workers - 1) // workers
    ranges: List[Tuple[int, int, int]] = []
    for i in range(workers):
        a = lo_start + i * span
        b = min(lo_start + (i + 1) * span, max_id)
        if a < b:
            ranges.append((a, b, i))
    return ranges


def _application_worker_ranges_by_count(
    cfg: Dict[str, Any], lo_start: int, max_id: int, workers: int,
) -> List[Tuple[int, int, int]]:
    """按有效订单行数均分 id 段，避免早期 id 段订单过密导致 worker 失衡。"""
    if workers <= 1 or max_id <= lo_start:
        return [(lo_start, max_id, 0)]

    src = connect_source(cfg)
    try:
        with src.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*) AS c
                FROM ng_loan_market.application
                WHERE applicationNo IS NOT NULL AND applicationNo <> ''
                  AND id > %s AND id <= %s
                """,
                (lo_start, max_id),
            )
            total = int(cur.fetchone()["c"] or 0)
        if total <= 0:
            return []
        if total < workers:
            mig_log(
                f"== application 订单数={total} < workers={workers}，降为单 worker =="
            )
            return [(lo_start, max_id, 0)]

        split_targets = [(total * i) // workers for i in range(1, workers)]
        split_ids: List[int] = []
        count = 0
        next_idx = 0
        sql = """
            SELECT id
            FROM ng_loan_market.application
            WHERE applicationNo IS NOT NULL AND applicationNo <> ''
              AND id > %s AND id <= %s
            ORDER BY id ASC
        """
        with src.cursor() as cur:
            cur.execute(sql, (lo_start, max_id))
            while True:
                chunk = cur.fetchmany(50000)
                if not chunk:
                    break
                for row in chunk:
                    count += 1
                    while next_idx < len(split_targets) and count >= split_targets[next_idx]:
                        split_ids.append(int(row["id"]))
                        next_idx += 1
                if next_idx >= len(split_targets):
                    break
    finally:
        _close_mysql_conn(src)

    bounds = [lo_start] + split_ids + [max_id]
    ranges: List[Tuple[int, int, int]] = []
    for i in range(len(bounds) - 1):
        a, b = bounds[i], bounds[i + 1]
        if a < b:
            ranges.append((a, b, i))
    return ranges


def _plan_application_worker_ranges(
    cfg: Dict[str, Any], lo_start: int, max_id: int, workers: int,
) -> List[Tuple[int, int, int]]:
    mode = (cfg.get("app_worker_balance") or "count").lower()
    if mode in ("id", "equal_id", "0", "false"):
        ranges = _application_worker_ranges_by_id(lo_start, max_id, workers)
        mig_log(f"== application worker 分段模式=id 等分 id 段 {ranges} ==")
        return ranges

    t0 = time.perf_counter()
    ranges = _application_worker_ranges_by_count(cfg, lo_start, max_id, workers)
    el = time.perf_counter() - t0
    mig_log(
        f"== application worker 分段模式=count 订单量均分 "
        f"elapsed={el:.1f}s 段 {ranges} =="
    )
    return ranges


def _with_source_conn(cfg: Dict[str, Any], fn, *args, **kwargs):
    conn = connect_source(cfg)
    try:
        return fn(conn, *args, **kwargs)
    finally:
        _close_mysql_conn(conn)


def _run_source_lookup_task(
    cfg: Dict[str, Any],
    step: str,
    perf_table: str,
    fetch_fn,
) -> Tuple[str, str, List[dict], float]:
    t0 = time.perf_counter()
    rows = _with_source_conn(cfg, fetch_fn)
    el = time.perf_counter() - t0
    return step, perf_table, rows, el


def exec_sql(conn, sql: str, params: Optional[tuple] = None) -> int:
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.rowcount


def exec_many_statements(conn, sql_block: str) -> None:
    """按分号拆分执行（跳过空语句）。"""
    buf: List[str] = []
    for line in sql_block.splitlines():
        stripped = line.strip()
        if stripped.startswith("--"):
            continue
        buf.append(line)
    text = "\n".join(buf)
    parts = [p.strip() for p in text.split(";") if p.strip()]
    with conn.cursor() as cur:
        for part in parts:
            cur.execute(part)


_progress_lock = threading.Lock()


def _progress_worker_key(base: str, worker_id: int) -> str:
    return f"{base}.W{worker_id}"


def _load_worker_resume_lo(
    prog: Dict[str, str],
    base_key: str,
    worker_id: int,
    lo_start: int,
    prefix: str,
) -> int:
    """读取 per-worker 进度；兼容旧版单键 user_lo（仅 worker 0）。"""
    lo = lo_start
    wkey = _progress_worker_key(base_key, worker_id)
    saved_raw = prog.get(wkey)
    if saved_raw is None and worker_id == 0:
        saved_raw = prog.get(base_key)
    if saved_raw is not None:
        saved = int(saved_raw)
        if saved > lo:
            lo = saved
            mig_log(f"{prefix} resume from {wkey}={lo}")
    return lo


def _is_phase_done(prog: Dict[str, str], key: str) -> bool:
    return prog.get(key, "").strip() in ("1", "true", "yes")


def load_progress(path: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    p = Path(path)
    if not p.exists():
        return out
    for line in p.read_text(encoding="utf-8").splitlines():
        if "=" in line:
            k, v = line.split("=", 1)
            out[k.strip()] = v.strip()
    return out


def save_progress(path: str, key: str, val: str) -> None:
    if not path:
        return
    with _progress_lock:
        data = load_progress(path)
        data[key] = val
        lines = [f"{k}={v}" for k, v in sorted(data.items())]
        Path(path).write_text("\n".join(lines) + "\n", encoding="utf-8")


def _maybe_save_progress(
    cfg: Dict[str, Any], key: str, val: str, batch_num: int,
) -> None:
    """按 PROGRESS_SAVE_EVERY 节流写进度，降低磁盘 I/O。"""
    every = max(1, int(cfg.get("progress_save_every", 1)))
    path = cfg.get("progress_file") or ""
    if not path:
        return
    if batch_num % every == 0:
        save_progress(path, key, val)


_log_file: Optional[str] = None
_skip_log_file: Optional[str] = None
_log_lock = threading.Lock()


def init_log(cfg: Dict[str, Any]) -> None:
    global _log_file, _skip_log_file
    _log_file = cfg.get("log_file") or None
    if _log_file:
        Path(_log_file).parent.mkdir(parents=True, exist_ok=True)
    explicit = (cfg.get("skip_log_file") or "").strip()
    if explicit:
        _skip_log_file = explicit
    elif _log_file:
        p = Path(_log_file)
        _skip_log_file = str(p.with_name(p.stem + ".skip.log"))
    else:
        _skip_log_file = "/tmp/ng_mig_skip.log"
    Path(_skip_log_file).parent.mkdir(parents=True, exist_ok=True)


def _skip_row_payload(data: Any) -> str:
    """将跳过/失败行序列化为 JSON 字符串写入 skip 日志。"""
    if data is None:
        return ""
    if isinstance(data, dict):
        return _json_dumps(data)
    return str(data)


def skip_log(kind: str, **fields: Any) -> None:
    """跳过/失败明细写入独立日志（VT 未命中、单行 insert/upsert 失败等）。"""
    if not _skip_log_file:
        return
    parts = [time.strftime("%Y-%m-%d %H:%M:%S"), kind]
    for key in sorted(fields.keys()):
        val = fields[key]
        if val is None:
            continue
        s = str(val).replace("\t", " ").replace("\n", " ")
        max_len = 12000 if key in ("data", "row") else 2000
        if len(s) > max_len:
            s = s[:max_len] + "..."
        parts.append(f"{key}={s}")
    line = "\t".join(parts)
    with _log_lock:
        with open(_skip_log_file, "a", encoding="utf-8") as f:
            f.write(line + "\n")


def log_skip_row(
    entity: str,
    reason: str,
    row: Any,
    **fields: Any,
) -> None:
    """整行跳过：写入 entity/reason 及完整源数据。"""
    skip_log(
        "row_skip",
        entity=entity,
        reason=reason,
        data=_skip_row_payload(row),
        **fields,
    )


def mig_log(msg: str) -> None:
    line = f"{time.strftime('%Y-%m-%d %H:%M:%S')} {msg}"
    with _log_lock:
        try:
            print(line, flush=True)
        except UnicodeEncodeError:
            sys.stdout.buffer.write((line + "\n").encode("utf-8", errors="replace"))
            sys.stdout.buffer.flush()
        if _log_file:
            with open(_log_file, "a", encoding="utf-8") as f:
                f.write(line + "\n")


def _fmt_speed(rows: int, elapsed: float) -> str:
    if elapsed <= 0:
        return "n/a"
    if rows > 0:
        rps = rows / elapsed
        ms = elapsed * 1000 / rows
        return f"{rps:.1f} rows/s, {ms:.1f} ms/row"
    return f"{elapsed:.2f}s"


def log_step(prefix: str, step: str, rows: int, elapsed: float, extra: str = "") -> None:
    speed = _fmt_speed(rows, elapsed)
    suffix = f" | {extra}" if extra else ""
    mig_log(f"{prefix} {step}: rows={rows} elapsed={elapsed:.2f}s ({speed}){suffix}")


def log_batch_summary(prefix: str, batch_elapsed: float, steps: List[Tuple[str, int, float]]) -> None:
    """steps: [(step_name, rows, elapsed), ...]"""
    total_rows = sum(r for _, r, _ in steps)
    parts = []
    for name, rows, el in steps:
        pct = (el / batch_elapsed * 100) if batch_elapsed > 0 else 0
        parts.append(f"{name}={el:.1f}s/{rows}r({pct:.0f}%)")
    mig_log(
        f"{prefix} BATCH_DONE total={batch_elapsed:.2f}s "
        f"throughput={_fmt_speed(total_rows, batch_elapsed)} | " + " ".join(parts)
    )


class CumulativeStats:
    def __init__(self, label: str, prefix: str, every: int) -> None:
        self.label = label
        self.prefix = prefix
        self.every = max(1, every)
        self.batch_count = 0
        self.total_elapsed = 0.0
        self.step_stats: Dict[str, Dict[str, float]] = {}

    def add_batch(self, batch_elapsed: float, steps: List[Tuple[str, int, float]]) -> None:
        self.batch_count += 1
        self.total_elapsed += batch_elapsed
        for name, rows, el in steps:
            s = self.step_stats.setdefault(name, {"rows": 0, "elapsed": 0.0})
            s["rows"] += rows
            s["elapsed"] += el
        if self.batch_count % self.every == 0:
            self._print_summary()

    def _print_summary(self) -> None:
        total_rows = sum(s["rows"] for s in self.step_stats.values())
        mig_log(
            f"{self.prefix} CUMULATIVE [{self.label}] batches={self.batch_count} "
            f"total={self.total_elapsed:.1f}s avg_batch={self.total_elapsed / self.batch_count:.2f}s "
            f"overall={_fmt_speed(total_rows, self.total_elapsed)}"
        )
        for name, s in sorted(self.step_stats.items(), key=lambda x: -x[1]["elapsed"]):
            mig_log(
                f"{self.prefix}   - {name}: rows={int(s['rows'])} elapsed={s['elapsed']:.1f}s "
                f"({_fmt_speed(int(s['rows']), s['elapsed'])})"
            )

    def finish(self) -> None:
        if self.batch_count % self.every != 0:
            self._print_summary()


class GlobalPerfStats:
    """全进程按表/阶段汇总耗时，便于从日志定位瓶颈。"""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._steps: Dict[str, Dict[str, float]] = {}

    def record(self, table: str, phase: str, rows: int, elapsed: float) -> None:
        key = f"{table}|{phase}"
        with self._lock:
            s = self._steps.setdefault(key, {"table": table, "phase": phase, "rows": 0, "elapsed": 0.0})
            s["rows"] += rows
            s["elapsed"] += elapsed

    def log_summary(self, label: str) -> None:
        with self._lock:
            items = list(self._steps.values())
        if not items:
            return
        total_el = sum(s["elapsed"] for s in items)
        total_rows = sum(int(s["rows"]) for s in items)
        mig_log(
            f"== PERF SUMMARY [{label}] steps={len(items)} "
            f"total_elapsed={total_el:.1f}s total_rows={total_rows} "
            f"overall={_fmt_speed(total_rows, total_el)} =="
        )
        for s in sorted(items, key=lambda x: -x["elapsed"]):
            rows = int(s["rows"])
            el = s["elapsed"]
            pct = (el / total_el * 100) if total_el > 0 else 0
            mig_log(
                f"  PERF table={s['table']} phase={s['phase']}: "
                f"rows={rows} elapsed={el:.1f}s ({pct:.1f}%) {_fmt_speed(rows, el)}"
            )


_worker_stats: Dict[int, CumulativeStats] = {}
_app_stats: Optional[CumulativeStats] = None
_global_perf: Optional[GlobalPerfStats] = None


def get_global_perf() -> GlobalPerfStats:
    global _global_perf
    if _global_perf is None:
        _global_perf = GlobalPerfStats()
    return _global_perf


def log_perf(
    prefix: str, table: str, phase: str, rows: int, elapsed: float, extra: str = "",
) -> None:
    get_global_perf().record(table, phase, rows, elapsed)
    speed = _fmt_speed(rows, elapsed)
    suffix = f" | {extra}" if extra else ""
    mig_log(
        f"{prefix} PERF table={table} phase={phase} rows={rows} "
        f"elapsed={elapsed:.2f}s ({speed}){suffix}"
    )


def _get_worker_stats(cfg: Dict[str, Any], worker_id: int) -> CumulativeStats:
    if worker_id not in _worker_stats:
        _worker_stats[worker_id] = CumulativeStats(
            "user", f"[W{worker_id}]", cfg["log_every"]
        )
    return _worker_stats[worker_id]


USER_INSERT_COLS = [
    "user_id", "app_id", "group_user_id", "info_user_id", "mobile", "password",
    "closed_time",
    "reg_device_uuid", "reg_time", "test_flag",
    "utm_source", "utm_medium", "utm_campaign", "utm_content", "utm_term",
    "campaign_id", "ad_group_id", "advertiser_id",
]

def _utm_fields_from_dac(dac: Optional[dict]) -> Dict[str, Any]:
    """与旧 SQL_USER_UTM_UPDATE 逻辑一致，在写入 user 时直接赋值。"""
    empty = {
        "utm_source": None,
        "utm_medium": None,
        "utm_campaign": None,
        "utm_content": None,
        "utm_term": None,
        "campaign_id": None,
        "ad_group_id": None,
        "advertiser_id": None,
    }
    if not dac:
        return empty
    channel = dac.get("channel") or ""
    ch_upper = str(channel).upper()
    utm_source = None
    if ch_upper == "ORGANIC":
        utm_source = "organic"
    elif ch_upper == "FB":
        utm_source = "facebook"
    elif ch_upper == "TT":
        utm_source = "tiktok"
    elif ch_upper == "GG":
        utm_source = "google"
    campaign_id = None
    ad_group_id = None
    if channel == "GG":
        campaign_id = dac.get("google_ads_campaign_id")
        ad_group_id = dac.get("google_ads_adgroup_id")
    elif channel == "FB":
        campaign_id = dac.get("fb_install_referrer_campaign_id")
        ad_group_id = dac.get("fb_install_referrer_campaign_group_id")
    return {
        **empty,
        "utm_source": utm_source,
        "campaign_id": campaign_id,
        "ad_group_id": ad_group_id,
    }


def _prepare_user_insert_rows(
    rows: List[dict],
    lookups: Optional[Dict[str, Any]] = None,
) -> None:
    dac_by_device = (lookups or {}).get("dac_by_device", {})
    lup_by_key = (lookups or {}).get("lup_by_key", {})
    for row in rows:
        row["group_user_id"] = row["user_id"]
        row["info_user_id"] = row["user_id"]
        device_id = _user_reg_device_id(row)
        dac = dac_by_device.get(device_id) if device_id else None
        row.update(_utm_fields_from_dac(dac))
        lup = _lookup_lup(lup_by_key, row) if lup_by_key else None
        row["password"] = (lup.get("password") if lup else None) or ""


_UD_VARCHAR_LIMITS = {
    "bvn": 64,
    "firstName": 64,
    "middleName": 64,
    "lastName": 64,
    "email": 128,
    "birthday": 32,
    "addressState": 64,
    "addressDistrict": 64,
    "address": 255,
    "profession": 64,
    "bankCode": 64,
    "bankAccount": 64,
}


def _clip_varchar_fields(rows: List[dict], limits: Dict[str, int]) -> None:
    for row in rows:
        for key, max_len in limits.items():
            val = row.get(key)
            if isinstance(val, str) and len(val) > max_len:
                row[key] = val[:max_len]


def drop_legacy_staging_tables(cfg: Dict[str, Any], log_banner: bool = True) -> None:
    """删除目标库全部 dt_mig_* 遗留物化表（不碰正式表）。"""
    tgt = connect_target(cfg)
    try:
        with tgt.cursor() as cur:
            cur.execute("SHOW TABLES LIKE 'dt\\_mig\\_%'")
            tables = [list(r.values())[0] for r in cur.fetchall()]
        if not tables:
            if log_banner:
                mig_log("== 目标库无 dt_mig_* 表 ==")
            return
        parts = [f"DROP TABLE IF EXISTS `{t}`" for t in sorted(tables)]
        exec_many_statements(tgt, ";\n".join(parts) + ";")
        tgt.commit()
        if log_banner:
            mig_log(f"== 已删除目标库 {len(tables)} 张 dt_mig_* 物化表 ==")
    finally:
        _close_mysql_conn(tgt)


def _clear_progress_file(cfg: Dict[str, Any]) -> None:
    path = cfg.get("progress_file") or ""
    if not path:
        return
    p = Path(path)
    if p.exists():
        p.unlink()
    mig_log(f"== [脚本内] 已重置进度文件 {path} ==")


def _prepare_run_at_start(cfg: Dict[str, Any], command: str) -> None:
    """full/user/user_info 启动：可选清理遗留 dt_mig_* 并重置进度。"""
    if command not in ("full", "user", "user_info"):
        return
    if cfg.get("_run_prepared"):
        return
    if cfg.get("drop_mat_on_start", True):
        mig_log("== DROP_MAT_ON_START=1：清理遗留 dt_mig_* 并重置进度 ==")
        t0 = time.perf_counter()
        drop_legacy_staging_tables(cfg, log_banner=False)
        mig_log(
            f"== 遗留物化表已删除 elapsed={time.perf_counter() - t0:.2f}s =="
        )
        _clear_progress_file(cfg)
    else:
        mig_log("== DROP_MAT_ON_START=0：保留进度，跳过物化表清理 ==")
    cfg["_run_prepared"] = True


def _session_opts(conn) -> None:
    exec_sql(conn, "SET SESSION unique_checks = 0")
    exec_sql(conn, "SET SESSION foreign_key_checks = 0")
    try:
        exec_sql(conn, "SET SESSION sql_log_bin = 0")
    except Exception:
        pass


class VtTokenResolver:
    """从源库 vt_token_cache 按明文查 token；未命中则不写入（不回退明文）。"""

    VT_MOBILE = "mobile"
    VT_EMERGENCY_CONTACT = "emergency_contact"
    VT_GAID = "gaid_idfa"
    VT_BANK = "bank_account"
    VT_ID_NUMBER = "id_number"
    VT_ID2 = "id2"

    def __init__(
        self,
        conn,
        enabled: bool = True,
        chunk: int = 2000,
        vt_db: str = "ng_loan_market",
        global_store: Optional[Dict[Tuple[str, str], str]] = None,
    ) -> None:
        self.conn = conn
        self.vt_db = (vt_db or "ng_loan_market").strip()
        self.enabled = enabled
        self.chunk = max(1, chunk)
        self._global_store = global_store if global_store is not None else _vt_global_store
        self._pending: Dict[str, set] = {}
        self._map: Dict[Tuple[str, str], str] = {}
        self.hit: Dict[str, int] = {}
        self.miss: Dict[str, int] = {}

    def register(self, vt_type: str, raw_value: Any) -> None:
        if not self.enabled or raw_value is None:
            return
        s = str(raw_value).strip()
        if not s:
            return
        self._pending.setdefault(vt_type, set()).add(s)

    def register_emergency_contacts(self, val: Any) -> None:
        parsed = _parse_emergency_contact(val)
        if not isinstance(parsed, list):
            return
        for item in parsed:
            mobile = None
            if isinstance(item, (list, tuple)) and len(item) >= 3:
                mobile = item[2]
            elif isinstance(item, dict):
                mobile = (
                    item.get("mobile") or item.get("contactNumber")
                    or item.get("contact_number")
                )
            if mobile is not None:
                _register_emergency_contact_mobile(self, mobile)

    def prefetch(self) -> None:
        if not self.enabled:
            return
        if self._global_store is not None:
            self._pending.clear()
            return
        for vt_type, values in self._pending.items():
            uniq = list(values)
            for i in range(0, len(uniq), self.chunk):
                part = uniq[i:i + self.chunk]
                ph = ",".join(["%s"] * len(part))
                sql = f"""
                    SELECT raw_value, token
                    FROM `{self.vt_db}`.vt_token_cache
                    WHERE vt_type = %s
                      AND status = 1
                      AND token IS NOT NULL AND token <> ''
                      AND raw_value IN ({ph})
                """
                with self.conn.cursor() as cur:
                    cur.execute(sql, [vt_type, *part])
                    for row in cur.fetchall():
                        self._map[(vt_type, row["raw_value"])] = row["token"]
        self._pending.clear()

    def resolve_token(
        self,
        vt_type: str,
        raw_value: Any,
        context: str = "",
        log_miss: bool = True,
        row_data: Any = None,
    ) -> Optional[str]:
        """VT 开启：命中返回 token，未命中返回 None。VT 关闭：返回明文。"""
        if raw_value is None:
            return None
        s = str(raw_value).strip()
        if not s:
            return None
        if not self.enabled:
            return s
        store = self._global_store if self._global_store is not None else self._map
        token = store.get((vt_type, s))
        if token:
            self.hit[vt_type] = self.hit.get(vt_type, 0) + 1
            return token
        self.miss[vt_type] = self.miss.get(vt_type, 0) + 1
        if log_miss:
            payload = _skip_row_payload(row_data) if row_data is not None else ""
            skip_log(
                "vt_miss",
                vt_type=vt_type,
                raw=s,
                context=context,
                data=payload or None,
            )
        return None

    def summary(self) -> str:
        if not self.enabled:
            return "vt=off"
        if self._global_store is not None:
            return "vt=preload"
        keys = sorted(set(list(self.hit.keys()) + list(self.miss.keys())))
        if not keys:
            return "vt=ok"
        parts = [f"{k}:hit={self.hit.get(k, 0)} miss={self.miss.get(k, 0)}" for k in keys]
        return "vt " + " ".join(parts)


def _register_user_batch_vt(vt: VtTokenResolver, rows_user: List[dict], lookups: Dict[str, Any]) -> None:
    for row in rows_user:
        vt.register(VtTokenResolver.VT_MOBILE, row.get("mobile"))
    for ud in lookups.get("ud_by_user", {}).values():
        vt.register(VtTokenResolver.VT_ID_NUMBER, ud.get("bvn"))
        vt.register(VtTokenResolver.VT_BANK, ud.get("bankAccount"))
        vt.register_emergency_contacts(ud.get("emergencyContact"))


def _register_app_batch_vt(
    vt: VtTokenResolver,
    raw_rows: List[dict],
    bvn_map: Dict[int, str],
) -> None:
    for row in raw_rows:
        vt.register(VtTokenResolver.VT_MOBILE, row.get("mobile"))
        vt.register(VtTokenResolver.VT_GAID, row.get("gaid_idfa"))
        vt.register(VtTokenResolver.VT_BANK, row.get("bank_account_number"))
        vt.register(VtTokenResolver.VT_ID2, row.get("id2"))
    for bvn in bvn_map.values():
        vt.register(VtTokenResolver.VT_ID_NUMBER, bvn)


USER_PRODUCT_COLS = [
    "group_user_id", "product_id", "schemes", "is_open",
    "credit_amount", "unpaid_amount", "locked_amount", "available_amount",
]

USER_INFO_COLS = [
    "user_id", "id_number", "full_name", "password", "live_image", "id_card", "info",
]

# ng 生产库 user_bankcard 已增加 id，迁移侧统一写入雪花 ID。
USER_BANKCARD_COLS = ["id", "group_user_id", "bank_code", "bank_account_number", "is_default"]


def _quote_col(name: str) -> str:
    return f"`{name}`"


def _is_deadlock_error(exc: BaseException) -> bool:
    return isinstance(exc, pymysql.err.OperationalError) and bool(
        exc.args and exc.args[0] == 1213
    )


# 连接/通信类瞬时错误（含 error=0 空消息）
_INSERT_TRANSIENT_ERRNO = frozenset({0, 1159, 1205, 2006, 2013, 2014, 2055})


def _is_transient_insert_error(exc: BaseException) -> bool:
    if _is_deadlock_error(exc):
        return False
    if isinstance(exc, pymysql.err.InterfaceError):
        return True
    if isinstance(exc, pymysql.err.OperationalError):
        if not exc.args:
            return True
        code = exc.args[0]
        if code in _INSERT_TRANSIENT_ERRNO:
            return True
    if isinstance(exc, pymysql.err.Error) and exc.args and exc.args[0] == 0:
        msg = exc.args[1] if len(exc.args) > 1 else ""
        if not str(msg).strip():
            return True
    return False


def _insert_row_retries(cfg: Dict[str, Any]) -> int:
    return max(1, int(cfg.get("insert_row_retries", 3)))


def _should_skip_row_on_error(exc: BaseException) -> bool:
    """单行失败可跳过；死锁交给外层批次重试；瞬时错误先重试。"""
    if _is_deadlock_error(exc) or _is_transient_insert_error(exc):
        return False
    return isinstance(exc, pymysql.err.Error)


def _row_insert_params(columns: List[str], row: dict) -> List[Any]:
    return [row.get(c) for c in columns]


def _execute_rows_with_retry(
    conn,
    cfg: Dict[str, Any],
    conn_kind: str,
    table: str,
    columns: List[str],
    rows: List[dict],
    build_sql,
) -> Tuple[Any, int]:
    """逐行写入；仅在重试仍失败时写 skip 日志（中间失败不打印）。"""
    affected = 0
    max_retries = _insert_row_retries(cfg)
    for row in rows:
        sql, params = build_sql(row)
        row_ok = False
        last_exc: Optional[BaseException] = None
        for attempt in range(max_retries):
            try:
                conn = _ensure_mysql_conn(conn, cfg, conn_kind)
                with conn.cursor() as cur:
                    cur.execute(sql, params)
                    affected += cur.rowcount
                row_ok = True
                break
            except pymysql.err.Error as exc:
                if _is_deadlock_error(exc):
                    raise
                last_exc = exc
                if (
                    attempt < max_retries - 1
                    and _is_transient_insert_error(exc)
                ):
                    conn = _reconnect_mysql(conn, cfg, conn_kind)
                    delay = 0.1 * (2 ** attempt) + random.uniform(0, 0.05)
                    time.sleep(delay)
                    continue
                break
        if not row_ok and last_exc is not None:
            detail = str(last_exc)
            if _is_transient_insert_error(last_exc):
                detail = f"retries exhausted ({max_retries}): {last_exc}"
            skip_log(
                "insert_fail",
                table=table,
                error=last_exc.args[0] if last_exc.args else type(last_exc).__name__,
                detail=detail,
                data=_skip_row_payload({c: row.get(c) for c in columns}),
            )
    return conn, affected


def _bulk_insert_rows(
    conn,
    cfg: Dict[str, Any],
    conn_kind: str,
    table: str,
    columns: List[str],
    rows: List[dict],
    batch_size: int,
    ignore: bool = False,
) -> Tuple[Any, int]:
    if not rows:
        return conn, 0
    ignore_sql = "IGNORE " if ignore else ""
    cols_sql = ", ".join(_quote_col(c) for c in columns)
    one = "(" + ",".join(["%s"] * len(columns)) + ")"
    insert_prefix = f"INSERT {ignore_sql}INTO `{table}` ({cols_sql}) VALUES "
    affected = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        if len(batch) == 1:
            conn, n = _execute_rows_with_retry(
                conn, cfg, conn_kind, table, columns, batch,
                lambda r: (insert_prefix + one, _row_insert_params(columns, r)),
            )
            affected += n
            continue
        values_sql = ",".join([one] * len(batch))
        sql = insert_prefix + values_sql
        params: List[Any] = []
        for row in batch:
            params.extend(_row_insert_params(columns, row))
        batch_ok = False
        last_exc: Optional[BaseException] = None
        for attempt in range(2):
            try:
                conn = _ensure_mysql_conn(conn, cfg, conn_kind)
                with conn.cursor() as cur:
                    cur.execute(sql, params)
                    affected += cur.rowcount
                batch_ok = True
                break
            except pymysql.err.Error as exc:
                if _is_deadlock_error(exc):
                    raise
                last_exc = exc
                if attempt == 0 and _is_transient_insert_error(exc):
                    conn = _reconnect_mysql(conn, cfg, conn_kind)
                    time.sleep(0.1 + random.uniform(0, 0.05))
                    continue
                break
        if batch_ok:
            continue
        if last_exc is not None and not _should_skip_row_on_error(last_exc):
            raise last_exc
        conn = _reconnect_mysql(conn, cfg, conn_kind)
        conn, n = _execute_rows_with_retry(
            conn, cfg, conn_kind, table, columns, batch,
            lambda r: (insert_prefix + one, _row_insert_params(columns, r)),
        )
        affected += n
    return conn, affected


def _bulk_upsert_rows(
    conn,
    cfg: Dict[str, Any],
    conn_kind: str,
    table: str,
    columns: List[str],
    rows: List[dict],
    batch_size: int,
    update_cols: List[str],
) -> Tuple[Any, int]:
    """INSERT ... ON DUPLICATE KEY UPDATE（按源序写入时后到的行覆盖 event_time）。"""
    if not rows or not update_cols:
        return conn, 0
    cols_sql = ", ".join(_quote_col(c) for c in columns)
    one = "(" + ",".join(["%s"] * len(columns)) + ")"
    upd_sql = ", ".join(
        f"{_quote_col(c)}=VALUES({_quote_col(c)})" for c in update_cols
    )
    insert_prefix = f"INSERT INTO `{table}` ({cols_sql}) VALUES "
    upsert_suffix = f" ON DUPLICATE KEY UPDATE {upd_sql}"
    affected = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        if len(batch) == 1:
            conn, n = _execute_rows_with_retry(
                conn, cfg, conn_kind, table, columns, batch,
                lambda r: (
                    insert_prefix + one + upsert_suffix,
                    _row_insert_params(columns, r),
                ),
            )
            affected += n
            continue
        values_sql = ",".join([one] * len(batch))
        sql = insert_prefix + values_sql + upsert_suffix
        params: List[Any] = []
        for row in batch:
            params.extend(_row_insert_params(columns, row))
        batch_ok = False
        last_exc: Optional[BaseException] = None
        for attempt in range(2):
            try:
                conn = _ensure_mysql_conn(conn, cfg, conn_kind)
                with conn.cursor() as cur:
                    cur.execute(sql, params)
                    affected += cur.rowcount
                batch_ok = True
                break
            except pymysql.err.Error as exc:
                if _is_deadlock_error(exc):
                    raise
                last_exc = exc
                if attempt == 0 and _is_transient_insert_error(exc):
                    conn = _reconnect_mysql(conn, cfg, conn_kind)
                    time.sleep(0.1 + random.uniform(0, 0.05))
                    continue
                break
        if batch_ok:
            continue
        if last_exc is not None and not _should_skip_row_on_error(last_exc):
            raise last_exc
        conn = _reconnect_mysql(conn, cfg, conn_kind)
        conn, n = _execute_rows_with_retry(
            conn, cfg, conn_kind, table, columns, batch,
            lambda r: (
                insert_prefix + one + upsert_suffix,
                _row_insert_params(columns, r),
            ),
        )
        affected += n
    return conn, affected


def _parse_emergency_contact(val: Any) -> Any:
    if val is None:
        return None
    if isinstance(val, (list, dict)):
        return val
    s = str(val).strip()
    if not s:
        return None
    if s[0] in "[{":
        try:
            return json.loads(s)
        except (json.JSONDecodeError, TypeError, ValueError):
            return s
    return s


def _mobile_vt_lookup_candidates(raw: str) -> List[str]:
    """紧急联系人手机号 VT 查询候选（源库多为本地号，字典多为 +234）。"""
    s = str(raw).strip()
    if not s:
        return []
    cands = [s]
    if s.startswith("+234"):
        cands.append(s[4:])
    elif s.startswith("234"):
        cands.append("+" + s)
        cands.append(s[3:])
    elif s.startswith("0"):
        cands.append("+234" + s[1:])
        cands.append(s[1:])
    else:
        cands.append("+234" + s)
    seen: set = set()
    out: List[str] = []
    for c in cands:
        if c and c not in seen:
            seen.add(c)
            out.append(c)
    return out


def _register_emergency_contact_mobile(vt: "VtTokenResolver", raw_value: Any) -> None:
    """紧急联系人：mobile / emergency_contact 两种 vt_type + 多格式候选均注册。"""
    for cand in _mobile_vt_lookup_candidates(str(raw_value).strip() if raw_value else ""):
        vt.register(VtTokenResolver.VT_MOBILE, cand)
        vt.register(VtTokenResolver.VT_EMERGENCY_CONTACT, cand)


def _resolve_contact_mobile(
    vt: Optional["VtTokenResolver"],
    mobile_raw: str,
    context: str = "",
    name: Any = None,
    relation: Any = None,
) -> Optional[str]:
    """VT 开启：先查 mobile，再查 emergency_contact；未命中返回 None。"""
    if not mobile_raw:
        return None
    if not vt:
        return mobile_raw
    name_s = str(name).strip() if name is not None else ""
    vt_types = (VtTokenResolver.VT_MOBILE, VtTokenResolver.VT_EMERGENCY_CONTACT)
    for cand in _mobile_vt_lookup_candidates(mobile_raw):
        for vt_type in vt_types:
            token = vt.resolve_token(
                vt_type, cand, context=context, log_miss=False,
            )
            if token:
                return token
    skip_log(
        "vt_miss",
        vt_type="emergency_contact_mobile",
        raw=mobile_raw,
        context=context,
        data=_skip_row_payload({
            "name": name_s or None,
            "mobile": mobile_raw,
            "relation": relation if relation is not None else None,
        }),
    )
    return None


def _emergency_contact_entry(
    name: Any,
    relation: Any,
    mobile: Any,
    vt: Optional["VtTokenResolver"],
    context: str = "",
) -> dict:
    mobile_raw = str(mobile).strip() if mobile is not None else ""
    name_s = str(name).strip() if name is not None else ""
    return {
        "name": name_s if name_s else None,
        "mobile": _resolve_contact_mobile(
            vt, mobile_raw, context=context, name=name, relation=relation,
        ),
        "relation": relation if relation is not None else None,
    }


def _empty_emergency_contacts() -> List[dict]:
    """无联系人：保留数组与 name/mobile/relation 三个 key，值均为 null。"""
    return [_emergency_contact_entry(None, None, None, None)]


def _format_emergency_contacts(
    val: Any,
    vt: Optional["VtTokenResolver"] = None,
    context: str = "",
) -> List[dict]:
    """源库 emergencyContact 多为 [[name, relation, mobile], ...]，转为对象数组。

    目标结构：[{name, mobile, relation}, ...]；无联系人时仍返回数组，项内 key 值为 null。
    VT 查 mobile 与 emergency_contact 两种 type；未命中时 mobile 为 null（不写明文）。
    """
    parsed = _parse_emergency_contact(val)
    if not isinstance(parsed, list):
        return _empty_emergency_contacts()
    out: List[dict] = []
    for item in parsed:
        if isinstance(item, (list, tuple)) and len(item) >= 3:
            entry = _emergency_contact_entry(
                item[0], item[1], item[2], vt, context=context,
            )
        elif isinstance(item, dict):
            entry = _emergency_contact_entry(
                item.get("name") or item.get("contactName") or item.get("contact_name"),
                (
                    item.get("relation") if item.get("relation") is not None
                    else item.get("contactRelationship")
                    if item.get("contactRelationship") is not None
                    else item.get("contact_relationship")
                ),
                item.get("mobile") or item.get("contactNumber") or item.get("contact_number"),
                vt,
                context=context,
            )
        else:
            continue
        out.append(entry)
    return out if out else _empty_emergency_contacts()


def _mobile_format_variants(mobile: Any) -> List[str]:
    """log_user_password.mobile 与 user.mobile 格式可能不一致，生成多种候选。"""
    if mobile in (None, ""):
        return []
    s = str(mobile).strip()
    if not s:
        return []
    variants = {s}
    if s.startswith("+234") and len(s) > 4:
        rest = s[4:]
        variants.add(rest)
        variants.add("0" + rest)
        variants.add("234" + rest)
    elif s.startswith("234") and len(s) > 3:
        rest = s[3:]
        variants.add("+" + s)
        variants.add("+234" + rest)
        variants.add("0" + rest)
    elif s.startswith("0") and len(s) > 1:
        rest = s[1:]
        variants.add("+234" + rest)
        variants.add("234" + rest)
    return list(variants)


def _lup_keys_for_pair(app_id: Any, mobile: Any) -> List[Tuple[Any, str]]:
    return [(app_id, variant) for variant in _mobile_format_variants(mobile)]


def _lookup_lup(lup_by_key: Dict[Any, dict], user_row: dict) -> Optional[dict]:
    """log_user_password 与 user 按 (appId, mobile) 对齐（VT token 不参与匹配）。"""
    app_id = user_row.get("app_id")
    seen: set = set()
    for mobile in (user_row.get("mobile_raw"), user_row.get("mobile_lookup")):
        for variant in _mobile_format_variants(mobile):
            key = (app_id, variant)
            if key in seen:
                continue
            seen.add(key)
            lup = lup_by_key.get(key)
            if lup:
                return lup
    return None


def _device_id_from_uuid(reg_device_uuid: Any) -> Optional[int]:
    if reg_device_uuid in (None, ""):
        return None
    try:
        device_id = int(reg_device_uuid)
    except (TypeError, ValueError):
        return None
    return device_id if device_id > 0 else None


def _user_reg_device_id(user_row: dict) -> Optional[int]:
    """user.deviceId（数值）；reg_device_uuid 改为 deviceUUID 后不再从中解析 id。"""
    raw = user_row.get("reg_device_id")
    if raw not in (None, "", 0):
        try:
            device_id = int(raw)
            return device_id if device_id > 0 else None
        except (TypeError, ValueError):
            pass
    return _device_id_from_uuid(user_row.get("reg_device_uuid"))


def _null_if_blank(val: Any) -> Any:
    """标量空串视为 JSON null；None 保持 None。"""
    if val is None:
        return None
    if isinstance(val, str):
        s = val.strip()
        return s if s else None
    return val


def _user_full_name(ud: Optional[dict]) -> Optional[str]:
    if not ud:
        return None
    parts = [ud.get("firstName"), ud.get("middleName"), ud.get("lastName")]
    name = " ".join(str(p).strip() for p in parts if p is not None and str(p).strip())
    return name if name else None


def _build_user_info_json(
    user_row: dict,
    ud: Optional[dict],
    uri: Optional[dict],
    channel: Optional[str],
    app_name: Optional[str] = None,
    vt: Optional[VtTokenResolver] = None,
) -> dict:
    """组装 user_info.info JSON：所有 key 固定存在，无值写 null（对齐 ng_migration SQL 结构）。"""
    ud = ud or {}
    uri = uri or {}
    app_id = user_row.get("app_id")
    reg_time = user_row.get("reg_time")
    if reg_time in (None, "", 0):
        reg_time = None
    return {
        "full_name": _user_full_name(ud),
        "email": _null_if_blank(ud.get("email")),
        "birthday": _null_if_blank(ud.get("birthday")),
        "gender": ud.get("gender") if ud.get("gender") is not None else None,
        "id_card": None,
        "live_image": None,
        "face_similarity": None,
        "address": {
            "province": _null_if_blank(ud.get("addressState")),
            "city": _null_if_blank(ud.get("addressDistrict")),
            "district": None,
            "village": None,
            "detail": _null_if_blank(ud.get("address")),
        },
        "company": _null_if_blank(ud.get("company")),
        "education": ud.get("education") if ud.get("education") is not None else None,
        "loan_purpose": None,
        "marital": ud.get("marital") if ud.get("marital") is not None else None,
        "job_type": None,
        "profession": _null_if_blank(ud.get("profession")),
        "religion": None,
        "salary": _null_if_blank(ud.get("salary")),
        "emergency_contacts": _format_emergency_contacts(
            ud.get("emergencyContact"), vt,
            context=f"user_id={user_row['user_id']}",
        ),
        "registration_ip": _null_if_blank(uri.get("ip")),
        "registration_time": reg_time,
        "children_num": (
            ud.get("numberOfChildren")
            if ud.get("numberOfChildren") is not None else None
        ),
        "pay_cycle": ud.get("payCycle") if ud.get("payCycle") is not None else None,
        "salary_day": ud.get("salaryDay") if ud.get("salaryDay") is not None else None,
        "survey": {
            "survey_loan_cnt": None,
            "survey_outstanding_cnt": None,
            "survey_overdue_max_days": None,
            "survey_overdue_6m": None,
            "survey_loan_amt_total": None,
        },
        "app": {
            "name": _null_if_blank(app_name),
            "app_id": str(app_id) if app_id is not None else None,
            "version": None,
        },
        "install_source": _null_if_blank(channel),
        "credit_limit": None,
    }


def _build_user_info_rows(
    rows_user: List[dict],
    lookups: Dict[str, Any],
    vt: Optional[VtTokenResolver] = None,
) -> List[dict]:
    ud_by_user = lookups["ud_by_user"]
    lup_by_key = lookups["lup_by_key"]
    uri_by_user = lookups["uri_by_user"]
    channel_by_device = lookups["channel_by_device"]
    out: List[dict] = []
    for user_row in rows_user:
        user_id = int(user_row["user_id"])
        ud = ud_by_user.get(user_id)
        uri = uri_by_user.get(user_id)
        lup = _lookup_lup(lup_by_key, user_row)
        device_id = _user_reg_device_id(user_row)
        channel = channel_by_device.get(device_id) if device_id else None
        full_name = ""
        if ud:
            parts = [ud.get("firstName"), ud.get("middleName"), ud.get("lastName")]
            full_name = " ".join(
                str(p).strip() for p in parts if p is not None and str(p).strip()
            )
        user_ctx = f"user_id={user_id}"
        bvn_raw = (ud.get("bvn") if ud else None) or ""
        id_number = ""
        if bvn_raw:
            if vt:
                id_number = vt.resolve_token(
                    VtTokenResolver.VT_ID_NUMBER, bvn_raw,
                    context=f"{user_ctx} field=id_number",
                    row_data={"user_id": user_id, "bvn": bvn_raw},
                ) or ""
            else:
                id_number = bvn_raw
        out.append({
            "user_id": user_id,
            "id_number": id_number,
            "full_name": full_name,
            "password": (lup.get("password") if lup else None) or "",
            "live_image": "",
            "id_card": "",
            "info": _json_dumps(_build_user_info_json(
                user_row, ud, uri, channel, user_row.get("app_name"), vt,
            )),
        })
    return out


def _build_bankcard_rows(
    lookups: Dict[str, Any],
    vt: Optional[VtTokenResolver] = None,
    allowed_user_ids: Optional[set] = None,
    cfg: Optional[Dict[str, Any]] = None,
) -> List[dict]:
    rows: List[dict] = []
    snowflake = get_snowflake_generator(cfg)
    for user_id, ud in lookups["ud_by_user"].items():
        if allowed_user_ids is not None and user_id not in allowed_user_ids:
            continue
        bank_code = (ud.get("bankCode") or "").strip()
        bank_account = (ud.get("bankAccount") or "").strip()
        if not bank_code or not bank_account:
            continue
        if vt:
            bank_token = vt.resolve_token(
                VtTokenResolver.VT_BANK, bank_account,
                context=f"user_id={user_id} field=bank",
                row_data={
                    "user_id": user_id,
                    "bank_code": bank_code,
                    "bank_account": bank_account,
                },
            )
            if not bank_token:
                continue
            bank_account_number = bank_token
        else:
            bank_account_number = bank_account
        rows.append({
            "id": snowflake.next_id(),
            "group_user_id": user_id,
            "bank_code": bank_code,
            "bank_account_number": bank_account_number,
            "is_default": 1,
        })
    return rows



def _index_lup_rows(lup_rows: List[dict]) -> Dict[Tuple[Any, str], dict]:
    out: Dict[Tuple[Any, str], dict] = {}
    for row in lup_rows:
        for key in _lup_keys_for_pair(row["appId"], row["mobile"]):
            out[key] = row
    return out


def _make_user_lookups(
    ud_rows: List[dict],
    lup_rows: List[dict],
    uri_rows: List[dict],
    dac_rows: List[dict],
    prod_rows: Optional[List[dict]] = None,
) -> Dict[str, Any]:
    dac_by_device = {
        int(r["deviceId"]): r
        for r in dac_rows
        if r.get("deviceId")
    }
    return {
        "ud_by_user": {int(r["userId"]): r for r in ud_rows},
        "lup_by_key": _index_lup_rows(lup_rows),
        "uri_by_user": {int(r["userId"]): r for r in uri_rows},
        "dac_by_device": dac_by_device,
        "channel_by_device": {
            device_id: r.get("channel")
            for device_id, r in dac_by_device.items()
        },
        "prod_rows": prod_rows or [],
    }


def _extract_user_batch_keys(rows_user: List[dict]) -> Dict[str, Any]:
    user_ids: List[int] = []
    app_mobile_pairs: List[Tuple[Any, str]] = []
    device_ids: List[int] = []
    seen_pairs: set = set()
    seen_devices: set = set()
    for row in rows_user:
        user_ids.append(int(row["user_id"]))
        app_id = row.get("app_id")
        for mobile in (row.get("mobile_raw"), row.get("mobile")):
            if app_id is None or mobile in (None, ""):
                continue
            for variant in _mobile_format_variants(mobile):
                key = (app_id, variant)
                if key not in seen_pairs:
                    seen_pairs.add(key)
                    app_mobile_pairs.append(key)
        device_id = _user_reg_device_id(row)
        if device_id and device_id not in seen_devices:
            seen_devices.add(device_id)
            device_ids.append(device_id)
    return {
        "user_ids": user_ids,
        "app_mobile_pairs": app_mobile_pairs,
        "device_ids": device_ids,
    }


def _select_user_batch_rows(src, lo: int, hi: int) -> List[dict]:
    m = "ng_loan_market"
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
            WHERE u.id > %s AND u.id <= %s
    """
    with src.cursor() as cur:
        cur.execute(sql, (lo, hi))
        return list(cur.fetchall())


def _fetch_user_batch_lookups(
    src,
    cfg: Dict[str, Any],
    rows_user: List[dict],
    lo: int,
    hi: int,
    prefix: str,
) -> Tuple[List[Tuple[str, int, float]], Dict[str, Any]]:
    """按本批 user 主键拉取依赖数据（先 user 再 keyed lookup，LOOKUP_PARALLEL 并行读源库）。"""
    timings: List[Tuple[str, int, float]] = []
    if not rows_user:
        return timings, _make_user_lookups([], [], [], [], [])
    keys = _extract_user_batch_keys(rows_user)
    user_ids = keys["user_ids"]
    app_mobile_pairs = keys["app_mobile_pairs"]
    device_ids = keys["device_ids"]
    lup_chunk = max(50, int(cfg.get("lup_pair_chunk", 400)))
    lookup_tasks = [
        ("mat_ud_sel", "ud_info", lambda c: _select_ud_rows_by_user_ids(c, user_ids)),
        (
            "mat_lup_sel", "lup_latest",
            lambda c: _fetch_lup_by_app_mobile(c, app_mobile_pairs, lup_chunk),
        ),
        ("mat_uri_sel", "uri_latest", lambda c: _fetch_uri_by_user_ids(c, user_ids)),
        ("mat_dac_sel", "dac_latest", lambda c: _fetch_dac_by_device_ids(c, device_ids)),
        ("mat_prod_sel", "user_product", lambda c: _inline_mat_user_product(c, cfg, lo, hi)),
    ]
    parallel = max(1, int(cfg.get("lookup_parallel", 1)))

    if parallel <= 1:
        ud_rows: List[dict] = []
        lup_rows: List[dict] = []
        uri_rows: List[dict] = []
        dac_rows: List[dict] = []
        prod_rows: List[dict] = []
        for step, perf_table, fetch_fn in lookup_tasks:
            t0 = time.perf_counter()
            rows = fetch_fn(src)
            el = time.perf_counter() - t0
            log_perf(prefix, perf_table, "source_select", len(rows), el)
            timings.append((step, len(rows), el))
            if step == "mat_ud_sel":
                ud_rows = rows
            elif step == "mat_lup_sel":
                lup_rows = rows
            elif step == "mat_uri_sel":
                uri_rows = rows
            elif step == "mat_dac_sel":
                dac_rows = rows
            else:
                prod_rows = rows
    else:
        results: Dict[str, List[dict]] = {}
        workers = min(parallel, len(lookup_tasks))
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futs = [
                pool.submit(_run_source_lookup_task, cfg, step, perf_table, fetch_fn)
                for step, perf_table, fetch_fn in lookup_tasks
            ]
            for fut in as_completed(futs):
                step, perf_table, rows, el = fut.result()
                log_perf(prefix, perf_table, "source_select", len(rows), el)
                timings.append((step, len(rows), el))
                results[step] = rows
        ud_rows = results["mat_ud_sel"]
        lup_rows = results["mat_lup_sel"]
        uri_rows = results["mat_uri_sel"]
        dac_rows = results["mat_dac_sel"]
        prod_rows = results["mat_prod_sel"]

    lookups = _make_user_lookups(ud_rows, lup_rows, uri_rows, dac_rows, prod_rows)
    return timings, lookups


def _build_user_product_rows(prod_rows: List[dict]) -> List[dict]:
    out: List[dict] = []
    for row in prod_rows:
        amount = int(row.get("amount") or 0)
        out.append({
            "group_user_id": int(row["userId"]),
            "product_id": str(row["productId"]),
            "schemes": _user_product_schemes_json(amount),
            "is_open": 1,
            "credit_amount": amount,
            "unpaid_amount": amount,
            "locked_amount": 0,
            "available_amount": 0,
        })
    return out


def _fetch_bvn_map_from_source(src, user_ids: List[int]) -> Dict[int, str]:
    if not user_ids:
        return {}
    m = "ng_loan_market"
    uniq = list({int(u) for u in user_ids if u})
    out: Dict[int, str] = {}
    for i in range(0, len(uniq), 5000):
        part = uniq[i:i + 5000]
        ph = ",".join(["%s"] * len(part))
        sql = f"""
            SELECT ud.`userId`, ud.bvn
            FROM {m}.user_data ud
            INNER JOIN (
                SELECT `userId`, MAX(id) AS max_id
                FROM {m}.user_data
                WHERE `userId` IN ({ph})
                GROUP BY `userId`
            ) t ON ud.`userId` = t.`userId` AND ud.id = t.max_id
        """
        with src.cursor() as cur:
            cur.execute(sql, part)
            for row in cur.fetchall():
                out[int(row["userId"])] = row.get("bvn") or ""
    return out


# 对齐 ng 生产库 application（有 coupon_code/bid；无 due_date）
APPLICATION_INSERT_COLS = [
    "application_no", "mobile", "coupon_code", "bid", "app_id", "app_version", "user_id",
    "group_user_id", "sn", "is_test", "is_first_apply", "is_auto_apply", "id_number",
    "gaid_idfa", "device_uuid", "session_id", "bank_code", "bank_account_name",
    "bank_account_number", "product_id", "product_scheme_id", "product_calculator_version",
    "product_scheme_param", "term", "periods", "repayment_method", "repayment_plan",
    "credit_limit", "loan_amount", "principal", "total_amount", "disbursed_amount",
    "created_time", "submited_time", "reviewed_time", "disbursed_time", "last_paid_time",
    "paid_off_time", "lock_expire_time", "status",
]

LOAN_INSERT_COLS = [
    "loan_no", "application_no", "period", "roll_sequence", "start_date", "due_date",
    "due_date_final", "principal", "interest", "admin_fee", "service_fee", "tax_fee",
    "penalty_amount", "reduction_amount", "total_amount", "paid_amount", "paid_time",
    "paid_off_date", "created_time", "status",
]

ID_MAPPING_COLS = ["id", "app_id", "mapping_id", "type", "event_time"]

# Target order no format: {country}{appId(4)}-{sn} / {country}-{sn}-{period}{roll}
COUNTRY_CODE = "ng"


def format_application_no(app_id: Any, suffix: Any) -> str:
    """Target application_no: ng{appId:04d}-{market applicationNo}.

    suffix 为目标单号后半段，一般为源库 market.applicationNo（如 178099546912018102），
    不是 core.application.sn。
    """
    tail = str(suffix or "").strip()
    if not tail:
        return ""
    try:
        app_id_int = int(app_id or 0)
    except (TypeError, ValueError):
        app_id_int = 0
    return f"{COUNTRY_CODE}{app_id_int:04d}-{tail}"


def format_loan_no(sn: Any, period: int = 1, roll_sequence: int = 0) -> str:
    core_sn = str(sn or "").strip()
    if not core_sn:
        return ""
    return f"{COUNTRY_CODE}-{core_sn}-{int(period):02d}{int(roll_sequence):03d}"


def formatted_application_no_from_row(row: dict) -> str:
    market_no = str(row.get("application_no") or row.get("sn") or "").strip()
    return format_application_no(row.get("app_id"), market_no)


def source_app_ids_to_target_application_nos(src, ids: Sequence[int]) -> Dict[int, str]:
    vals = sorted({int(x) for x in ids if x is not None})
    out: Dict[int, str] = {}
    if not vals:
        return out
    m = "ng_loan_market"
    for i in range(0, len(vals), 5000):
        part = vals[i:i + 5000]
        ph = ",".join(["%s"] * len(part))
        with src.cursor() as cur:
            cur.execute(
                f"""
                SELECT a.id, a.`appId` AS app_id, a.applicationNo AS market_no
                FROM {m}.application a
                WHERE a.id IN ({ph})
                  AND a.applicationNo IS NOT NULL AND a.applicationNo <> ''
                """,
                part,
            )
            rows = list(cur.fetchall())
        for row in rows:
            app_no = format_application_no(row.get("app_id"), row.get("market_no"))
            if app_no:
                out[int(row["id"])] = app_no
    return out

# 每条 application 按此顺序展开 mapping 行（与源 id 升序遍历配合）
_ID_MAPPING_TYPE_SPECS: List[Tuple[str, str, Optional[str]]] = [
    ("mobile", "mobile", VtTokenResolver.VT_MOBILE),
    ("gaid_idfa", "gaid_idfa", VtTokenResolver.VT_GAID),
    ("device_uuid", "device_uuid", None),
    ("bank_account", "bank_account_number", VtTokenResolver.VT_BANK),
    ("id_number", "_bvn", VtTokenResolver.VT_ID_NUMBER),
    ("id2", "id2", VtTokenResolver.VT_ID2),
]


def _unix_to_date_str(ts: Any) -> str:
    if ts in (None, "", 0):
        return "1970-01-01"
    try:
        return datetime.utcfromtimestamp(int(ts)).strftime("%Y-%m-%d")
    except (TypeError, ValueError, OSError):
        return "1970-01-01"


def _to_epoch_ms(val: Any) -> int:
    if val in (None, ""):
        return 0
    if isinstance(val, datetime):
        return int(val.timestamp() * 1000)
    if isinstance(val, date):
        return int(datetime.combine(val, datetime.min.time()).timestamp() * 1000)
    try:
        n = int(val)
    except (TypeError, ValueError):
        return 0
    return n * 1000 if n < 10**12 else n


def _map_application_status(src_status: Any) -> Any:
    mapping = {
        0: 1, 1: 1, 2: 1, 4: 1,
        5: 3,
        3: 5, 6: 5,
        8: 7,
        7: 11,
        9: 13, 10: 13,
        12: 15,
        11: 20, 13: 20, 14: 20, 16: 20,
        15: 23,
        17: 27, 18: 27, 19: 27,
    }
    try:
        key = int(src_status)
    except (TypeError, ValueError):
        return src_status
    return mapping.get(key, src_status)


def _application_repayment_plan_json(row: dict) -> str:
    return _json_dumps({
        "roll_sequence": 0,
        "period": 1,
        "principal": int(row.get("disburse_amount") or 0),
        "disbursed_amount": int(row.get("disburse_amount") or 0),
        "interest": 0,
        "admin_fee": int(row.get("core_orig_fee") or 0),
        "service_fee": 0,
        "tax_fee": 0,
        "reduction_amount": 0,
        "total_amount": int(row.get("repayment") or 0),
        "term": int(row.get("term") or 0),
        "start_date": _unix_to_date_str(row.get("apply_date")),
        "due_date": _unix_to_date_str(row.get("due_date")),
        "roll_allowed": 0,
    })


def _select_application_source_rows(src, lo: int, hi: int) -> List[dict]:
    m, c = "ng_loan_market", "ng_loan_core"
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
          AND a.id > %s AND a.id <= %s
        ORDER BY a.id ASC
    """
    with src.cursor() as cur:
        cur.execute(sql, (lo, hi))
        return list(cur.fetchall())


def _fetch_app_lookup_maps(
    cfg: Dict[str, Any],
    src,
    user_ids: List[int],
    sns: List[str],
) -> Tuple[Dict[int, str], Dict[str, int]]:
    parallel = max(1, int(cfg.get("lookup_parallel", 1)))
    if parallel <= 1:
        return (
            _fetch_bvn_map_from_source(src, user_ids),
            _fetch_repay_map(src, sns),
        )
    with ThreadPoolExecutor(max_workers=2) as pool:
        fb = pool.submit(_with_source_conn, cfg, _fetch_bvn_map_from_source, user_ids)
        fr = pool.submit(_with_source_conn, cfg, _fetch_repay_map, sns)
        return fb.result(), fr.result()


def _build_id_mapping_rows(
    raw_rows: List[dict],
    bvn_map: Dict[int, str],
    vt: Optional[VtTokenResolver] = None,
) -> List[dict]:
    """按源 application.id 升序，每单固定 type 顺序展开 id_mapping 行。"""
    out: List[dict] = []
    for row in raw_rows:
        mobile_raw = (row.get("mobile") or "").strip()
        if not mobile_raw:
            continue
        app_no = row.get("application_no") or ""
        app_ctx = f"application_no={app_no} app_id={row.get('app_id')}"
        if vt:
            anchor = vt.resolve_token(
                VtTokenResolver.VT_MOBILE, mobile_raw,
                context=f"{app_ctx} field=anchor_mobile",
                row_data=row,
            )
            if not anchor:
                continue
        else:
            anchor = mobile_raw
        app_id = int(row["app_id"])
        user_id = int(row["user_id"])
        event_time = int(row.get("event_time") or 0)
        bvn_raw = (bvn_map.get(user_id) or "").strip()
        for typ, field, vt_type in _ID_MAPPING_TYPE_SPECS:
            if field == "_bvn":
                raw = bvn_raw
            else:
                raw = row.get(field)
            if raw is None:
                continue
            s = str(raw).strip()
            if not s:
                continue
            if vt_type:
                if vt:
                    mapping_id = vt.resolve_token(
                        vt_type, s,
                        context=f"{app_ctx} type={typ}",
                        row_data={
                            "application_no": app_no,
                            "app_id": app_id,
                            "user_id": user_id,
                            "mapping_type": typ,
                            "field": field,
                            "raw": s,
                        },
                    )
                    if not mapping_id:
                        continue
                else:
                    mapping_id = s
            else:
                mapping_id = s
            out.append({
                "id": anchor,
                "app_id": app_id,
                "mapping_id": mapping_id,
                "type": typ,
                "event_time": event_time,
            })
    return out


def _build_application_rows(
    raw_rows: List[dict],
    bvn_map: Dict[int, str],
    repay_map: Dict[str, int],
    vt: Optional[VtTokenResolver] = None,
) -> List[dict]:
    scheme_param = APPLICATION_SCHEME_PARAM_JSON
    out: List[dict] = []
    for row in raw_rows:
        user_id = int(row["user_id"])
        market_sn = row.get("sn") or ""
        market_no = str(row.get("application_no") or "").strip()
        core_sn = str(row.get("core_sn") or "").strip()
        if not core_sn or not market_no:
            continue
        application_no = format_application_no(row.get("app_id"), market_no)
        if not application_no:
            continue
        apply_date = row.get("apply_date") or 0
        due_date = row.get("due_date") or 0
        amount = int(row.get("amount") or 0)
        mobile_raw = row.get("mobile") or ""
        bank_raw = row.get("bank_account_number") or ""
        bvn_raw = bvn_map.get(user_id, "") or ""
        gaid_raw = row.get("gaid_idfa")
        app_ctx = f"application_no={application_no} market_no={row['application_no']} user_id={user_id}"
        if vt:
            mobile_val = vt.resolve_token(
                VtTokenResolver.VT_MOBILE, mobile_raw,
                context=f"{app_ctx} field=mobile",
                row_data=row,
            )
            if not mobile_val:
                continue
            if bank_raw:
                bank_val = vt.resolve_token(
                    VtTokenResolver.VT_BANK, bank_raw,
                    context=f"{app_ctx} field=bank",
                    row_data=row,
                )
                if not bank_val:
                    continue
            else:
                bank_val = ""
            if bvn_raw:
                id_number_val = vt.resolve_token(
                    VtTokenResolver.VT_ID_NUMBER, bvn_raw,
                    context=f"{app_ctx} field=id_number",
                    row_data={**row, "bvn": bvn_raw},
                )
                if not id_number_val:
                    continue
            else:
                id_number_val = ""
            gaid_val = None
            if gaid_raw not in (None, ""):
                gaid_val = vt.resolve_token(
                    VtTokenResolver.VT_GAID, gaid_raw,
                    context=f"{app_ctx} field=gaid",
                    row_data=row,
                )
        else:
            mobile_val = mobile_raw
            bank_val = bank_raw
            id_number_val = bvn_raw
            gaid_val = gaid_raw if gaid_raw not in (None, "") else None
        out.append({
            "application_no": application_no,
            "mobile": mobile_val,
            "coupon_code": "",
            "bid": row.get("bid") or "ng01",
            "app_id": row["app_id"],
            "app_version": row["app_version"],
            "user_id": user_id,
            "group_user_id": user_id,
            "sn": core_sn,
            "core_sn": core_sn,
            "is_test": 0,
            "is_first_apply": int(row.get("is_first_apply") or 0),
            "is_auto_apply": 0,
            "id_number": id_number_val,
            "gaid_idfa": gaid_val,
            "device_uuid": row.get("device_uuid") or "",
            "session_id": None,
            "bank_code": row.get("bank_code") or "",
            "bank_account_name": "",
            "bank_account_number": bank_val,
            "product_id": row.get("product_id"),
            "product_scheme_id": "PROD-002-D7",
            "product_calculator_version": "1",
            "product_scheme_param": scheme_param,
            "term": row.get("term"),
            "periods": 1,
            "repayment_method": 1,
            "repayment_plan": _application_repayment_plan_json(row),
            "credit_limit": amount,
            "loan_amount": amount,
            "principal": int(row.get("disburse_amount") or 0),
            "total_amount": int(row.get("repayment") or 0),
            "disbursed_amount": int(row.get("disburse_amount") or 0),
            "created_time": int(apply_date) * 1000 if apply_date else 0,
            "submited_time": int(row.get("core_apply_time") or 0) * 1000,
            "reviewed_time": int(row.get("core_audit_time") or 0) * 1000,
            "disbursed_time": int(row.get("disburse_time") or 0) * 1000,
            "last_paid_time": repay_map.get(market_sn, 0) * 1000,
            "paid_off_time": int(row.get("paid_time") or 0) * 1000,
            "lock_expire_time": (int(apply_date) + 7 * 86400) * 1000 if apply_date else 0,
            "status": _map_application_status(row.get("src_status")),
        })
    return out


def _map_loan_status(rp_status: Any, repaid_amt: Any) -> Any:
    try:
        st = int(rp_status)
        repaid = int(repaid_amt or 0)
    except (TypeError, ValueError):
        return rp_status
    if st == 1 and repaid == 0:
        return 20
    if st == 1 and repaid != 0:
        return 24
    if st == 3:
        return 23
    if st == 4:
        return 25
    if st == 2:
        return 27
    return st


def _build_loan_row(rp: dict, application_no: str) -> dict:
    st = int(rp.get("status") or 0)
    repaid = int(rp.get("repaid_amt") or 0)
    repay_last = int(rp.get("repay_last_time") or 0)
    settle_time = int(rp.get("settle_time") or 0)
    paid_amount = repaid if st in (2, 4) else 0
    paid_time_ms = _to_epoch_ms(repay_last) if repay_last > 0 else 0
    period = 1
    roll_sequence = 0
    loan_sn = str(rp.get("sn") or "").strip()
    return {
        "loan_no": format_loan_no(loan_sn, period, roll_sequence),
        "application_no": application_no,
        "period": period,
        "roll_sequence": roll_sequence,
        "start_date": _unix_to_date_str(rp.get("start_date")),
        "due_date": _unix_to_date_str(rp.get("due_date")),
        "due_date_final": _unix_to_date_str(rp.get("due_date")),
        "principal": int(rp.get("prin_amt") or 0),
        "interest": int(rp.get("interest") or 0),
        "admin_fee": int(rp.get("orig_fee") or 0),
        "service_fee": 0,
        "tax_fee": 0,
        "penalty_amount": int(rp.get("penalty") or 0),
        "reduction_amount": 0,
        "total_amount": int(rp.get("amt") or 0),
        "paid_amount": paid_amount,
        "paid_time": paid_time_ms if paid_time_ms > 0 else None,
        "paid_off_date": _unix_to_date_str(settle_time) if settle_time > 0 else None,
        "created_time": _to_epoch_ms(rp.get("created_at")),
        "status": _map_loan_status(st, repaid),
    }


def _fetch_loan_rows_from_source(
    src, sn_to_app_no: Dict[str, str],
) -> List[dict]:
    if not sn_to_app_no:
        return []
    c = "ng_loan_core"
    uniq = list(sn_to_app_no.keys())
    out: List[dict] = []
    for i in range(0, len(uniq), 2000):
        part = uniq[i:i + 2000]
        ph = ",".join(["%s"] * len(part))
        sql = f"""
            SELECT rp.sn, rp.plan_sn, rp.start_date, rp.due_date, rp.prin_amt, rp.interest,
                   rp.orig_fee, rp.penalty, rp.amt, rp.`status`, rp.repaid_amt,
                   rp.repay_last_time, rp.settle_time, rp.created_at
            FROM {c}.repay_plan rp
            INNER JOIN (
                SELECT sn, MAX(plan_sn) AS max_plan_sn
                FROM {c}.repay_plan
                WHERE sn IN ({ph})
                GROUP BY sn
            ) pick ON rp.sn = pick.sn AND rp.plan_sn = pick.max_plan_sn
        """
        with src.cursor() as cur:
            cur.execute(sql, part)
            for rp in cur.fetchall():
                app_no = sn_to_app_no.get(str(rp["sn"]))
                if app_no:
                    out.append(_build_loan_row(rp, app_no))
    return out


def _fetch_repay_map(src, ext_sns: List[str]) -> Dict[str, int]:
    """market.applicationNo(ext_sn) → core.application.sn → repay_record 最近还款时间。"""
    if not ext_sns:
        return {}
    uniq = list({s for s in ext_sns if s})
    out: Dict[str, int] = {}
    c = "ng_loan_core"
    for i in range(0, len(uniq), 2000):
        part = uniq[i:i + 2000]
        ph = ",".join(["%s"] * len(part))
        with src.cursor() as cur:
            cur.execute(
                f"""
                SELECT ca.ext_sn AS ext_sn, MAX(rr.repay_time) AS t
                FROM {c}.application ca
                INNER JOIN {c}.repay_record rr ON rr.sn = ca.sn
                WHERE ca.ext_sn IN ({ph})
                GROUP BY ca.ext_sn
                """,
                part,
            )
            for row in cur.fetchall():
                out[row["ext_sn"]] = int(row["t"] or 0)
    return out


_UD_SELECT_COLS = """
    ud.id, ud.`userId`, ud.bvn, ud.`firstName`, ud.`middleName`, ud.`lastName`,
    ud.email, ud.birthday, ud.gender, ud.`addressState`, ud.`addressDistrict`,
    ud.address, ud.company, ud.education, ud.marital, ud.profession, ud.salary,
    ud.`emergencyContact`, ud.`numberOfChildren`, ud.`payCycle`, ud.`salaryDay`,
    ud.bankCode, ud.bankAccount
"""


def _select_ud_rows_by_user_ids(src, user_ids: List[int], chunk: int = 5000) -> List[dict]:
    if not user_ids:
        return []
    m = "ng_loan_market"
    uniq = list({int(u) for u in user_ids})
    out: List[dict] = []
    for i in range(0, len(uniq), chunk):
        part = uniq[i:i + chunk]
        ph = ",".join(["%s"] * len(part))
        sql = f"""
            SELECT {_UD_SELECT_COLS}
            FROM {m}.user_data ud
            INNER JOIN (
                SELECT `userId`, MAX(id) AS max_id
                FROM {m}.user_data
                WHERE `userId` IN ({ph})
                GROUP BY `userId`
            ) t ON ud.`userId` = t.`userId` AND ud.id = t.max_id
        """
        with src.cursor() as cur:
            cur.execute(sql, part)
            out.extend(cur.fetchall())
    if not out:
        return []
    _clip_varchar_fields(out, _UD_VARCHAR_LIMITS)
    for row in out:
        row["ud_id"] = row.pop("id")
    return out


def _fetch_lup_by_app_mobile(
    src,
    pairs: List[Tuple[Any, str]],
    chunk: int = 400,
) -> List[dict]:
    if not pairs:
        return []
    if _lup_global_store is not None:
        out: List[dict] = []
        seen: set = set()
        for app_id, mobile in pairs:
            pair_key = (app_id, mobile)
            if pair_key in seen:
                continue
            seen.add(pair_key)
            password = None
            matched_mobile = mobile
            for variant in _mobile_format_variants(mobile):
                pw = _lup_global_store.get((app_id, variant))
                if pw is not None:
                    password = pw
                    matched_mobile = variant
                    break
            if password is None:
                continue
            out.append({
                "appId": app_id,
                "mobile": matched_mobile,
                "password": password,
                "src_id": 0,
            })
        return out
    m = "ng_loan_market"
    out: List[dict] = []
    for i in range(0, len(pairs), chunk):
        part = pairs[i:i + chunk]
        tuple_ph = ",".join(["(%s,%s)"] * len(part))
        params: List[Any] = []
        for app_id, mobile in part:
            params.extend([app_id, mobile])
        sql = f"""
            SELECT lup.id, lup.`appId`, lup.mobile, lup.password
            FROM {m}.log_user_password lup
            INNER JOIN (
                SELECT lup2.`appId`, lup2.mobile, MAX(lup2.id) AS max_id
                FROM {m}.log_user_password lup2
                WHERE (lup2.`appId`, lup2.mobile) IN ({tuple_ph})
                GROUP BY lup2.`appId`, lup2.mobile
            ) t ON lup.`appId` = t.`appId` AND lup.mobile = t.mobile AND lup.id = t.max_id
        """
        with src.cursor() as cur:
            cur.execute(sql, params)
            for row in cur.fetchall():
                out.append({
                    "appId": row["appId"],
                    "mobile": row["mobile"],
                    "password": row["password"],
                    "src_id": row["id"],
                })
    return out


def _fetch_uri_by_user_ids(src, user_ids: List[int], chunk: int = 5000) -> List[dict]:
    if not user_ids:
        return []
    m = "ng_loan_market"
    uniq = list({int(u) for u in user_ids})
    out: List[dict] = []
    for i in range(0, len(uniq), chunk):
        part = uniq[i:i + chunk]
        ph = ",".join(["%s"] * len(part))
        sql = f"""
            SELECT uri.id, uri.`userId`, uri.ip
            FROM {m}.user_reg_ip uri
            INNER JOIN (
                SELECT `userId`, MAX(id) AS max_id
                FROM {m}.user_reg_ip
                WHERE `userId` IN ({ph})
                GROUP BY `userId`
            ) t ON uri.`userId` = t.`userId` AND uri.id = t.max_id
        """
        with src.cursor() as cur:
            cur.execute(sql, part)
            out.extend(cur.fetchall())
    return [{"userId": r["userId"], "ip": r["ip"], "uri_id": r["id"]} for r in out]


def _fetch_dac_by_device_ids(src, device_ids: List[int], chunk: int = 5000) -> List[dict]:
    if not device_ids:
        return []
    m = "ng_loan_market"
    uniq = list({int(d) for d in device_ids if int(d) > 0})
    out: List[dict] = []
    for i in range(0, len(uniq), chunk):
        part = uniq[i:i + chunk]
        ph = ",".join(["%s"] * len(part))
        sql = f"""
            SELECT dac.id, dac.`deviceId`, dac.channel, dac.google_ads_campaign_id,
                   dac.google_ads_adgroup_id, dac.fb_install_referrer_campaign_id,
                   dac.fb_install_referrer_campaign_group_id
            FROM {m}.device_ad_channel dac
            INNER JOIN (
                SELECT `deviceId`, MAX(id) AS max_id
                FROM {m}.device_ad_channel
                WHERE `deviceId` IN ({ph})
                GROUP BY `deviceId`
            ) t ON dac.`deviceId` = t.`deviceId` AND dac.id = t.max_id
        """
        with src.cursor() as cur:
            cur.execute(sql, part)
            out.extend(cur.fetchall())
    return out


def _inline_mat_user_product(src, cfg: Dict[str, Any], lo: int, hi: int) -> List[dict]:
    m = "ng_loan_market"
    sql = f"""
        SELECT pick.`userId` AS userId, pick.`productId` AS productId, a.amount
            FROM (
            SELECT `userId`, `productId`, MAX(id) AS max_id
            FROM {m}.application
            WHERE `userId` > %s AND `userId` <= %s
              AND `productId` IS NOT NULL AND `productId` <> 0
            GROUP BY `userId`, `productId`
            ) pick
        INNER JOIN {m}.application a ON a.id = pick.max_id
    """
    with src.cursor() as cur:
        cur.execute(sql, (lo, hi))
        return list(cur.fetchall())


def _migrate_user_batch_once(
    cfg: Dict[str, Any],
    lo: int,
    hi: int,
    worker_id: int,
    prefix: str,
    batch_t0: float,
    step_timings: List[Tuple[str, int, float]],
    src,
    tgt,
):
    """单批：先 user 主键范围，再 keyed lookup，长查询后保活目标连接。"""
    t0 = time.perf_counter()
    rows_user = _select_user_batch_rows(src, lo, hi)
    el = time.perf_counter() - t0
    log_perf(prefix, "user", "source_select", len(rows_user), el)
    step_timings.append(("user_sel", len(rows_user), el))

    mat_timings, lookups = _fetch_user_batch_lookups(
        src, cfg, rows_user, lo, hi, prefix,
    )
    step_timings.extend(mat_timings)

    t0 = time.perf_counter()
    ins_rows = 0
    vt: Optional[VtTokenResolver] = None
    if rows_user:
        vt = VtTokenResolver(
            src,
            enabled=cfg.get("vt_token_enable", True),
            chunk=cfg.get("vt_token_chunk", 2000),
            vt_db=cfg.get("vt_token_db", "ng_loan_market"),
        )
        _register_user_batch_vt(vt, rows_user, lookups)
        t_vt = time.perf_counter()
        vt.prefetch()
        log_step(prefix, "vt_token", 0, time.perf_counter() - t_vt, extra=vt.summary())
        rows_user_ok: List[dict] = []
        vt_skip_user = 0
        for row in rows_user:
            mobile_raw = row.get("mobile") or ""
            user_ctx = f"user_id={row['user_id']}"
            mobile_token = vt.resolve_token(
                VtTokenResolver.VT_MOBILE, mobile_raw,
                context=f"{user_ctx} field=mobile",
                row_data=row,
            )
            if not mobile_token:
                vt_skip_user += 1
                continue
            row["mobile_lookup"] = row.get("mobile")
            row["mobile"] = mobile_token
            rows_user_ok.append(row)
        rows_user = rows_user_ok
        if vt_skip_user:
            log_step(prefix, "vt_skip", vt_skip_user, 0, extra="user mobile no token")
        _prepare_user_insert_rows(rows_user, lookups)
        tgt = _ensure_mysql_conn(tgt, cfg, "target")
        tgt, ins_rows = _bulk_insert_rows(
            tgt, cfg, "target",
            "user", USER_INSERT_COLS, rows_user,
            cfg["user_insert_batch"], ignore=True,
        )
    ins_el = time.perf_counter() - t0
    skip_note = ""
    if rows_user and ins_rows < len(rows_user):
        skip_note = f"skipped={len(rows_user) - ins_rows}"
    log_perf(prefix, "user", "target_insert", ins_rows, ins_el, extra=skip_note)
    step_timings.append(("user_ins", ins_rows, ins_el))

    ok_user_ids = {int(r["user_id"]) for r in rows_user}

    t0 = time.perf_counter()
    info_rows = 0
    rows_info = _build_user_info_rows(
        rows_user, lookups, vt if rows_user else None,
    )
    if rows_info:
        _ping_mysql_conn(tgt, cfg, "target")
        tgt, info_rows = _bulk_insert_rows(
            tgt, cfg, "target",
            "user_info", USER_INFO_COLS, rows_info,
            cfg["user_insert_batch"], ignore=True,
        )
    el = time.perf_counter() - t0
    log_perf(prefix, "user_info", "target_insert", info_rows, el)
    step_timings.append(("info_ins", info_rows, el))

    t0 = time.perf_counter()
    bc_rows = 0
    rows_bc = _build_bankcard_rows(
        lookups, vt if rows_user else None, ok_user_ids or None, cfg,
    )
    if rows_bc:
        _ping_mysql_conn(tgt, cfg, "target")
        tgt, bc_rows = _bulk_insert_rows(
            tgt, cfg, "target",
            "user_bankcard", USER_BANKCARD_COLS, rows_bc,
            cfg["user_insert_batch"], ignore=True,
        )
    el = time.perf_counter() - t0
    log_perf(prefix, "user_bankcard", "target_insert", bc_rows, el)
    step_timings.append(("bc_ins", bc_rows, el))

    t0 = time.perf_counter()
    prod_rows = 0
    prod_src = [
        p for p in (lookups.get("prod_rows") or [])
        if int(p["userId"]) in ok_user_ids
    ]
    rows_prod = _build_user_product_rows(prod_src)
    if rows_prod:
        _ping_mysql_conn(tgt, cfg, "target")
        tgt, prod_rows = _bulk_insert_rows(
            tgt, cfg, "target",
            "user_product", USER_PRODUCT_COLS, rows_prod,
            cfg["user_insert_batch"], ignore=True,
        )
    el = time.perf_counter() - t0
    log_perf(prefix, "user_product", "target_insert", prod_rows, el)
    step_timings.append(("prod_ins", prod_rows, el))

    wrote = bool(ins_rows or info_rows or bc_rows or prod_rows)
    t0 = time.perf_counter()
    if wrote:
        tgt.commit()
    commit_el = time.perf_counter() - t0
    log_step(prefix, "commit", 0, commit_el)

    batch_elapsed = time.perf_counter() - batch_t0
    log_batch_summary(prefix, batch_elapsed, step_timings)
    _get_worker_stats(cfg, worker_id).add_batch(batch_elapsed, step_timings)
    return tgt


def _migrate_user_info_batch_once(
    cfg: Dict[str, Any],
    lo: int,
    hi: int,
    worker_id: int,
    prefix: str,
    batch_t0: float,
    step_timings: List[Tuple[str, int, float]],
    src,
    tgt,
):
    """单批：仅灌 user_info（不写入 user / bankcard / product）。"""
    t0 = time.perf_counter()
    rows_user = _select_user_batch_rows(src, lo, hi)
    el = time.perf_counter() - t0
    log_perf(prefix, "user", "source_select", len(rows_user), el)
    step_timings.append(("user_sel", len(rows_user), el))

    mat_timings, lookups = _fetch_user_batch_lookups(
        src, cfg, rows_user, lo, hi, prefix,
    )
    step_timings.extend(mat_timings)

    t0 = time.perf_counter()
    info_rows = 0
    vt: Optional[VtTokenResolver] = None
    if rows_user:
        vt = VtTokenResolver(
            src,
            enabled=cfg.get("vt_token_enable", True),
            chunk=cfg.get("vt_token_chunk", 2000),
            vt_db=cfg.get("vt_token_db", "ng_loan_market"),
        )
        _register_user_batch_vt(vt, rows_user, lookups)
        t_vt = time.perf_counter()
        vt.prefetch()
        log_step(prefix, "vt_token", 0, time.perf_counter() - t_vt, extra=vt.summary())
        rows_user_ok: List[dict] = []
        vt_skip_user = 0
        for row in rows_user:
            mobile_raw = row.get("mobile") or ""
            user_ctx = f"user_id={row['user_id']}"
            mobile_token = vt.resolve_token(
                VtTokenResolver.VT_MOBILE, mobile_raw,
                context=f"{user_ctx} field=mobile",
                row_data=row,
            )
            if not mobile_token:
                vt_skip_user += 1
                continue
            row["mobile_lookup"] = row.get("mobile")
            row["mobile"] = mobile_token
            rows_user_ok.append(row)
        rows_user = rows_user_ok
        if vt_skip_user:
            log_step(prefix, "vt_skip", vt_skip_user, 0, extra="user mobile no token")
        rows_info = _build_user_info_rows(
            rows_user, lookups, vt if rows_user else None,
        )
        if rows_info:
            tgt = _ensure_mysql_conn(tgt, cfg, "target")
            tgt, info_rows = _bulk_insert_rows(
                tgt, cfg, "target",
                "user_info", USER_INFO_COLS, rows_info,
                cfg["user_insert_batch"], ignore=True,
            )
    el = time.perf_counter() - t0
    log_perf(prefix, "user_info", "target_insert", info_rows, el)
    step_timings.append(("info_ins", info_rows, el))

    t0 = time.perf_counter()
    tgt.commit()
    commit_el = time.perf_counter() - t0
    log_step(prefix, "commit", 0, commit_el)

    batch_elapsed = time.perf_counter() - batch_t0
    log_batch_summary(prefix, batch_elapsed, step_timings)
    _get_worker_stats(cfg, worker_id).add_batch(batch_elapsed, step_timings)
    return tgt


def migrate_user_info_batch(
    cfg: Dict[str, Any],
    lo: int,
    hi: int,
    worker_id: int = 0,
    src=None,
    tgt=None,
):
    """单批 user_info（含死锁重试）。"""
    prefix = f"[info W{worker_id} batch ({lo},{hi}]"
    own_src = src is None
    own_tgt = tgt is None
    if own_src:
        src = connect_source(cfg)
    if own_tgt:
        tgt = connect_target(cfg)
        _session_opts(tgt)

    max_retries = max(1, cfg.get("deadlock_max_retries", 5))
    out_tgt = tgt
    try:
        for attempt in range(max_retries):
            batch_t0 = time.perf_counter()
            step_timings: List[Tuple[str, int, float]] = []
            try:
                out_tgt = _migrate_user_info_batch_once(
                    cfg, lo, hi, worker_id, prefix, batch_t0,
                    step_timings, src, out_tgt,
                )
                if not own_tgt:
                    return out_tgt
                return None
            except pymysql.err.Error as exc:
                out_tgt = _prepare_batch_retry_tgt(out_tgt, cfg, "target")
                if not _is_batch_retryable_error(exc) or attempt >= max_retries - 1:
                    raise
                delay = _batch_retry_backoff(attempt)
                _log_batch_retry(prefix, exc, attempt, max_retries, delay)
                time.sleep(delay)
    except Exception:
        try:
            out_tgt.rollback()
        except Exception:
            pass
        raise
    finally:
        if own_src:
            _close_mysql_conn(src)
        if own_tgt:
            _close_mysql_conn(out_tgt)


def migrate_user_batch(
    cfg: Dict[str, Any],
    lo: int,
    hi: int,
    worker_id: int = 0,
    src=None,
    tgt=None,
):
    """单批：源库 SELECT + Python 组装 → 目标库 INSERT 正式表（含死锁重试）。"""
    prefix = f"[W{worker_id} batch ({lo},{hi}]"
    own_src = src is None
    own_tgt = tgt is None
    if own_src:
        src = connect_source(cfg)
    if own_tgt:
        tgt = connect_target(cfg)
        _session_opts(tgt)

    max_retries = max(1, cfg.get("deadlock_max_retries", 5))
    out_tgt = tgt
    try:
        for attempt in range(max_retries):
            batch_t0 = time.perf_counter()
            step_timings: List[Tuple[str, int, float]] = []
            try:
                out_tgt = _migrate_user_batch_once(
                    cfg, lo, hi, worker_id, prefix, batch_t0,
                    step_timings, src, out_tgt,
                )
                if not own_tgt:
                    return out_tgt
                return None
            except pymysql.err.Error as exc:
                out_tgt = _prepare_batch_retry_tgt(out_tgt, cfg, "target")
                if not _is_batch_retryable_error(exc) or attempt >= max_retries - 1:
                    raise
                delay = _batch_retry_backoff(attempt)
                _log_batch_retry(prefix, exc, attempt, max_retries, delay)
                time.sleep(delay)
    except Exception:
        try:
            out_tgt.rollback()
        except Exception:
            pass
        raise
    finally:
        if own_src:
            _close_mysql_conn(src)
        if own_tgt:
            _close_mysql_conn(out_tgt)


def _worker_range(cfg: Dict[str, Any], lo_start: int, lo_end: int, worker_id: int) -> None:
    batch = cfg["user_batch"]
    progress = cfg["progress_file"]
    lo = lo_start
    users_total = lo_end - lo_start
    t_worker = time.perf_counter()
    users_synced = 0
    prog = load_progress(progress)
    wkey = _progress_worker_key("user_lo", worker_id)
    lo = _load_worker_resume_lo(prog, "user_lo", worker_id, lo_start, f"[W{worker_id}]")
    users_synced = max(0, lo - lo_start)
    mig_log(
        f"[W{worker_id}] START range=({lo},{lo_end}] total={users_total} "
        f"batch_size={batch} log_every={cfg['log_every']}"
    )
    src = connect_source(cfg)
    tgt = connect_target(cfg)
    _session_opts(tgt)
    batch_num = 0
    try:
        while lo < lo_end:
            hi = min(lo + batch, lo_end)
            ret = migrate_user_batch(cfg, lo, hi, worker_id, src=src, tgt=tgt)
            if ret is not None:
                tgt = ret
            lo = hi
            batch_num += 1
            users_synced = lo - lo_start
            _maybe_save_progress(cfg, wkey, str(lo), batch_num)
            elapsed = time.perf_counter() - t_worker
            pct = (users_synced * 100.0 / users_total) if users_total else 100.0
            mig_log(
                f"[W{worker_id}] PROGRESS synced={users_synced}/{users_total} "
                f"({pct:.1f}%) id=({lo_start},{lo}] "
                f"elapsed={elapsed:.1f}s {_fmt_speed(users_synced, elapsed)}"
            )
    except Exception:
        try:
            tgt.rollback()
        except Exception:
            pass
        raise
    finally:
        _close_mysql_conn(src)
        _close_mysql_conn(tgt)
    if batch_num > 0:
        save_progress(progress, wkey, str(lo))
    _get_worker_stats(cfg, worker_id).finish()
    total_el = time.perf_counter() - t_worker
    mig_log(
        f"[W{worker_id}] DONE range=({lo_start},{lo_end}] "
        f"users={users_synced} elapsed={total_el:.1f}s {_fmt_speed(users_synced, total_el)}"
    )


def _user_info_worker_range(cfg: Dict[str, Any], lo_start: int, lo_end: int, worker_id: int) -> None:
    batch = cfg["user_batch"]
    progress = cfg["progress_file"]
    lo = lo_start
    users_total = lo_end - lo_start
    t_worker = time.perf_counter()
    users_synced = 0
    prog = load_progress(progress)
    key = "user_info_lo"
    wkey = _progress_worker_key(key, worker_id)
    lo = _load_worker_resume_lo(prog, key, worker_id, lo_start, f"[info W{worker_id}]")
    users_synced = max(0, lo - lo_start)
    mig_log(
        f"[info W{worker_id}] START range=({lo},{lo_end}] total={users_total} "
        f"batch_size={batch} log_every={cfg['log_every']}"
    )
    src = connect_source(cfg)
    tgt = connect_target(cfg)
    _session_opts(tgt)
    batch_num = 0
    try:
        while lo < lo_end:
            hi = min(lo + batch, lo_end)
            ret = migrate_user_info_batch(cfg, lo, hi, worker_id, src=src, tgt=tgt)
            if ret is not None:
                tgt = ret
            lo = hi
            batch_num += 1
            users_synced = lo - lo_start
            _maybe_save_progress(cfg, wkey, str(lo), batch_num)
            elapsed = time.perf_counter() - t_worker
            mig_log(
                f"[info W{worker_id}] PROGRESS synced={users_synced}/{users_total} "
                f"elapsed={elapsed:.1f}s {_fmt_speed(users_synced, elapsed)}"
            )
    finally:
        _close_mysql_conn(src)
        _close_mysql_conn(tgt)
    if batch_num > 0:
        save_progress(progress, wkey, str(lo))
    total_el = time.perf_counter() - t_worker
    mig_log(
        f"[info W{worker_id}] DONE range=({lo_start},{lo_end}] "
        f"users={users_synced} elapsed={total_el:.1f}s {_fmt_speed(users_synced, total_el)}"
    )


def migrate_user_info_all(cfg: Dict[str, Any]) -> None:
    mig_log("== user_info 模式: 仅灌 user_info ==")
    preload_vt_token_store(cfg)
    preload_lup_store(cfg)
    max_id = cfg["max_user_id"]
    lo_env = cfg["lo"]
    hi_env = cfg["hi"]
    lo_start = int(lo_env) if lo_env else 0
    lo_end = int(hi_env) if hi_env else max_id
    workers = _effective_workers(cfg["workers"], cfg)
    users_total = lo_end - lo_start

    mig_log(
        f"== user_info sync plan range=({lo_start},{lo_end}] users={users_total} "
        f"batch={cfg['user_batch']} workers={workers} "
        f"progress={cfg['progress_file']} =="
    )

    if workers == 1:
        _user_info_worker_range(cfg, lo_start, lo_end, 0)
        get_global_perf().log_summary("USER_INFO")
        return

    span = (lo_end - lo_start + workers - 1) // workers
    ranges = []
    for i in range(workers):
        a = lo_start + i * span
        b = min(lo_start + (i + 1) * span, lo_end)
        if a < b:
            ranges.append((a, b, i))

    mig_log(f"== user_info 启动 {len(ranges)} 个 worker, id 段 {ranges} ==")
    with ThreadPoolExecutor(max_workers=len(ranges)) as pool:
        futs = [pool.submit(_user_info_worker_range, cfg, a, b, wid) for a, b, wid in ranges]
        for f in as_completed(futs):
            f.result()
    get_global_perf().log_summary("USER_INFO")


def migrate_user_all(cfg: Dict[str, Any]) -> None:
    mig_log("== user 模式: 源库按批 SELECT + Python 组装写正式表 ==")
    preload_vt_token_store(cfg)
    preload_lup_store(cfg)
    max_id = cfg["max_user_id"]
    lo_env = cfg["lo"]
    hi_env = cfg["hi"]
    lo_start = int(lo_env) if lo_env else 0
    lo_end = int(hi_env) if hi_env else max_id
    workers = _effective_workers(cfg["workers"], cfg)
    users_total = lo_end - lo_start

    mig_log(
        f"== user sync plan range=({lo_start},{lo_end}] users={users_total} "
        f"batch={cfg['user_batch']} workers={workers} lookup_parallel={cfg['lookup_parallel']} "
        f"json={'orjson' if _USING_ORJSON else 'stdlib'} progress={cfg['progress_file']} =="
    )

    if workers == 1:
        _worker_range(cfg, lo_start, lo_end, 0)
        get_global_perf().log_summary("USER")
    else:
        span = (lo_end - lo_start + workers - 1) // workers
        ranges = []
        for i in range(workers):
            a = lo_start + i * span
            b = min(lo_start + (i + 1) * span, lo_end)
            if a < b:
                ranges.append((a, b, i))

        mig_log(f"== 启动 {len(ranges)} 个 worker, id 段 {ranges} ==")
        with ThreadPoolExecutor(max_workers=len(ranges)) as pool:
            futs = [pool.submit(_worker_range, cfg, a, b, wid) for a, b, wid in ranges]
            for f in as_completed(futs):
                f.result()
        get_global_perf().log_summary("USER")

    progress = cfg.get("progress_file") or ""
    if progress:
        save_progress(progress, "full_user_done", "1")
        mig_log(f"== user 阶段完成，已标记 full_user_done=1 progress={progress} ==")


def _migrate_app_batch_once(
    cfg: Dict[str, Any],
    lo: int,
    hi: int,
    worker_id: int,
    prefix: str,
    batch_t0: float,
    step_timings: List[Tuple[str, int, float]],
    src,
    tgt,
):
    """单批 application + loan + id_mapping：源库 SELECT + Python 组装 + bulk INSERT。"""
    t0 = time.perf_counter()
    raw_rows = _select_application_source_rows(src, lo, hi)
    el = time.perf_counter() - t0
    log_perf(prefix, "application", "source_select", len(raw_rows), el)
    step_timings.append(("app_sel", len(raw_rows), el))

    ins_rows = 0
    rows: List[dict] = []
    sn_to_app_no: Dict[str, str] = {}
    bvn_map: Dict[int, str] = {}
    vt: Optional[VtTokenResolver] = None
    if raw_rows:
        user_ids = [int(r["user_id"]) for r in raw_rows]
        sns = [r["sn"] for r in raw_rows if r.get("sn")]
        t0 = time.perf_counter()
        bvn_map, repay_map = _fetch_app_lookup_maps(cfg, src, user_ids, sns)
        vt = VtTokenResolver(
            src,
            enabled=cfg.get("vt_token_enable", True),
            chunk=cfg.get("vt_token_chunk", 2000),
            vt_db=cfg.get("vt_token_db", "ng_loan_market"),
        )
        _register_app_batch_vt(vt, raw_rows, bvn_map)
        t_vt = time.perf_counter()
        vt.prefetch()
        log_step(prefix, "vt_token", 0, time.perf_counter() - t_vt, extra=vt.summary())
        rows = _build_application_rows(raw_rows, bvn_map, repay_map, vt)
        included_app_nos = {r["application_no"] for r in rows}
        sn_to_app_no = {
            str(r["core_sn"]): r["application_no"]
            for r in rows
            if r.get("core_sn") and r["application_no"] in included_app_nos
        }
        app_skip = len(raw_rows) - len(rows)
        if app_skip:
            log_step(prefix, "vt_skip", app_skip, 0, extra="application VT miss")
        el = time.perf_counter() - t0
        log_perf(prefix, "application", "assemble", len(rows), el)
        step_timings.append(("app_asm", len(rows), el))
        t0 = time.perf_counter()
        tgt = _ensure_mysql_conn(tgt, cfg, "target")
        tgt, ins_rows = _bulk_insert_rows(
            tgt, cfg, "target",
            "application", APPLICATION_INSERT_COLS, rows,
            cfg["app_insert_batch"], ignore=True,
        )
        el = time.perf_counter() - t0
        log_perf(prefix, "application", "target_insert", ins_rows, el)
        step_timings.append(("app_ins", ins_rows, el))

    t0 = time.perf_counter()
    rows_loan = _fetch_loan_rows_from_source(src, sn_to_app_no)
    el = time.perf_counter() - t0
    log_perf(prefix, "loan", "source_select", len(rows_loan), el)
    step_timings.append(("loan_sel", len(rows_loan), el))

    t0 = time.perf_counter()
    loan_ins = 0
    if rows_loan:
        _ping_mysql_conn(tgt, cfg, "target")
        tgt, loan_ins = _bulk_insert_rows(
            tgt, cfg, "target",
            "loan", LOAN_INSERT_COLS, rows_loan,
            cfg["app_insert_batch"], ignore=True,
        )
    el = time.perf_counter() - t0
    log_perf(prefix, "loan", "target_insert", loan_ins, el)
    step_timings.append(("loan_ins", loan_ins, el))

    t0 = time.perf_counter()
    map_ins = 0
    if raw_rows and vt is not None:
        included_app_nos = {r["application_no"] for r in rows} if rows else set()
        map_src = [
            r for r in raw_rows
            if formatted_application_no_from_row(r) in included_app_nos
        ]
        rows_map = _build_id_mapping_rows(map_src, bvn_map, vt)
        if rows_map:
            _ping_mysql_conn(tgt, cfg, "target")
            tgt, map_ins = _bulk_upsert_rows(
                tgt, cfg, "target",
                "id_mapping", ID_MAPPING_COLS, rows_map,
                cfg["id_mapping_insert_batch"], ["event_time"],
            )
    el = time.perf_counter() - t0
    log_perf(prefix, "id_mapping", "target_upsert", map_ins, el)
    step_timings.append(("map_ins", map_ins, el))

    wrote = bool(ins_rows or loan_ins or map_ins)
    t0 = time.perf_counter()
    if wrote:
        tgt.commit()
    commit_el = time.perf_counter() - t0
    log_step(prefix, "commit", 0, commit_el)

    batch_elapsed = time.perf_counter() - batch_t0
    log_batch_summary(prefix, batch_elapsed, step_timings)
    global _app_stats
    if _app_stats:
        _app_stats.add_batch(batch_elapsed, step_timings)
    return tgt


def migrate_app_batch(
    cfg: Dict[str, Any],
    lo: int,
    hi: int,
    worker_id: int = 0,
    src=None,
    tgt=None,
):
    """单批 application + loan（含死锁重试）。"""
    prefix = f"[app W{worker_id} batch ({lo},{hi}]"
    own_src = src is None
    own_tgt = tgt is None
    if own_src:
        src = connect_source(cfg)
    if own_tgt:
        tgt = connect_target(cfg)
        _session_opts(tgt)

    max_retries = max(1, cfg.get("deadlock_max_retries", 5))
    out_tgt = tgt
    try:
        for attempt in range(max_retries):
            batch_t0 = time.perf_counter()
            step_timings: List[Tuple[str, int, float]] = []
            try:
                out_tgt = _migrate_app_batch_once(
                    cfg, lo, hi, worker_id, prefix, batch_t0,
                    step_timings, src, out_tgt,
                )
                if not own_tgt:
                    return out_tgt
                return None
            except pymysql.err.Error as exc:
                out_tgt = _prepare_batch_retry_tgt(out_tgt, cfg, "target")
                if not _is_batch_retryable_error(exc) or attempt >= max_retries - 1:
                    raise
                delay = _batch_retry_backoff(attempt)
                _log_batch_retry(prefix, exc, attempt, max_retries, delay)
                time.sleep(delay)
    except Exception:
        try:
            out_tgt.rollback()
        except Exception:
            pass
        raise
    finally:
        if own_src:
            _close_mysql_conn(src)
        if own_tgt:
            _close_mysql_conn(out_tgt)


def _app_worker_range(cfg: Dict[str, Any], lo_start: int, lo_end: int, worker_id: int) -> None:
    batch = cfg["app_batch"]
    progress = cfg["progress_file"]
    prog = load_progress(progress)
    wkey = _progress_worker_key("app_lo", worker_id)
    lo = _load_worker_resume_lo(prog, "app_lo", worker_id, lo_start, f"[app W{worker_id}]")
    t_worker = time.perf_counter()
    synced = max(0, lo - lo_start)
    total = lo_end - lo_start
    if lo >= lo_end:
        mig_log(f"[app W{worker_id}] SKIP already done range=({lo_start},{lo_end}]")
        return
    mig_log(
        f"[app W{worker_id}] START range=({lo},{lo_end}] total={total} batch={batch}"
    )
    src = connect_source(cfg)
    tgt = connect_target(cfg)
    _session_opts(tgt)
    batch_num = 0
    try:
        while lo < lo_end:
            hi = min(lo + batch, lo_end)
            ret = migrate_app_batch(cfg, lo, hi, worker_id, src=src, tgt=tgt)
            if ret is not None:
                tgt = ret
            lo = hi
            batch_num += 1
            synced = lo - lo_start
            _maybe_save_progress(cfg, wkey, str(lo), batch_num)
            elapsed = time.perf_counter() - t_worker
            mig_log(
                f"[app W{worker_id}] PROGRESS synced={synced}/{total} "
                f"elapsed={elapsed:.1f}s {_fmt_speed(synced, elapsed)}"
            )
    finally:
        _close_mysql_conn(src)
        _close_mysql_conn(tgt)
    if batch_num > 0:
        save_progress(progress, wkey, str(lo))
    total_el = time.perf_counter() - t_worker
    mig_log(
        f"[app W{worker_id}] DONE range=({lo_start},{lo_end}] "
        f"rows={synced} elapsed={total_el:.1f}s {_fmt_speed(synced, total_el)}"
    )


def migrate_application_all(cfg: Dict[str, Any]) -> None:
    preload_vt_token_store(cfg)
    src = connect_source(cfg)
    try:
        with src.cursor() as cur:
            if cfg["max_app_id"]:
                max_id = int(cfg["max_app_id"])
            else:
                cur.execute("SELECT MAX(id) AS m FROM ng_loan_market.application")
                max_id = int(cur.fetchone()["m"] or 0)
    finally:
        _close_mysql_conn(src)

    batch = cfg["app_batch"]
    progress = cfg["progress_file"]
    prog = load_progress(progress)
    if _is_phase_done(prog, "full_app_done"):
        mig_log("== application 已完成（full_app_done=1），跳过 ==")
        return

    lo_env = cfg["lo"]
    lo_start = int(lo_env) if lo_env else 0
    workers = _effective_workers(cfg["app_workers"], cfg)
    global _app_stats
    _app_stats = CumulativeStats("application", "[app]", cfg["log_every"])
    mig_log(
        f"== application plan id=({lo_start},{max_id}] batch={batch} workers={workers} "
        f"app_insert_batch={cfg['app_insert_batch']} "
        f"id_mapping_insert_batch={cfg['id_mapping_insert_batch']} "
        f"worker_balance={cfg.get('app_worker_balance', 'count')} "
        f"lookup_parallel={cfg['lookup_parallel']} progress={progress} =="
    )

    if workers == 1:
        _app_worker_range(cfg, lo_start, max_id, 0)
    else:
        ranges = _plan_application_worker_ranges(cfg, lo_start, max_id, workers)
        if not ranges:
            mig_log("== application 无数据可同步 ==")
        else:
            active_workers = cfg.get("app_active_workers", 0) or len(ranges)
            active_workers = max(1, min(int(active_workers), len(ranges)))
            mig_log(
                f"== application 启动 {len(ranges)} worker "
                f"active_workers={active_workers} =="
            )
            with ThreadPoolExecutor(max_workers=active_workers) as pool:
                futs = [
                    pool.submit(_app_worker_range, cfg, a, b, wid)
                    for a, b, wid in ranges
                ]
                for f in as_completed(futs):
                    f.result()

    if progress:
        save_progress(progress, "app_lo", str(max_id))
        save_progress(progress, "full_app_done", "1")
        mig_log(f"== application 阶段完成，已标记 full_app_done=1 ==")
    _app_stats.finish()
    get_global_perf().log_summary("APPLICATION")
    mig_log(f"== application DONE end={max_id} ==")


def migrate_full(cfg: Dict[str, Any]) -> None:
    t0 = time.perf_counter()
    progress = cfg.get("progress_file") or ""
    prog = load_progress(progress) if progress else {}
    mig_log(
        f"== FULL 开始: 源库按批同步 user + application 正式表 "
        f"resume user_done={prog.get('full_user_done', '0')} "
        f"app_done={prog.get('full_app_done', '0')} =="
    )
    if _is_phase_done(prog, "full_user_done"):
        mig_log("== FULL resume: user 阶段已完成，跳过 migrate_user_all ==")
    else:
        migrate_user_all(cfg)
    if _is_phase_done(prog, "full_app_done"):
        mig_log("== FULL resume: application 阶段已完成，跳过 migrate_application_all ==")
    else:
        migrate_application_all(cfg)
    verify_counts(cfg)
    get_global_perf().log_summary("FULL")
    total_el = time.perf_counter() - t0
    mig_log(f"== FULL 完成 total={total_el:.1f}s ({total_el / 60:.1f} min) ==")


def verify_counts(cfg: Dict[str, Any]) -> None:
    tgt = connect_target(cfg)
    try:
        tables = [
            "user", "user_info", "user_bankcard", "user_product",
            "application", "loan", "id_mapping",
        ]
        with tgt.cursor() as cur:
            for t in tables:
                cur.execute(f"SELECT COUNT(*) AS c FROM `{t}`")
                print(f"  {t}: {cur.fetchone()['c']}")
    finally:
        _close_mysql_conn(tgt)


def main() -> None:
    parser = argparse.ArgumentParser(description="尼日跨库迁移")
    parser.add_argument(
        "command",
        choices=["user", "user_info", "application", "full", "verify", "drop_staging"],
    )
    args = parser.parse_args()
    cfg = load_env()
    init_log(cfg)
    global _global_perf
    _global_perf = GlobalPerfStats()
    mig_log(f"skip_log={_skip_log_file}")
    mig_log(
        f"== ng_migration {args.command} START "
        f"source={cfg['source_host']} target={cfg['target_host']}/{cfg['target_db']} "
        f"DROP_MAT_ON_START={int(cfg['drop_mat_on_start'])} "
        f"PROGRESS_FILE={cfg.get('progress_file') or '-'} =="
    )
    prog_path = cfg.get("progress_file") or ""
    if prog_path and Path(prog_path).exists():
        prog = load_progress(prog_path)
        if prog:
            mig_log(f"== 当前进度: {prog} ==")

    if args.command in ("full", "user", "user_info"):
        _prepare_run_at_start(cfg, args.command)

    if args.command == "user":
        migrate_user_all(cfg)
    elif args.command == "user_info":
        migrate_user_info_all(cfg)
    elif args.command == "application":
        migrate_application_all(cfg)
    elif args.command == "full":
        migrate_full(cfg)
    elif args.command == "verify":
        verify_counts(cfg)
    elif args.command == "drop_staging":
        drop_legacy_staging_tables(cfg)

    mig_log(f"== ng_migration {args.command} END ==")


if __name__ == "__main__":
    main()
