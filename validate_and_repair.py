#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Validate and repair missing rows for ng migration.

First version scope:
- validates missing rows for user/user_info/user_bankcard/user_product/application/loan/id_mapping
- repairs missing rows by replaying existing migration batch logic over compact id ranges
- never deletes target extra rows
- writes success/failure CSV reports and optionally sends a Feishu webhook summary
"""
import argparse
import calendar
import csv
import json
import sys
import time
from decimal import Decimal
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, NamedTuple, Optional, Sequence, Tuple
from urllib import request

import ng_migration_run as mig


HERE = Path(__file__).resolve().parent
REPORT_DIR = HERE / "reports"
USER_TABLES = ("user", "user_info", "user_bankcard", "user_product")
APP_TABLES = ("application", "loan", "id_mapping")
TARGET_LOOKUP_CHUNK = 500
TARGET_MAPPING_CHUNK = 200
FIELD_DIFF_LOOKUP_CHUNK = 200
APP_VALIDATE_BATCH = 10000


_repair_log_file: Optional[Path] = None


class RepairPlan(NamedTuple):
    user_ids: List[int]
    bankcard_user_ids: List[int]
    product_keys: List[Tuple[int, str]]
    app_ids: List[int]


class FieldDiff(NamedTuple):
    scope: str
    key: str
    column: str
    expected: Any
    actual: Any


class DateWindow(NamedTuple):
    start: datetime
    end: datetime

    @property
    def start_epoch(self) -> int:
        return int(self.start.timestamp())

    @property
    def end_epoch(self) -> int:
        return int(self.end.timestamp())

    @property
    def start_sql(self) -> str:
        return self.start.strftime("%Y-%m-%d %H:%M:%S")

    @property
    def end_sql(self) -> str:
        return self.end.strftime("%Y-%m-%d %H:%M:%S")


class WindowSourceCache:
    """One-month window source rows preloaded in batches for reuse across validate/field-diff."""

    def __init__(self, window: "DateWindow") -> None:
        self.window = window
        self.user_ids: List[int] = []
        self.bankcard_user_ids: List[int] = []
        self.product_keys: List[Tuple[int, str]] = []
        self.app_ids_by_apply: List[int] = []
        self.loan_app_ids: List[int] = []
        self.mapping_app_ids: List[int] = []
        self.all_app_ids: List[int] = []
        self.rows_user: List[dict] = []
        self.rows_info: List[dict] = []
        self.rows_bankcard: List[dict] = []
        self.product_rows: List[dict] = []
        self.src_map: Dict[int, str] = {}
        self.app_keys_by_no: Dict[str, Tuple[str, int, str]] = {}
        self.src_mapping_by_app_id: Dict[Tuple[str, int, str, str], set] = {}
        self.app_rows: List[dict] = []
        self.loan_rows: List[dict] = []
        self.mapping_rows: List[dict] = []


def set_repair_log(path: str) -> None:
    global _repair_log_file
    _repair_log_file = Path(path) if path else None
    if _repair_log_file:
        _repair_log_file.parent.mkdir(parents=True, exist_ok=True)


def repair_log(message: str) -> None:
    line = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {message}"
    print(line, flush=True)
    if _repair_log_file:
        with _repair_log_file.open("a", encoding="utf-8") as f:
            f.write(line + "\n")


def flush_writer(writer) -> None:
    flush = getattr(writer, "flush", None)
    if flush:
        flush()


def q_rows(conn, sql: str, params: Sequence[Any] = ()) -> List[dict]:
    sql_l = sql.strip().lower()
    if not sql_l.startswith("select"):
        raise ValueError("validate_and_repair only allows SELECT for helper queries")
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return list(cur.fetchall())


def q_int(conn, sql: str, params: Sequence[Any] = ()) -> int:
    rows = q_rows(conn, sql, params)
    if not rows:
        return 0
    return int(next(iter(rows[0].values())) or 0)


def q_values(conn, sql: str, params: Sequence[Any] = (), key: str = None) -> List[Any]:
    rows = q_rows(conn, sql, params)
    if not rows:
        return []
    if key is None:
        key = next(iter(rows[0].keys()))
    return [r[key] for r in rows]


def _target_conn_retry(cfg: Dict[str, Any], conn, fn):
    last_exc: Optional[BaseException] = None
    for attempt in range(3):
        try:
            if attempt:
                mig._close_mysql_conn(conn)
                conn = mig.connect_target(cfg)
                mig._session_opts(conn)
            return conn, fn(conn)
        except Exception as exc:
            last_exc = exc
            repair_log(f"target query retry attempt={attempt + 1}/3 {type(exc).__name__}: {exc}")
            if attempt >= 2:
                raise
            time.sleep(5 * (attempt + 1))
    raise last_exc  # type: ignore[misc]


def _refresh_target_conn(cfg: Dict[str, Any], conn):
    mig._close_mysql_conn(conn)
    conn = mig.connect_target(cfg)
    mig._session_opts(conn)
    return conn


def _quote_ident(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def _values_equal(expected: Any, actual: Any) -> bool:
    if expected is actual:
        return True
    if expected is None or actual is None:
        return expected is None and actual is None
    if isinstance(expected, Decimal) or isinstance(actual, Decimal):
        try:
            return Decimal(str(expected)) == Decimal(str(actual))
        except Exception:
            return False
    if isinstance(expected, (int, float)) and isinstance(actual, (int, float, Decimal)):
        try:
            return Decimal(str(expected)) == Decimal(str(actual))
        except Exception:
            return False
    return str(expected) == str(actual)


def _diff_rows(scope: str, key: str, expected: dict, actual: Optional[dict], columns: Sequence[str]) -> List[FieldDiff]:
    if not actual:
        return []
    diffs: List[FieldDiff] = []
    for col in columns:
        if not _values_equal(expected.get(col), actual.get(col)):
            diffs.append(FieldDiff(scope, key, col, expected.get(col), actual.get(col)))
    return diffs


def _json_cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return str(value)


def chunk_ranges(lo: int, hi: int, size: int) -> Iterable[Tuple[int, int]]:
    cur = lo
    while cur < hi:
        nxt = min(cur + size, hi)
        yield cur, nxt
        cur = nxt


def _minus_one_calendar_month(dt: datetime) -> datetime:
    year = dt.year
    month = dt.month - 1
    if month == 0:
        year -= 1
        month = 12
    day = min(dt.day, calendar.monthrange(year, month)[1])
    return dt.replace(year=year, month=month, day=day)


def compute_date_window(name: str, now: Optional[datetime] = None) -> DateWindow:
    if name != "last-month":
        raise ValueError(f"unsupported date window: {name}")
    cur = now or datetime.now()
    end = cur.replace(hour=0, minute=0, second=0, microsecond=0)
    start = _minus_one_calendar_month(end)
    return DateWindow(start=start, end=end)


def compact_ids_to_ranges(ids: Sequence[int], margin: int = 0) -> List[Tuple[int, int]]:
    vals = sorted({int(x) for x in ids if x is not None})
    if not vals:
        return []
    ranges: List[Tuple[int, int]] = []
    start = prev = vals[0]
    for val in vals[1:]:
        if val == prev + 1:
            prev = val
            continue
        ranges.append((max(0, start - 1 - margin), prev + margin))
        start = prev = val
    ranges.append((max(0, start - 1 - margin), prev + margin))
    return ranges


def missing_scalar_keys(expected: Sequence[Any], actual: Sequence[Any]) -> List[Any]:
    actual_set = {x for x in actual}
    return sorted({x for x in expected if x not in actual_set})


def missing_tuple_keys(expected: Sequence[Tuple], actual: Sequence[Tuple]) -> List[Tuple]:
    actual_set = {tuple(x) for x in actual}
    return sorted({tuple(x) for x in expected if tuple(x) not in actual_set})


def target_user_ids(tgt, lo: int, hi: int) -> set:
    return {
        int(x) for x in q_values(
            tgt,
            "SELECT user_id FROM `user` WHERE user_id > %s AND user_id <= %s",
            (lo, hi),
            "user_id",
        )
    }


def target_user_info_ids(tgt, lo: int, hi: int) -> set:
    return {
        int(x) for x in q_values(
            tgt,
            "SELECT user_id FROM user_info WHERE user_id > %s AND user_id <= %s",
            (lo, hi),
            "user_id",
        )
    }


def source_user_ids_by_created_window(src, window: DateWindow) -> List[int]:
    return [
        int(x) for x in q_values(
            src,
            """
            SELECT u.id AS user_id
            FROM ng_loan_market.`user` u
            WHERE u.created >= %s AND u.created < %s
            ORDER BY u.id ASC
            """,
            (window.start_sql, window.end_sql),
            "user_id",
        )
    ]


def source_column_exists(src, schema: str, table: str, column: str) -> bool:
    return bool(q_int(
        src,
        """
        SELECT COUNT(*) AS c
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s AND COLUMN_NAME=%s
        """,
        (schema, table, column),
    ))


def source_bankcard_user_ids_by_window(src, window: DateWindow) -> List[int]:
    if source_column_exists(src, "ng_loan_market", "user_data", "created"):
        time_filter = "ud.created >= %s AND ud.created < %s"
        params: Tuple[Any, ...] = (window.start_sql, window.end_sql)
        join_user = ""
    elif source_column_exists(src, "ng_loan_market", "user_data", "updated"):
        time_filter = "ud.updated >= %s AND ud.updated < %s"
        params = (window.start_sql, window.end_sql)
        join_user = ""
    else:
        time_filter = "u.created >= %s AND u.created < %s"
        params = (window.start_sql, window.end_sql)
        join_user = "INNER JOIN ng_loan_market.`user` u ON u.id = ud.userId"
    return [
        int(x) for x in q_values(
            src,
            f"""
            SELECT DISTINCT ud.userId AS user_id
            FROM ng_loan_market.user_data ud
            {join_user}
            WHERE {time_filter}
              AND ud.bankCode IS NOT NULL AND ud.bankCode <> ''
              AND ud.bankAccount IS NOT NULL AND ud.bankAccount <> ''
            ORDER BY ud.userId ASC
            """,
            params,
            "user_id",
        )
    ]


def source_user_product_keys_by_apply_date_window(src, window: DateWindow) -> List[Tuple[int, str]]:
    rows = q_rows(
        src,
        """
        SELECT a.userId AS user_id, a.productId AS product_id
        FROM ng_loan_market.application a
        WHERE a.applyDate >= %s AND a.applyDate < %s
          AND a.productId IS NOT NULL AND a.productId <> 0
        GROUP BY a.userId, a.productId
        ORDER BY a.userId ASC, a.productId ASC
        """,
        (window.start_epoch, window.end_epoch),
    )
    return [(int(r["user_id"]), str(r["product_id"])) for r in rows]


def source_user_ids(src, lo: int, hi: int) -> set:
    return {
        int(x) for x in q_values(
            src,
            "SELECT id AS user_id FROM ng_loan_market.`user` WHERE id > %s AND id <= %s",
            (lo, hi),
            "user_id",
        )
    }


def validate_user_missing(src, tgt, max_user_id: int, batch_size: int) -> Dict[str, List[int]]:
    out = {
        "user": [],
        "user_info": [],
        "user_bankcard": [],
        "user_product": [],
        "user_info_without_user": [],
        "user_without_user_info": [],
    }
    scanned = 0
    t0 = time.perf_counter()
    for lo, hi in chunk_ranges(0, max_user_id, batch_size):
        src_ids = source_user_ids(src, lo, hi)
        tgt_user = target_user_ids(tgt, lo, hi)
        tgt_info = target_user_info_ids(tgt, lo, hi)
        src_bank = source_bankcard_user_ids(src, lo, hi)
        tgt_bank = target_bankcard_user_ids(tgt, lo, hi)
        src_products = source_user_product_keys(src, lo, hi)
        tgt_products = target_user_product_keys(tgt, lo, hi)
        out["user"].extend(sorted(src_ids - tgt_user))
        out["user_info"].extend(sorted(src_ids - tgt_info))
        out["user_bankcard"].extend(missing_scalar_keys(src_bank, tgt_bank))
        out["user_product"].extend(missing_tuple_keys(src_products, tgt_products))
        out["user_info_without_user"].extend(sorted(tgt_info - tgt_user))
        out["user_without_user_info"].extend(sorted(tgt_user - tgt_info))
        scanned += len(src_ids)
        print(
            f"validate user scanned={scanned} range=({lo},{hi}] "
            f"missing_user={len(out['user'])} missing_info={len(out['user_info'])} "
            f"elapsed={time.perf_counter() - t0:.1f}s",
            flush=True,
        )
    return out


def validate_user_missing_for_window(
    src,
    tgt,
    window: DateWindow,
    cache: Optional[WindowSourceCache] = None,
) -> Dict[str, List[Any]]:
    if cache is not None:
        user_ids = cache.user_ids
        bankcard_user_ids = cache.bankcard_user_ids
        product_keys = cache.product_keys
    else:
        user_ids = source_user_ids_by_created_window(src, window)
        bankcard_user_ids = source_bankcard_user_ids_by_window(src, window)
        product_keys = source_user_product_keys_by_apply_date_window(src, window)

    tgt_user = target_existing_user_ids(tgt, user_ids)
    tgt_info = target_existing_user_info_ids(tgt, user_ids)
    tgt_bank = target_existing_bankcard_user_ids(tgt, bankcard_user_ids)
    tgt_products = target_existing_user_product_keys(tgt, product_keys)

    out: Dict[str, List[Any]] = {
        "user": sorted(set(user_ids) - tgt_user),
        "user_info": sorted(set(user_ids) - tgt_info),
        "user_bankcard": sorted(set(bankcard_user_ids) - tgt_bank),
        "user_product": missing_tuple_keys(product_keys, list(tgt_products)),
        "user_info_without_user": sorted((tgt_info & set(user_ids)) - tgt_user),
        "user_without_user_info": sorted((tgt_user & set(user_ids)) - tgt_info),
    }
    repair_log(
        "validate user window "
        f"range=[{window.start_sql},{window.end_sql}) "
        f"user_src={len(user_ids)} bankcard_src={len(bankcard_user_ids)} "
        f"product_src={len(product_keys)} missing_user={len(out['user'])} "
        f"missing_info={len(out['user_info'])} missing_bankcard={len(out['user_bankcard'])} "
        f"missing_product={len(out['user_product'])}"
    )
    return out


def source_bankcard_user_ids(src, lo: int, hi: int) -> List[int]:
    rows = q_rows(
        src,
        """
        SELECT DISTINCT ud.userId AS user_id
        FROM ng_loan_market.user_data ud
        INNER JOIN (
            SELECT userId, MAX(id) AS max_id
            FROM ng_loan_market.user_data
            WHERE userId > %s AND userId <= %s
            GROUP BY userId
        ) latest ON latest.userId = ud.userId AND latest.max_id = ud.id
        WHERE ud.bankCode IS NOT NULL AND ud.bankCode <> ''
          AND ud.bankAccount IS NOT NULL AND ud.bankAccount <> ''
        """,
        (lo, hi),
    )
    return [int(r["user_id"]) for r in rows]


def target_bankcard_user_ids(tgt, lo: int, hi: int) -> List[int]:
    rows = q_rows(
        tgt,
        """
        SELECT DISTINCT group_user_id AS user_id
        FROM user_bankcard
        WHERE group_user_id > %s AND group_user_id <= %s
        """,
        (lo, hi),
    )
    return [int(r["user_id"]) for r in rows]


def source_user_product_keys(src, lo: int, hi: int) -> List[Tuple[int, str]]:
    rows = q_rows(
        src,
        """
        SELECT pick.userId AS user_id, pick.productId AS product_id
        FROM (
            SELECT userId, productId, MAX(id) AS max_id
            FROM ng_loan_market.application
            WHERE userId > %s AND userId <= %s
              AND productId IS NOT NULL AND productId <> 0
            GROUP BY userId, productId
        ) pick
        """,
        (lo, hi),
    )
    return [(int(r["user_id"]), str(r["product_id"])) for r in rows]


def target_user_product_keys(tgt, lo: int, hi: int) -> List[Tuple[int, str]]:
    rows = q_rows(
        tgt,
        """
        SELECT group_user_id AS user_id, product_id
        FROM user_product
        WHERE group_user_id > %s AND group_user_id <= %s
        """,
        (lo, hi),
    )
    return [(int(r["user_id"]), str(r["product_id"])) for r in rows]


def source_application_nos(src, lo: int, hi: int) -> Dict[int, str]:
    rows = q_rows(
        src,
        """
        SELECT id, applicationNo AS application_no
        FROM ng_loan_market.application
        WHERE id > %s AND id <= %s
          AND applicationNo IS NOT NULL AND applicationNo <> ''
        """,
        (lo, hi),
    )
    return {int(r["id"]): str(r["application_no"]) for r in rows}


def source_application_ids_by_apply_date_window(src, window: DateWindow) -> List[int]:
    return [
        int(x) for x in q_values(
            src,
            """
            SELECT id
            FROM ng_loan_market.application
            WHERE applyDate >= %s AND applyDate < %s
              AND applicationNo IS NOT NULL AND applicationNo <> ''
            ORDER BY id ASC
            """,
            (window.start_epoch, window.end_epoch),
            "id",
        )
    ]


def source_application_ids_by_created_window(src, window: DateWindow) -> List[int]:
    return [
        int(x) for x in q_values(
            src,
            """
            SELECT id
            FROM ng_loan_market.application
            WHERE created >= %s AND created < %s
              AND applicationNo IS NOT NULL AND applicationNo <> ''
            ORDER BY id ASC
            """,
            (window.start_sql, window.end_sql),
            "id",
        )
    ]


def source_application_ids_by_repay_plan_created_window(src, window: DateWindow) -> List[int]:
    return [
        int(x) for x in q_values(
            src,
            """
            SELECT DISTINCT a.id
            FROM ng_loan_market.application a
            INNER JOIN ng_loan_core.application ca ON ca.ext_sn = a.applicationNo
            INNER JOIN ng_loan_core.repay_plan rp ON rp.sn = ca.sn
            WHERE rp.created_at >= %s AND rp.created_at < %s
              AND a.applicationNo IS NOT NULL AND a.applicationNo <> ''
            ORDER BY a.id ASC
            """,
            (window.start_sql, window.end_sql),
            "id",
        )
    ]


def select_application_validation_source_rows(src, lo: int, hi: int) -> List[dict]:
    rows = mig._select_application_source_rows(src, lo, hi)
    id_rows = q_rows(
        src,
        """
        SELECT id, applicationNo AS application_no
        FROM ng_loan_market.application
        WHERE id > %s AND id <= %s
          AND applicationNo IS NOT NULL AND applicationNo <> ''
        """,
        (lo, hi),
    )
    id_by_no = {str(r["application_no"]): int(r["id"]) for r in id_rows}
    for row in rows:
        row["source_app_id"] = id_by_no.get(str(row.get("application_no") or ""))
    return rows


def _build_application_validation_rows_for_ids(
    cfg: Dict[str, Any],
    src,
    app_ids: Sequence[int],
) -> Tuple[Dict[int, str], Dict[str, Tuple[str, int, str]], Dict[Tuple[str, int, str, str], set]]:
    ids = sorted({int(x) for x in app_ids if x is not None})
    src_map: Dict[int, str] = {}
    app_keys_by_no: Dict[str, Tuple[str, int, str]] = {}
    src_mapping_by_app_id: Dict[Tuple[str, int, str, str], set] = {}
    if not ids:
        return src_map, app_keys_by_no, src_mapping_by_app_id
    wanted = set(ids)
    for lo, hi in compact_ids_to_ranges(ids):
        part_src_map, part_keys, part_mapping = build_application_validation_rows(cfg, src, lo, hi)
        allowed_nos = {
            app_no
            for app_id, app_no in part_src_map.items()
            if int(app_id) in wanted
        }
        src_map.update({
            int(app_id): app_no
            for app_id, app_no in part_src_map.items()
            if int(app_id) in wanted
        })
        app_keys_by_no.update({
            app_no: key
            for app_no, key in part_keys.items()
            if app_no in allowed_nos
        })
        for key, key_app_ids in part_mapping.items():
            kept = {int(app_id) for app_id in key_app_ids if int(app_id) in wanted}
            if kept:
                src_mapping_by_app_id.setdefault(key, set()).update(kept)
    return src_map, app_keys_by_no, src_mapping_by_app_id


def _merge_app_missing(out: Dict[str, List[Any]], part: Dict[str, List[Any]]) -> None:
    for key in ("application_ids", "loan_app_nos", "id_mapping", "_repair_application_ids"):
        out[key].extend(part.get(key, []))


def _validate_application_missing_with_source(
    cfg: Dict[str, Any],
    tgt,
    application_ids: Sequence[int],
    loan_ids: Sequence[int],
    id_mapping_ids: Sequence[int],
    src_map: Dict[int, str],
    app_keys_by_no: Dict[str, Tuple[str, int, str]],
    src_mapping_by_app_id: Dict[Tuple[str, int, str, str], set],
    log_prefix: str = "validate application ids",
) -> Dict[str, List[Any]]:
    out: Dict[str, List[Any]] = {
        "application_ids": [],
        "loan_app_nos": [],
        "id_mapping": [],
        "_repair_application_ids": [],
    }
    lookup_chunk = int(cfg.get("repair_lookup_chunk", TARGET_LOOKUP_CHUNK))
    mapping_chunk = int(cfg.get("repair_mapping_chunk", TARGET_MAPPING_CHUNK))
    app_id_by_no = {app_no: app_id for app_id, app_no in src_map.items()}

    app_id_set = {int(x) for x in application_ids if x is not None}
    loan_id_set = {int(x) for x in loan_ids if x is not None}
    mapping_id_set = {int(x) for x in id_mapping_ids if x is not None}

    if app_id_set or loan_id_set or mapping_id_set:
        needed_app_keys = [
            key for app_no, key in app_keys_by_no.items()
            if int(app_id_by_no.get(app_no, 0)) in (app_id_set | loan_id_set | mapping_id_set)
        ]
        repair_log(f"{log_prefix} target_lookup start keys={len(needed_app_keys)}")
        tgt, tgt_app_keys = target_application_keys(cfg, tgt, needed_app_keys, lookup_chunk)
        repair_log(f"{log_prefix} target_lookup done matched={len(tgt_app_keys)}")
        existing_app_nos = {
            app_no for app_no, key in app_keys_by_no.items()
            if key in tgt_app_keys
        }
    else:
        existing_app_nos = set()

    if app_id_set:
        app_missing = [
            app_id for app_id, app_no in src_map.items()
            if app_id in app_id_set and app_no not in existing_app_nos
        ]
        out["application_ids"].extend(sorted(app_missing))
        out["_repair_application_ids"].extend(sorted(app_missing))

    if loan_id_set:
        loan_nos = [
            app_no for app_id, app_no in src_map.items()
            if app_id in loan_id_set
        ]
        loan_existing_app_nos = [no for no in loan_nos if no in existing_app_nos]
        missing_app_for_loan = [
            app_id for app_id, app_no in src_map.items()
            if app_id in loan_id_set and app_no not in existing_app_nos
        ]
        tgt_loans = target_loan_app_nos(cfg, tgt, loan_existing_app_nos, lookup_chunk)
        missing_loan_nos = sorted(no for no in loan_existing_app_nos if no not in tgt_loans)
        out["loan_app_nos"].extend(missing_loan_nos)
        out["_repair_application_ids"].extend(sorted(missing_app_for_loan))
        out["_repair_application_ids"].extend(
            sorted(app_id_by_no[no] for no in missing_loan_nos if no in app_id_by_no)
        )

    if mapping_id_set:
        expected_mapping = [
            key for key, app_ids_for_key in src_mapping_by_app_id.items()
            if app_ids_for_key & mapping_id_set
        ]
        tgt, tgt_mapping = target_id_mapping_keys(cfg, tgt, expected_mapping, mapping_chunk)
        missing_mapping = missing_tuple_keys(expected_mapping, tgt_mapping)
        out["id_mapping"].extend(missing_mapping)
        for key in missing_mapping:
            out["_repair_application_ids"].extend(sorted(src_mapping_by_app_id.get(key, set()) & mapping_id_set))

    out["_repair_application_ids"] = sorted({int(x) for x in out["_repair_application_ids"]})
    return out


def validate_application_missing_for_ids(
    cfg: Dict[str, Any],
    src,
    tgt,
    application_ids: Sequence[int] = (),
    loan_ids: Sequence[int] = (),
    id_mapping_ids: Sequence[int] = (),
    cache: Optional[WindowSourceCache] = None,
) -> Dict[str, List[Any]]:
    all_ids = sorted({
        int(x)
        for x in [*application_ids, *loan_ids, *id_mapping_ids]
        if x is not None
    })
    repair_log(f"validate application ids start count={len(all_ids)}")
    if cache is not None:
        batch_ids = set(all_ids)
        src_map, app_keys_by_no, src_mapping_by_app_id = _slice_application_source_for_ids(
            cache.src_map, cache.app_keys_by_no, cache.src_mapping_by_app_id, batch_ids,
        )
        repair_log(
            f"validate application ids cache_slice apps={len(src_map)} "
            f"keys={len(app_keys_by_no)} mapping_keys={len(src_mapping_by_app_id)}"
        )
    else:
        src_map, app_keys_by_no, src_mapping_by_app_id = _build_application_validation_rows_for_ids(cfg, src, all_ids)
        repair_log(
            f"validate application ids src_load done apps={len(src_map)} "
            f"keys={len(app_keys_by_no)} mapping_keys={len(src_mapping_by_app_id)}"
        )
    return _validate_application_missing_with_source(
        cfg,
        tgt,
        application_ids,
        loan_ids,
        id_mapping_ids,
        src_map,
        app_keys_by_no,
        src_mapping_by_app_id,
    )


def validate_application_missing_for_window(
    cfg: Dict[str, Any],
    src,
    tgt,
    window: DateWindow,
    cache: Optional[WindowSourceCache] = None,
) -> Dict[str, List[Any]]:
    if cache is not None:
        app_ids = cache.app_ids_by_apply
        loan_ids = cache.loan_app_ids
        mapping_ids = cache.mapping_app_ids
    else:
        app_ids = source_application_ids_by_apply_date_window(src, window)
        loan_ids = source_application_ids_by_repay_plan_created_window(src, window)
        mapping_ids = source_application_ids_by_created_window(src, window)
    repair_log(
        "validate application window "
        f"range=[{window.start_sql},{window.end_sql}) "
        f"application_src={len(app_ids)} loan_src={len(loan_ids)} "
        f"id_mapping_src={len(mapping_ids)}"
    )
    app_id_set = {int(x) for x in app_ids if x is not None}
    loan_id_set = {int(x) for x in loan_ids if x is not None}
    mapping_id_set = {int(x) for x in mapping_ids if x is not None}
    all_ids = sorted(app_id_set | loan_id_set | mapping_id_set)
    out: Dict[str, List[Any]] = {
        "application_ids": [],
        "loan_app_nos": [],
        "id_mapping": [],
        "_repair_application_ids": [],
    }
    if not all_ids:
        return out
    batch_size = max(1, int(cfg.get("app_validate_batch", APP_VALIDATE_BATCH)))
    total_batches = (len(all_ids) + batch_size - 1) // batch_size
    for batch_no, i in enumerate(range(0, len(all_ids), batch_size), start=1):
        batch_ids = set(all_ids[i:i + batch_size])
        repair_log(f"validate application batch {batch_no}/{total_batches} ids={len(batch_ids)}")
        part = validate_application_missing_for_ids(
            cfg,
            src,
            tgt,
            application_ids=sorted(app_id_set & batch_ids),
            loan_ids=sorted(loan_id_set & batch_ids),
            id_mapping_ids=sorted(mapping_id_set & batch_ids),
            cache=cache,
        )
        _merge_app_missing(out, part)
    out["_repair_application_ids"] = sorted({int(x) for x in out["_repair_application_ids"]})
    repair_log(
        "validate application window done "
        f"missing_application={len(out['application_ids'])} "
        f"missing_loan={len(out['loan_app_nos'])} "
        f"missing_id_mapping={len(out['id_mapping'])}"
    )
    return out


def source_application_ids_by_nos(src, app_nos: Sequence[str]) -> Dict[str, int]:
    vals = sorted({str(x) for x in app_nos if x})
    out: Dict[str, int] = {}
    for i in range(0, len(vals), 5000):
        part = vals[i:i + 5000]
        if not part:
            continue
        ph = ",".join(["%s"] * len(part))
        rows = q_rows(
            src,
            f"""
            SELECT id, applicationNo AS application_no
            FROM ng_loan_market.application
            WHERE applicationNo IN ({ph})
            """,
            part,
        )
        for row in rows:
            out[str(row["application_no"])] = int(row["id"])
    return out


def target_application_nos(tgt, app_nos: Sequence[str], chunk_size: int = TARGET_LOOKUP_CHUNK) -> set:
    vals = [str(x) for x in app_nos if x]
    if not vals:
        return set()
    out = set()
    chunk_size = max(1, int(chunk_size or TARGET_LOOKUP_CHUNK))
    for i in range(0, len(vals), chunk_size):
        part = vals[i:i + chunk_size]
        ph = ",".join(["%s"] * len(part))
        rows = q_values(
            tgt,
            f"SELECT application_no FROM application WHERE application_no IN ({ph})",
            part,
            "application_no",
        )
        out.update(str(x) for x in rows)
    return out


def target_loan_app_nos(
    cfg: Dict[str, Any],
    tgt,
    app_nos: Sequence[str],
    chunk_size: int = TARGET_LOOKUP_CHUNK,
) -> set:
    vals = [str(x) for x in app_nos if x]
    if not vals:
        return set()
    out = set()
    chunk_size = max(1, int(chunk_size or TARGET_LOOKUP_CHUNK))
    total_chunks = (len(vals) + chunk_size - 1) // chunk_size
    for chunk_idx, i in enumerate(range(0, len(vals), chunk_size), start=1):
        part = vals[i:i + chunk_size]
        ph = ",".join(["%s"] * len(part))

        def _query(conn, _ph=ph, _part=part):
            rows = q_values(
                conn,
                f"SELECT DISTINCT application_no FROM loan WHERE application_no IN ({_ph})",
                _part,
                "application_no",
            )
            return {str(x) for x in rows}

        tgt, part_out = _target_conn_retry(cfg, tgt, _query)
        out.update(part_out)
        if chunk_idx == 1 or chunk_idx % 20 == 0 or chunk_idx == total_chunks:
            repair_log(f"validate loan target lookup chunk={chunk_idx}/{total_chunks} matched={len(out)}")
    return out


def target_application_keys(
    cfg: Dict[str, Any],
    tgt,
    keys: Sequence[Tuple[str, int, str]],
    chunk_size: int = TARGET_LOOKUP_CHUNK,
) -> Tuple[Any, set]:
    vals = sorted({(str(mobile), int(group_user_id), str(sn)) for mobile, group_user_id, sn in keys})
    if not vals:
        return tgt, set()
    out = set()
    chunk_size = max(1, min(int(chunk_size or TARGET_LOOKUP_CHUNK), 50))
    total_chunks = (len(vals) + chunk_size - 1) // chunk_size
    for chunk_idx, i in enumerate(range(0, len(vals), chunk_size), start=1):
        part = vals[i:i + chunk_size]
        wanted = set(part)
        sns = sorted({sn for _, _, sn in part})
        ph = ",".join(["%s"] * len(sns))
        tgt, rows = _target_conn_retry(
            cfg,
            tgt,
            lambda c, sql=f"SELECT mobile, group_user_id, sn FROM application WHERE sn IN ({ph})", params=sns: q_rows(c, sql, params),
        )
        out.update(
            key for key in (
                (str(r["mobile"]), int(r["group_user_id"]), str(r["sn"]))
                for r in rows
            )
            if key in wanted
        )
        if chunk_idx == 1 or chunk_idx % 20 == 0 or chunk_idx == total_chunks:
            repair_log(f"validate app target lookup chunk={chunk_idx}/{total_chunks} matched={len(out)}")
    return tgt, out


def build_application_validation_rows(cfg: Dict[str, Any], src, lo: int, hi: int) -> Tuple[Dict[int, str], Dict[str, Tuple[str, int, str]], Dict[str, set]]:
    raw_rows = select_application_validation_source_rows(src, lo, hi)
    if not raw_rows:
        return {}, {}, {}
    user_ids = [int(r["user_id"]) for r in raw_rows]
    bvn_map = mig._fetch_bvn_map_from_source(src, user_ids)
    vt = mig.VtTokenResolver(
        src,
        enabled=cfg.get("vt_token_enable", True),
        chunk=cfg.get("vt_token_chunk", 2000),
        vt_db=cfg.get("vt_token_db", "ng_loan_market"),
    )
    mig._register_app_batch_vt(vt, raw_rows, bvn_map)
    vt.prefetch()
    app_rows = mig._build_application_rows(raw_rows, bvn_map, {}, vt)
    source_map = {
        int(r["source_app_id"]): str(r["application_no"])
        for r in raw_rows
        if r.get("source_app_id") is not None
    }
    app_keys_by_no = {
        str(r["application_no"]): (str(r["mobile"]), int(r["group_user_id"]), str(r["sn"]))
        for r in app_rows
    }
    mapping_by_app_id: Dict[Tuple[str, int, str, str], set] = {}
    app_no_to_id = {app_no: app_id for app_id, app_no in source_map.items()}
    included_nos = set(app_keys_by_no)
    for raw_row in raw_rows:
        app_no = str(raw_row.get("application_no") or "")
        if app_no not in included_nos:
            continue
        source_app_id = app_no_to_id.get(app_no)
        if source_app_id is None:
            continue
        for row in mig._build_id_mapping_rows([raw_row], bvn_map, vt):
            key = (str(row["id"]), int(row["app_id"]), str(row["mapping_id"]), str(row["type"]))
            mapping_by_app_id.setdefault(key, set()).add(int(source_app_id))
    return source_map, app_keys_by_no, mapping_by_app_id


def validate_application_missing(
    cfg: Dict[str, Any],
    src,
    tgt,
    max_app_id: int,
    batch_size: int,
) -> Dict[str, List[Any]]:
    out: Dict[str, List[Any]] = {
        "application_ids": [],
        "loan_app_nos": [],
        "id_mapping": [],
        "_repair_application_ids": [],
    }
    scanned = 0
    t0 = time.perf_counter()
    for lo, hi in chunk_ranges(0, max_app_id, batch_size):
        for attempt in range(3):
            try:
                src_map, app_keys_by_no, src_mapping_by_app_id = build_application_validation_rows(cfg, src, lo, hi)
                src_nos = list(src_map.values())
                repair_log(f"validate app range=({lo},{hi}] source_rows={len(src_nos)}")
                lookup_chunk = int(cfg.get("repair_lookup_chunk", TARGET_LOOKUP_CHUNK))
                mapping_chunk = int(cfg.get("repair_mapping_chunk", TARGET_MAPPING_CHUNK))
                tgt, tgt_app_keys = target_application_keys(cfg, tgt, list(app_keys_by_no.values()), lookup_chunk)
                tgt_apps = {app_no for app_no, key in app_keys_by_no.items() if key in tgt_app_keys}
                missing_nos = {no for no in src_nos if no not in tgt_apps}
                out["application_ids"].extend(
                    app_id for app_id, app_no in src_map.items() if app_no in missing_nos
                )
                tgt_loans = target_loan_app_nos(tgt, [no for no in src_nos if no in tgt_apps], lookup_chunk)
                missing_loan_nos = sorted(no for no in src_nos if no in tgt_apps and no not in tgt_loans)
                out["loan_app_nos"].extend(missing_loan_nos)
                src_mapping = list(src_mapping_by_app_id.keys())
                repair_log(f"validate app range=({lo},{hi}] id_mapping_expected={len(src_mapping)}")
                tgt, tgt_mapping = target_id_mapping_keys(cfg, tgt, src_mapping, mapping_chunk)
                missing_mapping = missing_tuple_keys(src_mapping, tgt_mapping)
                out["id_mapping"].extend(missing_mapping)
                repair_ids = set(app_id for app_id, app_no in src_map.items() if app_no in missing_nos)
                repair_ids.update(
                    app_id for app_id, app_no in src_map.items()
                    if app_no in missing_loan_nos
                )
                for key in missing_mapping:
                    repair_ids.update(src_mapping_by_app_id.get(key, set()))
                out["_repair_application_ids"].extend(sorted(repair_ids))
                scanned += len(src_map)
                print(
                    f"validate app scanned={scanned} range=({lo},{hi}] "
                    f"missing_app={len(out['application_ids'])} missing_loan={len(out['loan_app_nos'])} "
                    f"missing_mapping={len(out['id_mapping'])} "
                    f"repair_app={len(set(out['_repair_application_ids']))} "
                    f"elapsed={time.perf_counter() - t0:.1f}s",
                    flush=True,
                )
                break
            except Exception as exc:
                repair_log(
                    f"validate app range failed ({lo},{hi}] attempt={attempt + 1}/3 "
                    f"{type(exc).__name__}: {exc}"
                )
                if attempt >= 2:
                    raise
                mig._close_mysql_conn(tgt)
                tgt = mig.connect_target(cfg)
                mig._session_opts(tgt)
                time.sleep(1 + attempt)
    return out


def source_id_mapping_keys(
    cfg: Dict[str, Any],
    src,
    lo: int,
    hi: int,
) -> List[Tuple[str, int, str, str]]:
    raw_rows = mig._select_application_source_rows(src, lo, hi)
    if not raw_rows:
        return []
    user_ids = [int(r["user_id"]) for r in raw_rows]
    bvn_map = mig._fetch_bvn_map_from_source(src, user_ids)
    vt = mig.VtTokenResolver(
        src,
        enabled=cfg.get("vt_token_enable", True),
        chunk=cfg.get("vt_token_chunk", 2000),
        vt_db=cfg.get("vt_token_db", "ng_loan_market"),
    )
    mig._register_app_batch_vt(vt, raw_rows, bvn_map)
    vt.prefetch()
    rows = mig._build_id_mapping_rows(raw_rows, bvn_map, vt)
    return [
        (str(r["id"]), int(r["app_id"]), str(r["mapping_id"]), str(r["type"]))
        for r in rows
    ]


def source_id_mapping_key_app_ids(
    cfg: Dict[str, Any],
    src,
    lo: int,
    hi: int,
    app_no_to_id: Dict[str, int],
) -> Dict[Tuple[str, int, str, str], set]:
    raw_rows = mig._select_application_source_rows(src, lo, hi)
    if not raw_rows:
        return {}
    user_ids = [int(r["user_id"]) for r in raw_rows]
    bvn_map = mig._fetch_bvn_map_from_source(src, user_ids)
    vt = mig.VtTokenResolver(
        src,
        enabled=cfg.get("vt_token_enable", True),
        chunk=cfg.get("vt_token_chunk", 2000),
        vt_db=cfg.get("vt_token_db", "ng_loan_market"),
    )
    mig._register_app_batch_vt(vt, raw_rows, bvn_map)
    vt.prefetch()
    out: Dict[Tuple[str, int, str, str], set] = {}
    for raw_row in raw_rows:
        source_app_id = app_no_to_id.get(str(raw_row.get("application_no") or ""))
        if source_app_id is None:
            continue
        for row in mig._build_id_mapping_rows([raw_row], bvn_map, vt):
            key = (str(row["id"]), int(row["app_id"]), str(row["mapping_id"]), str(row["type"]))
            out.setdefault(key, set()).add(int(source_app_id))
    return out


def _target_id_mapping_keys_part(
    cfg: Dict[str, Any],
    tgt,
    part: Sequence[Tuple[str, int, str, str]],
) -> Tuple[Any, List[Tuple[str, int, str, str]]]:
    wanted = {(str(anchor), int(app_id), str(mapping_id), str(typ)) for anchor, app_id, mapping_id, typ in part}
    anchors = sorted({anchor for anchor, _, _, _ in wanted})
    ph = ",".join(["%s"] * len(anchors))
    tgt, rows = _target_conn_retry(
        cfg,
        tgt,
        lambda c, sql=f"SELECT id, app_id, mapping_id, type FROM id_mapping WHERE id IN ({ph})", params=anchors: q_rows(c, sql, params),
    )
    found = [
        key for key in (
            (str(r["id"]), int(r["app_id"]), str(r["mapping_id"]), str(r["type"]))
            for r in rows
        )
        if key in wanted
    ]
    return tgt, found


def target_id_mapping_keys(
    cfg: Dict[str, Any],
    tgt,
    expected_keys: Sequence[Tuple[str, int, str, str]],
    chunk_size: int = TARGET_MAPPING_CHUNK,
) -> Tuple[Any, List[Tuple[str, int, str, str]]]:
    if not expected_keys:
        return tgt, []
    out: List[Tuple[str, int, str, str]] = []
    chunk_size = max(1, min(int(chunk_size or TARGET_MAPPING_CHUNK), 100))
    for i in range(0, len(expected_keys), chunk_size):
        stack = [list(expected_keys[i:i + chunk_size])]
        while stack:
            part = stack.pop()
            if not part:
                continue
            try:
                tgt, found = _target_id_mapping_keys_part(cfg, tgt, part)
                out.extend(found)
            except Exception:
                if len(part) <= 1:
                    raise
                mid = len(part) // 2
                stack.append(part[mid:])
                stack.append(part[:mid])
    return tgt, out


def target_exists_user(tgt, user_id: int) -> bool:
    return bool(q_int(tgt, "SELECT COUNT(*) AS c FROM `user` WHERE user_id=%s", (user_id,)))


def target_exists_user_info(tgt, user_id: int) -> bool:
    return bool(q_int(tgt, "SELECT COUNT(*) AS c FROM user_info WHERE user_id=%s", (user_id,)))


def target_existing_user_ids(tgt, user_ids: Sequence[int]) -> set:
    vals = sorted({int(x) for x in user_ids if x is not None})
    out = set()
    for i in range(0, len(vals), 5000):
        part = vals[i:i + 5000]
        if not part:
            continue
        ph = ",".join(["%s"] * len(part))
        out.update(int(x) for x in q_values(
            tgt,
            f"SELECT user_id FROM `user` WHERE user_id IN ({ph})",
            part,
            "user_id",
        ))
    return out


def target_existing_user_info_ids(tgt, user_ids: Sequence[int]) -> set:
    vals = sorted({int(x) for x in user_ids if x is not None})
    out = set()
    for i in range(0, len(vals), 5000):
        part = vals[i:i + 5000]
        if not part:
            continue
        ph = ",".join(["%s"] * len(part))
        out.update(int(x) for x in q_values(
            tgt,
            f"SELECT user_id FROM user_info WHERE user_id IN ({ph})",
            part,
            "user_id",
        ))
    return out


def target_exists_user_bankcard(tgt, user_id: int) -> bool:
    return bool(q_int(tgt, "SELECT COUNT(*) AS c FROM user_bankcard WHERE group_user_id=%s", (user_id,)))


def target_existing_bankcard_user_ids(tgt, user_ids: Sequence[int]) -> set:
    vals = sorted({int(x) for x in user_ids if x is not None})
    out = set()
    for i in range(0, len(vals), 5000):
        part = vals[i:i + 5000]
        if not part:
            continue
        ph = ",".join(["%s"] * len(part))
        out.update(int(x) for x in q_values(
            tgt,
            f"SELECT DISTINCT group_user_id FROM user_bankcard WHERE group_user_id IN ({ph})",
            part,
            "group_user_id",
        ))
    return out


def target_exists_user_product(tgt, user_id: int, product_id: str) -> bool:
    return bool(q_int(
        tgt,
        "SELECT COUNT(*) AS c FROM user_product WHERE group_user_id=%s AND product_id=%s",
        (user_id, str(product_id)),
    ))


def target_existing_user_product_keys(tgt, keys: Sequence[Tuple[int, str]]) -> set:
    vals = sorted({(int(user_id), str(product_id)) for user_id, product_id in keys})
    out = set()
    for i in range(0, len(vals), 1000):
        part = vals[i:i + 1000]
        if not part:
            continue
        tuple_ph = ",".join(["(%s,%s)"] * len(part))
        params: List[Any] = []
        for user_id, product_id in part:
            params.extend([user_id, product_id])
        rows = q_rows(
            tgt,
            "SELECT group_user_id, product_id FROM user_product "
            f"WHERE (group_user_id, product_id) IN ({tuple_ph})",
            params,
        )
        out.update((int(r["group_user_id"]), str(r["product_id"])) for r in rows)
    return out


def _target_rows_by_key_chunk_sql(
    table: str,
    key_cols: Sequence[str],
    columns: Sequence[str],
    part: Sequence[dict],
) -> Tuple[str, List[Any]]:
    cols_sql = ", ".join(_quote_ident(c) for c in columns)
    if len(key_cols) == 1:
        col = key_cols[0]
        keys = [row.get(col) for row in part]
        ph = ",".join(["%s"] * len(keys))
        sql = f"SELECT {cols_sql} FROM {_quote_ident(table)} WHERE {_quote_ident(col)} IN ({ph})"
        return sql, list(keys)
    cols_expr = ", ".join(_quote_ident(c) for c in key_cols)
    tuple_ph = []
    params: List[Any] = []
    for row in part:
        tuple_ph.append(f"({','.join(['%s'] * len(key_cols))})")
        params.extend(row.get(c) for c in key_cols)
    sql = f"SELECT {cols_sql} FROM {_quote_ident(table)} WHERE ({cols_expr}) IN ({','.join(tuple_ph)})"
    return sql, params


def target_rows_by_key(
    tgt,
    table: str,
    key_cols: Sequence[str],
    rows: Sequence[dict],
    columns: Sequence[str],
    chunk_size: int = FIELD_DIFF_LOOKUP_CHUNK,
    cfg: Optional[Dict[str, Any]] = None,
    log_label: str = "",
) -> Dict[Tuple[Any, ...], dict]:
    if not rows:
        return {}
    out: Dict[Tuple[Any, ...], dict] = {}
    chunk_size = max(1, int(chunk_size or FIELD_DIFF_LOOKUP_CHUNK))
    total_chunks = (len(rows) + chunk_size - 1) // chunk_size
    fetch_t0 = time.time()
    for chunk_idx, i in enumerate(range(0, len(rows), chunk_size), start=1):
        chunk_t0 = time.time()
        part = list(rows[i:i + chunk_size])
        sql, params = _target_rows_by_key_chunk_sql(table, key_cols, columns, part)

        def _query(conn, _sql=sql, _params=params):
            return q_rows(conn, _sql, _params)

        if cfg is not None:
            tgt, got = _target_conn_retry(cfg, tgt, _query)
        else:
            got = _query(tgt)
        for row in got:
            out[tuple(row.get(c) for c in key_cols)] = row
        if log_label and (chunk_idx == 1 or chunk_idx % 20 == 0 or chunk_idx == total_chunks):
            repair_log(
                f"field diff fetch {log_label} chunk={chunk_idx}/{total_chunks} "
                f"rows={len(part)} fetched={len(got)} total={len(out)} "
                f"chunk_elapsed={time.time() - chunk_t0:.1f}s total_elapsed={time.time() - fetch_t0:.1f}s"
            )
    return out


def update_target_field(tgt, table: str, key_cols: Sequence[str], key_values: Sequence[Any], column: str, value: Any) -> int:
    where = " AND ".join(f"{_quote_ident(c)}=%s" for c in key_cols)
    sql = f"UPDATE {_quote_ident(table)} SET {_quote_ident(column)}=%s WHERE {where}"
    with tgt.cursor() as cur:
        cur.execute(sql, [value, *key_values])
        return int(cur.rowcount or 0)


def _prepare_expected_user_rows_for_ids(cfg: Dict[str, Any], src, user_ids: Sequence[int]) -> Tuple[List[dict], List[dict], List[dict], List[dict]]:
    ids = sorted({int(x) for x in user_ids if x is not None})
    if not ids:
        return [], [], [], []
    rows_user: List[dict] = []
    for lo, hi in compact_ids_to_ranges(ids):
        rows_user.extend(mig._select_user_batch_rows(src, lo, hi))
    wanted = set(ids)
    rows_user = [row for row in rows_user if int(row["user_id"]) in wanted]
    if not rows_user:
        return [], [], [], []
    keys = mig._extract_user_batch_keys(rows_user)
    ud_rows = mig._select_ud_rows_by_user_ids(src, keys["user_ids"])
    lup_rows = mig._fetch_lup_by_app_mobile(src, keys["app_mobile_pairs"], int(cfg.get("lup_pair_chunk", 400)))
    uri_rows = mig._fetch_uri_by_user_ids(src, keys["user_ids"])
    dac_rows = mig._fetch_dac_by_device_ids(src, keys["device_ids"])
    product_rows = _source_user_product_rows_for_keys(src, source_user_product_keys_for_user_ids(src, ids))
    lookups = mig._make_user_lookups(ud_rows, lup_rows, uri_rows, dac_rows, product_rows)
    vt = mig.VtTokenResolver(
        src,
        enabled=cfg.get("vt_token_enable", True),
        chunk=cfg.get("vt_token_chunk", 2000),
        vt_db=cfg.get("vt_token_db", "ng_loan_market"),
    )
    mig._register_user_batch_vt(vt, rows_user, lookups)
    vt.prefetch()
    rows_user_ok: List[dict] = []
    for row in rows_user:
        token = vt.resolve_token(
            mig.VtTokenResolver.VT_MOBILE,
            row.get("mobile"),
            context=f"user_id={row['user_id']} field=mobile",
            row_data=row,
            log_miss=False,
        )
        if not token:
            continue
        row = dict(row)
        row["mobile"] = token
        rows_user_ok.append(row)
    mig._prepare_user_insert_rows(rows_user_ok, lookups)
    ok_user_ids = {int(row["user_id"]) for row in rows_user_ok}
    rows_info = mig._build_user_info_rows(rows_user_ok, lookups, vt)
    rows_bankcard = mig._build_bankcard_rows(lookups, vt, ok_user_ids or None, cfg)
    rows_product = mig._build_user_product_rows(product_rows)
    return rows_user_ok, rows_info, rows_bankcard, rows_product


def source_user_product_keys_for_user_ids(src, user_ids: Sequence[int]) -> List[Tuple[int, str]]:
    vals = sorted({int(x) for x in user_ids if x is not None})
    out: List[Tuple[int, str]] = []
    for i in range(0, len(vals), 1000):
        part = vals[i:i + 1000]
        if not part:
            continue
        ph = ",".join(["%s"] * len(part))
        rows = q_rows(
            src,
            f"""
            SELECT userId AS user_id, productId AS product_id
            FROM ng_loan_market.application
            WHERE userId IN ({ph})
              AND productId IS NOT NULL AND productId <> 0
            GROUP BY userId, productId
            """,
            part,
        )
        out.extend((int(r["user_id"]), str(r["product_id"])) for r in rows)
    return out


def _source_user_product_rows_for_keys(src, keys: Sequence[Tuple[int, str]]) -> List[dict]:
    vals = sorted({(int(user_id), str(product_id)) for user_id, product_id in keys})
    out: List[dict] = []
    for i in range(0, len(vals), 500):
        part = vals[i:i + 500]
        if not part:
            continue
        tuple_ph = ",".join(["(%s,%s)"] * len(part))
        params: List[Any] = []
        for user_id, product_id in part:
            params.extend([user_id, product_id])
        rows = q_rows(
            src,
            f"""
            SELECT pick.userId AS userId, pick.productId AS productId, a.amount
            FROM (
                SELECT userId, productId, MAX(id) AS max_id
                FROM ng_loan_market.application
                WHERE (userId, productId) IN ({tuple_ph})
                  AND productId IS NOT NULL AND productId <> 0
                GROUP BY userId, productId
            ) pick
            INNER JOIN ng_loan_market.application a ON a.id = pick.max_id
            """,
            params,
        )
        out.extend(rows)
    return out


def _expected_application_related_rows_for_ids(
    cfg: Dict[str, Any],
    src,
    app_ids: Sequence[int],
) -> Tuple[List[dict], List[dict], List[dict]]:
    ids = sorted({int(x) for x in app_ids if x is not None})
    if not ids:
        return [], [], []
    raw_rows: List[dict] = []
    wanted = set(ids)
    app_no_to_id = source_app_ids_to_nos(src, ids)
    wanted_nos = set(app_no_to_id.values())
    for lo, hi in compact_ids_to_ranges(ids):
        raw_rows.extend(
            row for row in mig._select_application_source_rows(src, lo, hi)
            if str(row.get("application_no") or "") in wanted_nos
        )
    if not raw_rows:
        for lo, hi in compact_ids_to_ranges(ids):
            raw_rows.extend(
                row for row in mig._select_application_source_rows(src, lo, hi)
                if str(row.get("application_no") or "") in wanted_nos
            )
    if not raw_rows:
        return [], [], []
    user_ids = [int(r["user_id"]) for r in raw_rows]
    sns = [r["sn"] for r in raw_rows if r.get("sn")]
    bvn_map, repay_map = mig._fetch_app_lookup_maps(cfg, src, user_ids, sns)
    vt = mig.VtTokenResolver(
        src,
        enabled=cfg.get("vt_token_enable", True),
        chunk=cfg.get("vt_token_chunk", 2000),
        vt_db=cfg.get("vt_token_db", "ng_loan_market"),
    )
    mig._register_app_batch_vt(vt, raw_rows, bvn_map)
    vt.prefetch()
    app_rows = mig._build_application_rows(raw_rows, bvn_map, repay_map, vt)
    included = {row["application_no"] for row in app_rows}
    sn_to_app_no = {
        str(r["core_sn"]): r["application_no"]
        for r in raw_rows
        if r.get("core_sn") and r["application_no"] in included
    }
    loan_rows = mig._fetch_loan_rows_from_source(src, sn_to_app_no)
    map_src = [r for r in raw_rows if r["application_no"] in included]
    mapping_rows = mig._build_id_mapping_rows(map_src, bvn_map, vt)
    return app_rows, loan_rows, mapping_rows


def _load_application_batch_source(
    cfg: Dict[str, Any],
    src,
    app_ids: Sequence[int],
) -> Tuple[Dict[int, str], Dict[str, Tuple[str, int, str]], Dict[Tuple[str, int, str, str], set], List[dict], List[dict], List[dict]]:
    """Load application validation indexes and field-diff rows from source in one pass."""
    ids = sorted({int(x) for x in app_ids if x is not None})
    if not ids:
        return {}, {}, {}, [], [], []
    wanted = set(ids)
    app_no_to_id = source_app_ids_to_nos(src, ids)
    wanted_nos = {str(v) for v in app_no_to_id.values() if v}
    id_by_no = {str(app_no): app_id for app_id, app_no in app_no_to_id.items() if app_no}
    raw_rows: List[dict] = []
    for lo, hi in compact_ids_to_ranges(ids):
        for row in mig._select_application_source_rows(src, lo, hi):
            app_no = str(row.get("application_no") or "")
            if app_no not in wanted_nos:
                continue
            row = dict(row)
            row["source_app_id"] = id_by_no.get(app_no)
            raw_rows.append(row)
    if not raw_rows:
        return {}, {}, {}, [], [], []
    user_ids = [int(r["user_id"]) for r in raw_rows]
    sns = [r["sn"] for r in raw_rows if r.get("sn")]
    bvn_map, repay_map = mig._fetch_app_lookup_maps(cfg, src, user_ids, sns)
    vt = mig.VtTokenResolver(
        src,
        enabled=cfg.get("vt_token_enable", True),
        chunk=cfg.get("vt_token_chunk", 2000),
        vt_db=cfg.get("vt_token_db", "ng_loan_market"),
    )
    mig._register_app_batch_vt(vt, raw_rows, bvn_map)
    vt.prefetch()
    app_rows = mig._build_application_rows(raw_rows, bvn_map, repay_map, vt)
    source_map = {
        int(r["source_app_id"]): str(r["application_no"])
        for r in raw_rows
        if r.get("source_app_id") is not None
    }
    app_keys_by_no = {
        str(r["application_no"]): (str(r["mobile"]), int(r["group_user_id"]), str(r["sn"]))
        for r in app_rows
    }
    mapping_by_app_id: Dict[Tuple[str, int, str, str], set] = {}
    app_no_to_source_id = {app_no: app_id for app_id, app_no in source_map.items()}
    included_nos = set(app_keys_by_no)
    for raw_row in raw_rows:
        app_no = str(raw_row.get("application_no") or "")
        if app_no not in included_nos:
            continue
        source_app_id = app_no_to_source_id.get(app_no)
        if source_app_id is None:
            continue
        for row in mig._build_id_mapping_rows([raw_row], bvn_map, vt):
            key = (str(row["id"]), int(row["app_id"]), str(row["mapping_id"]), str(row["type"]))
            mapping_by_app_id.setdefault(key, set()).add(int(source_app_id))
    included = {row["application_no"] for row in app_rows}
    sn_to_app_no = {
        str(r["core_sn"]): r["application_no"]
        for r in raw_rows
        if r.get("core_sn") and r["application_no"] in included
    }
    loan_rows = mig._fetch_loan_rows_from_source(src, sn_to_app_no)
    map_src = [r for r in raw_rows if r["application_no"] in included]
    mapping_rows = mig._build_id_mapping_rows(map_src, bvn_map, vt)
    return source_map, app_keys_by_no, mapping_by_app_id, app_rows, loan_rows, mapping_rows


def load_window_source_cache(cfg: Dict[str, Any], src, window: DateWindow) -> WindowSourceCache:
    """Preload one-month window source rows into memory in batches."""
    cache = WindowSourceCache(window=window)
    batch_size = max(1, int(cfg.get("app_validate_batch", APP_VALIDATE_BATCH)))
    total_t0 = time.time()
    repair_log(
        f"window cache start range=[{window.start_sql},{window.end_sql}) batch_size={batch_size}"
    )

    id_t0 = time.time()
    cache.user_ids = source_user_ids_by_created_window(src, window)
    cache.bankcard_user_ids = source_bankcard_user_ids_by_window(src, window)
    cache.product_keys = source_user_product_keys_by_apply_date_window(src, window)
    cache.app_ids_by_apply = source_application_ids_by_apply_date_window(src, window)
    cache.loan_app_ids = source_application_ids_by_repay_plan_created_window(src, window)
    cache.mapping_app_ids = source_application_ids_by_created_window(src, window)
    cache.all_app_ids = sorted({
        int(x)
        for x in (cache.app_ids_by_apply + cache.loan_app_ids + cache.mapping_app_ids)
        if x is not None
    })
    product_user_ids = sorted({user_id for user_id, _ in cache.product_keys})
    all_prepare_ids = sorted(set(cache.user_ids) | set(cache.bankcard_user_ids) | set(product_user_ids))
    repair_log(
        f"window cache ids elapsed={time.time() - id_t0:.1f}s "
        f"users={len(cache.user_ids)} bankcard={len(cache.bankcard_user_ids)} "
        f"product_keys={len(cache.product_keys)} apps={len(cache.all_app_ids)} "
        f"prepare_ids={len(all_prepare_ids)}"
    )

    user_batches = max(1, (len(all_prepare_ids) + batch_size - 1) // batch_size) if all_prepare_ids else 0
    user_t0 = time.time()
    for batch_no, i in enumerate(range(0, len(all_prepare_ids), batch_size), start=1):
        batch_ids = all_prepare_ids[i:i + batch_size]
        batch_t0 = time.time()
        rows_user, rows_info, rows_bankcard, _ = _prepare_expected_user_rows_for_ids(cfg, src, batch_ids)
        cache.rows_user.extend(rows_user)
        cache.rows_info.extend(rows_info)
        cache.rows_bankcard.extend(rows_bankcard)
        repair_log(
            f"window cache user batch {batch_no}/{user_batches} ids={len(batch_ids)} "
            f"user={len(rows_user)} info={len(rows_info)} bankcard={len(rows_bankcard)} "
            f"elapsed={time.time() - batch_t0:.1f}s"
        )
    repair_log(
        f"window cache user done rows_user={len(cache.rows_user)} rows_info={len(cache.rows_info)} "
        f"rows_bankcard={len(cache.rows_bankcard)} elapsed={time.time() - user_t0:.1f}s"
    )

    product_batches = max(1, (len(cache.product_keys) + batch_size - 1) // batch_size) if cache.product_keys else 0
    product_t0 = time.time()
    for batch_no, i in enumerate(range(0, len(cache.product_keys), batch_size), start=1):
        batch_keys = cache.product_keys[i:i + batch_size]
        batch_t0 = time.time()
        product_rows = mig._build_user_product_rows(_source_user_product_rows_for_keys(src, batch_keys))
        cache.product_rows.extend(product_rows)
        repair_log(
            f"window cache user_product batch {batch_no}/{product_batches} keys={len(batch_keys)} "
            f"rows={len(product_rows)} elapsed={time.time() - batch_t0:.1f}s"
        )
    repair_log(f"window cache user_product done rows={len(cache.product_rows)} elapsed={time.time() - product_t0:.1f}s")

    app_batches = max(1, (len(cache.all_app_ids) + batch_size - 1) // batch_size) if cache.all_app_ids else 0
    app_t0 = time.time()
    for batch_no, i in enumerate(range(0, len(cache.all_app_ids), batch_size), start=1):
        batch_ids = cache.all_app_ids[i:i + batch_size]
        batch_t0 = time.time()
        src_map, app_keys, mapping_by_app_id, app_rows, loan_rows, mapping_rows = _load_application_batch_source(
            cfg, src, batch_ids,
        )
        cache.src_map.update(src_map)
        cache.app_keys_by_no.update(app_keys)
        for key, app_ids_for_key in mapping_by_app_id.items():
            cache.src_mapping_by_app_id.setdefault(key, set()).update(app_ids_for_key)
        cache.app_rows.extend(app_rows)
        cache.loan_rows.extend(loan_rows)
        cache.mapping_rows.extend(mapping_rows)
        repair_log(
            f"window cache application batch {batch_no}/{app_batches} ids={len(batch_ids)} "
            f"apps={len(app_rows)} loans={len(loan_rows)} mappings={len(mapping_rows)} "
            f"elapsed={time.time() - batch_t0:.1f}s"
        )
    repair_log(
        f"window cache application done apps={len(cache.app_rows)} loans={len(cache.loan_rows)} "
        f"mappings={len(cache.mapping_rows)} elapsed={time.time() - app_t0:.1f}s"
    )
    repair_log(
        f"window cache ready total_elapsed={time.time() - total_t0:.1f}s "
        f"users={len(cache.rows_user)} products={len(cache.product_rows)} apps={len(cache.app_rows)}"
    )
    return cache


def _slice_application_source_for_ids(
    src_map: Dict[int, str],
    app_keys_by_no: Dict[str, Tuple[str, int, str]],
    src_mapping_by_app_id: Dict[Tuple[str, int, str, str], set],
    batch_ids: set,
) -> Tuple[Dict[int, str], Dict[str, Tuple[str, int, str]], Dict[Tuple[str, int, str, str], set]]:
    batch_ids = {int(x) for x in batch_ids}
    part_src_map = {app_id: app_no for app_id, app_no in src_map.items() if app_id in batch_ids}
    app_id_by_no = {app_no: app_id for app_id, app_no in src_map.items()}
    part_keys = {
        app_no: key
        for app_no, key in app_keys_by_no.items()
        if int(app_id_by_no.get(app_no, 0)) in batch_ids
    }
    part_mapping: Dict[Tuple[str, int, str, str], set] = {}
    for key, app_ids_for_key in src_mapping_by_app_id.items():
        kept = {int(app_id) for app_id in app_ids_for_key if int(app_id) in batch_ids}
        if kept:
            part_mapping[key] = kept
    return part_src_map, part_keys, part_mapping


def _build_field_diff_skip_context(
    user_missing: Optional[Dict[str, List[Any]]],
    app_missing: Optional[Dict[str, List[Any]]],
    src,
    cache: Optional[WindowSourceCache] = None,
) -> Dict[str, Any]:
    user_missing = user_missing or {}
    app_missing = app_missing or {}
    ctx: Dict[str, Any] = {
        "user_ids": {int(x) for x in user_missing.get("user", []) if x is not None},
        "user_info_ids": {int(x) for x in user_missing.get("user_info", []) if x is not None},
        "bankcard_ids": {int(x) for x in user_missing.get("user_bankcard", []) if x is not None},
        "product_keys": {
            (int(user_id), str(product_id))
            for user_id, product_id in user_missing.get("user_product", [])
        },
        "app_nos": set(),
        "loan_nos": {str(x) for x in app_missing.get("loan_app_nos", []) if x},
        "mapping_keys": set(app_missing.get("id_mapping", [])),
    }
    missing_app_ids = [int(x) for x in app_missing.get("application_ids", []) if x is not None]
    if missing_app_ids:
        if cache is not None:
            ctx["app_nos"] = {str(cache.src_map[aid]) for aid in missing_app_ids if aid in cache.src_map}
        else:
            ctx["app_nos"] = {str(v) for v in source_app_ids_to_nos(src, missing_app_ids).values() if v}
    return ctx


def collect_field_diffs_for_window(
    cfg: Dict[str, Any],
    src,
    tgt,
    window: DateWindow,
    table_sel: set,
    chunk_size: int = FIELD_DIFF_LOOKUP_CHUNK,
    user_missing: Optional[Dict[str, List[Any]]] = None,
    app_missing: Optional[Dict[str, List[Any]]] = None,
    cache: Optional[WindowSourceCache] = None,
) -> List[FieldDiff]:
    diffs: List[FieldDiff] = []
    do_user = "all" in table_sel or "user" in table_sel
    do_app = "all" in table_sel or "application" in table_sel
    lookup_kw = {"cfg": cfg, "chunk_size": chunk_size}
    skip = _build_field_diff_skip_context(user_missing, app_missing, src, cache=cache)
    batch_size = max(1, int(cfg.get("app_validate_batch", APP_VALIDATE_BATCH)))
    if do_user:
        repair_log(
            f"field diff user phase start chunk_size={chunk_size} batch_size={batch_size} "
            f"skip_user={len(skip['user_ids'])} skip_info={len(skip['user_info_ids'])} "
            f"skip_bankcard={len(skip['bankcard_ids'])} skip_product={len(skip['product_keys'])} "
            f"source={'cache' if cache is not None else 'live'}"
        )
        if cache is not None:
            rows_user_all = list(cache.rows_user)
            rows_info_all = list(cache.rows_info)
            rows_bankcard_all = list(cache.rows_bankcard)
            product_keys = list(cache.product_keys)
            repair_log(
                f"field diff user cache rows_user={len(rows_user_all)} rows_info={len(rows_info_all)} "
                f"rows_bankcard={len(rows_bankcard_all)} product_keys={len(product_keys)}"
            )
        else:
            win_t0 = time.time()
            user_ids = sorted(set(source_user_ids_by_created_window(src, window)) | set(source_bankcard_user_ids_by_window(src, window)))
            product_keys = source_user_product_keys_by_apply_date_window(src, window)
            product_user_ids = sorted({user_id for user_id, _ in product_keys})
            all_prepare_ids = sorted(set(user_ids) | set(product_user_ids))
            repair_log(
                f"field diff user window ids elapsed={time.time() - win_t0:.1f}s "
                f"users={len(user_ids)} product_keys={len(product_keys)} prepare_ids={len(all_prepare_ids)}"
            )
            rows_user_all = []
            rows_info_all = []
            rows_bankcard_all = []
            user_batches = max(1, (len(all_prepare_ids) + batch_size - 1) // batch_size) if all_prepare_ids else 0
            for batch_no, i in enumerate(range(0, len(all_prepare_ids), batch_size), start=1):
                batch_ids = all_prepare_ids[i:i + batch_size]
                src_t0 = time.time()
                rows_user, rows_info, rows_bankcard, _rows_product_for_users = _prepare_expected_user_rows_for_ids(cfg, src, batch_ids)
                repair_log(
                    f"field diff user src_load batch {batch_no}/{user_batches} ids={len(batch_ids)} "
                    f"user={len(rows_user)} info={len(rows_info)} bankcard={len(rows_bankcard)} "
                    f"elapsed={time.time() - src_t0:.1f}s"
                )
                rows_user_all.extend(rows_user)
                rows_info_all.extend(rows_info)
                rows_bankcard_all.extend(rows_bankcard)
        user_actual: Dict[Tuple[Any, ...], dict] = {}
        info_actual: Dict[Tuple[Any, ...], dict] = {}
        bank_actual: Dict[Tuple[Any, ...], dict] = {}
        user_fetch_batches = max(1, (len(rows_user_all) + batch_size - 1) // batch_size) if rows_user_all else 0
        for batch_no, i in enumerate(range(0, len(rows_user_all), batch_size), start=1):
            rows_user_fetch = [
                r for r in rows_user_all[i:i + batch_size]
                if int(r["user_id"]) not in skip["user_ids"]
            ]
            if rows_user_fetch:
                user_actual.update(target_rows_by_key(
                    tgt, "user", ["user_id"], rows_user_fetch, mig.USER_INSERT_COLS,
                    log_label=f"user/b{batch_no}", **lookup_kw,
                ))
        info_fetch_batches = max(1, (len(rows_info_all) + batch_size - 1) // batch_size) if rows_info_all else 0
        for batch_no, i in enumerate(range(0, len(rows_info_all), batch_size), start=1):
            rows_info_fetch = [
                r for r in rows_info_all[i:i + batch_size]
                if int(r["user_id"]) not in skip["user_info_ids"]
            ]
            if rows_info_fetch:
                info_actual.update(target_rows_by_key(
                    tgt, "user_info", ["user_id"], rows_info_fetch, mig.USER_INFO_COLS,
                    log_label=f"user_info/b{batch_no}", **lookup_kw,
                ))
        bank_fetch_batches = max(1, (len(rows_bankcard_all) + batch_size - 1) // batch_size) if rows_bankcard_all else 0
        for batch_no, i in enumerate(range(0, len(rows_bankcard_all), batch_size), start=1):
            rows_bankcard_fetch = [
                r for r in rows_bankcard_all[i:i + batch_size]
                if int(r["group_user_id"]) not in skip["bankcard_ids"]
            ]
            if rows_bankcard_fetch:
                bank_actual.update(target_rows_by_key(
                    tgt, "user_bankcard", ["group_user_id"], rows_bankcard_fetch, mig.USER_BANKCARD_COLS,
                    log_label=f"user_bankcard/b{batch_no}", **lookup_kw,
                ))
        repair_log(
            f"field diff user fetch done user_batches={user_fetch_batches} "
            f"info_batches={info_fetch_batches} bankcard_batches={bank_fetch_batches}"
        )
        for row in rows_user_all:
            key = (row["user_id"],)
            diffs.extend(_diff_rows("user", str(row["user_id"]), row, user_actual.get(key), [c for c in mig.USER_INSERT_COLS if c != "user_id"]))
        for row in rows_info_all:
            key = (row["user_id"],)
            diffs.extend(_diff_rows("user_info", str(row["user_id"]), row, info_actual.get(key), [c for c in mig.USER_INFO_COLS if c != "user_id"]))
        for row in rows_bankcard_all:
            key = (row["group_user_id"],)
            cols = [c for c in mig.USER_BANKCARD_COLS if c not in ("group_user_id", "id")]
            actual = bank_actual.get(key)
            if actual and int(actual.get("id") or 0) == 0:
                diffs.append(FieldDiff("user_bankcard", str(row["group_user_id"]), "id", row.get("id"), actual.get("id")))
            diffs.extend(_diff_rows("user_bankcard", str(row["group_user_id"]), row, actual, cols))
        product_rows_all: List[dict] = []
        product_actual: Dict[Tuple[Any, ...], dict] = {}
        if cache is not None:
            product_rows_all = list(cache.product_rows)
            repair_log(f"field diff user_product cache rows={len(product_rows_all)}")
        else:
            product_batches = max(1, (len(product_keys) + batch_size - 1) // batch_size) if product_keys else 0
            for batch_no, i in enumerate(range(0, len(product_keys), batch_size), start=1):
                batch_keys = product_keys[i:i + batch_size]
                src_t0 = time.time()
                product_rows = mig._build_user_product_rows(_source_user_product_rows_for_keys(src, batch_keys))
                repair_log(
                    f"field diff user_product src_load batch {batch_no}/{product_batches} keys={len(batch_keys)} "
                    f"rows={len(product_rows)} elapsed={time.time() - src_t0:.1f}s"
                )
                product_rows_all.extend(product_rows)
        product_fetch_batches = max(1, (len(product_rows_all) + batch_size - 1) // batch_size) if product_rows_all else 0
        for batch_no, i in enumerate(range(0, len(product_rows_all), batch_size), start=1):
            product_rows = product_rows_all[i:i + batch_size]
            product_rows_fetch = [
                r for r in product_rows
                if (int(r["group_user_id"]), str(r["product_id"])) not in skip["product_keys"]
            ]
            product_actual.update(target_rows_by_key(
                tgt, "user_product", ["group_user_id", "product_id"], product_rows_fetch, mig.USER_PRODUCT_COLS,
                log_label=f"user_product/b{batch_no}", **lookup_kw,
            ))
        for row in product_rows_all:
            key = (row["group_user_id"], row["product_id"])
            cols = [c for c in mig.USER_PRODUCT_COLS if c not in ("group_user_id", "product_id")]
            diffs.extend(_diff_rows("user_product", f"({row['group_user_id']},{row['product_id']})", row, product_actual.get(key), cols))
        repair_log(f"field diff user phase end diffs={len(diffs)}")
        tgt = _refresh_target_conn(cfg, tgt)
        repair_log("field diff target reconnected before application phase")
    if do_app:
        repair_log(
            f"field diff application phase start chunk_size={chunk_size} batch_size={batch_size} "
            f"skip_app_nos={len(skip['app_nos'])} skip_loan_nos={len(skip['loan_nos'])} "
            f"skip_mapping={len(skip['mapping_keys'])} source={'cache' if cache is not None else 'live'}"
        )
        if cache is not None:
            all_app_ids = list(cache.all_app_ids)
            repair_log(
                f"field diff application cache apps={len(cache.app_rows)} "
                f"loans={len(cache.loan_rows)} mappings={len(cache.mapping_rows)}"
            )
        else:
            app_ids = set(source_application_ids_by_apply_date_window(src, window))
            loan_ids = set(source_application_ids_by_repay_plan_created_window(src, window))
            mapping_ids = set(source_application_ids_by_created_window(src, window))
            all_app_ids = sorted(app_ids | loan_ids | mapping_ids)
        total_batches = max(1, (len(all_app_ids) + batch_size - 1) // batch_size) if all_app_ids else 0
        for batch_no, i in enumerate(range(0, len(all_app_ids), batch_size), start=1):
            batch_ids = set(all_app_ids[i:i + batch_size])
            repair_log(f"field diff application batch {batch_no}/{total_batches} ids={len(batch_ids)}")
            if cache is not None:
                batch_id_set = {int(x) for x in batch_ids}
                part_src_map, _, part_mapping = _slice_application_source_for_ids(
                    cache.src_map, cache.app_keys_by_no, cache.src_mapping_by_app_id, batch_id_set,
                )
                batch_nos = {str(v) for v in part_src_map.values()}
                part_mapping_keys = set(part_mapping.keys())
                app_rows = [r for r in cache.app_rows if str(r["application_no"]) in batch_nos]
                loan_rows = [r for r in cache.loan_rows if str(r["loan_no"]) in batch_nos]
                mapping_rows = [
                    r for r in cache.mapping_rows
                    if (str(r["id"]), int(r["app_id"]), str(r["mapping_id"]), str(r["type"])) in part_mapping_keys
                ]
                repair_log(
                    f"field diff application batch {batch_no}/{total_batches} cache_slice "
                    f"apps={len(app_rows)} loans={len(loan_rows)} mappings={len(mapping_rows)}"
                )
            else:
                src_t0 = time.time()
                app_rows, loan_rows, mapping_rows = _expected_application_related_rows_for_ids(
                    cfg, src, sorted(batch_ids),
                )
                repair_log(
                    f"field diff application batch {batch_no}/{total_batches} src_load "
                    f"apps={len(app_rows)} loans={len(loan_rows)} mappings={len(mapping_rows)} "
                    f"elapsed={time.time() - src_t0:.1f}s"
                )
            app_rows_fetch = [r for r in app_rows if str(r["application_no"]) not in skip["app_nos"]]
            loan_rows_fetch = [r for r in loan_rows if str(r["loan_no"]) not in skip["loan_nos"]]
            mapping_rows_fetch = [
                r for r in mapping_rows
                if (r["id"], r["app_id"], r["mapping_id"], r["type"]) not in skip["mapping_keys"]
            ]
            repair_log(
                f"field diff application batch {batch_no}/{total_batches} fetch "
                f"apps={len(app_rows_fetch)}/{len(app_rows)} "
                f"loans={len(loan_rows_fetch)}/{len(loan_rows)} "
                f"mappings={len(mapping_rows_fetch)}/{len(mapping_rows)}"
            )
            if batch_no > 1 and batch_no % 3 == 1:
                tgt = _refresh_target_conn(cfg, tgt)
            app_actual = target_rows_by_key(
                tgt, "application", ["application_no"], app_rows_fetch, mig.APPLICATION_INSERT_COLS,
                log_label=f"application/b{batch_no}", **lookup_kw,
            )
            loan_actual = target_rows_by_key(
                tgt, "loan", ["loan_no"], loan_rows_fetch, mig.LOAN_INSERT_COLS,
                log_label=f"loan/b{batch_no}", **lookup_kw,
            )
            mapping_actual = target_rows_by_key(
                tgt, "id_mapping", ["id", "app_id", "mapping_id", "type"], mapping_rows_fetch, mig.ID_MAPPING_COLS,
                log_label=f"id_mapping/b{batch_no}", **lookup_kw,
            )
            for row in app_rows:
                key = (row["application_no"],)
                cols = [c for c in mig.APPLICATION_INSERT_COLS if c != "application_no"]
                diffs.extend(_diff_rows("application", str(row["application_no"]), row, app_actual.get(key), cols))
            for row in loan_rows:
                key = (row["loan_no"],)
                cols = [c for c in mig.LOAN_INSERT_COLS if c != "loan_no"]
                diffs.extend(_diff_rows("loan", str(row["loan_no"]), row, loan_actual.get(key), cols))
            for row in mapping_rows:
                key = (row["id"], row["app_id"], row["mapping_id"], row["type"])
                diffs.extend(_diff_rows("id_mapping", "|".join(str(x) for x in key), row, mapping_actual.get(key), ["event_time"]))
            repair_log(f"field diff application batch {batch_no}/{total_batches} end diffs={len(diffs)}")
        repair_log(f"field diff application phase end diffs={len(diffs)}")
    repair_log(f"field diff window total={len(diffs)}")
    return diffs


FIELD_DIFF_KEY_COLS = {
    "user": ("user_id",),
    "user_info": ("user_id",),
    "user_bankcard": ("group_user_id",),
    "user_product": ("group_user_id", "product_id"),
    "application": ("application_no",),
    "loan": ("loan_no",),
    "id_mapping": ("id", "app_id", "mapping_id", "type"),
}


def _field_diff_key_values(diff: FieldDiff) -> Tuple[Any, ...]:
    if diff.scope == "user_product":
        parts = diff.key.strip("()").split(",", 1)
        return (int(parts[0]), parts[1])
    if diff.scope == "id_mapping":
        anchor, app_id, mapping_id, typ = diff.key.split("|", 3)
        return (anchor, int(app_id), mapping_id, typ)
    if diff.scope in ("user", "user_info", "user_bankcard"):
        return (int(diff.key),)
    return (diff.key,)


def apply_field_diffs(tgt, diffs: Sequence[FieldDiff], writer) -> Dict[str, int]:
    counts = {"success": 0, "failed": 0}
    for diff in diffs:
        try:
            key_cols = FIELD_DIFF_KEY_COLS[diff.scope]
            updated = update_target_field(tgt, diff.scope, key_cols, _field_diff_key_values(diff), diff.column, diff.expected)
            tgt.commit()
            status = "success" if updated >= 0 else "failed"
            counts[status] += 1
            writer.writerow({
                "scope": diff.scope,
                "key": diff.key,
                "column": diff.column,
                "status": status,
                "reason": "field_updated",
                "source_value": _json_cell(diff.expected),
                "target_value": _json_cell(diff.actual),
            })
            flush_writer(writer)
        except Exception as exc:
            tgt.rollback()
            counts["failed"] += 1
            writer.writerow({
                "scope": diff.scope,
                "key": diff.key,
                "column": diff.column,
                "status": "failed",
                "reason": type(exc).__name__ + ": " + str(exc),
                "source_value": _json_cell(diff.expected),
                "target_value": _json_cell(diff.actual),
            })
            flush_writer(writer)
    return counts


def target_exists_application(tgt, app_no: str) -> bool:
    return bool(q_int(tgt, "SELECT COUNT(*) AS c FROM application WHERE application_no=%s", (app_no,)))


def target_exists_loan(tgt, app_no: str) -> bool:
    return bool(q_int(tgt, "SELECT COUNT(*) AS c FROM loan WHERE application_no=%s", (app_no,)))


def repair_user_ranges(cfg: Dict[str, Any], ids: Sequence[int], writer) -> None:
    if not ids:
        return
    repair_log(f"user repair start ids={len(set(ids))} preload=off")
    cfg = dict(cfg)
    cfg["vt_preload"] = False
    cfg["lup_preload"] = False
    cfg["lookup_parallel"] = 1
    cfg["user_insert_batch"] = min(int(cfg.get("user_insert_batch", 5000)), 1000)
    for lo, hi in compact_ids_to_ranges(ids):
        repair_log(f"user repair range start ({lo},{hi}]")
        src = mig.connect_source(cfg)
        tgt = mig.connect_target(cfg)
        mig._session_opts(tgt)
        try:
            before_missing = [x for x in ids if lo < int(x) <= hi]
            ret = mig.migrate_user_batch(cfg, lo, hi, worker_id=0, src=src, tgt=tgt)
            if ret is not None:
                tgt = ret
            for user_id in before_missing:
                ok_user = target_exists_user(tgt, int(user_id))
                ok_info = target_exists_user_info(tgt, int(user_id))
                writer.writerow({
                    "scope": "user",
                    "key": user_id,
                    "status": "success" if ok_user and ok_info else "failed",
                    "reason": "exists_after_replay" if ok_user and ok_info else f"user={int(ok_user)} user_info={int(ok_info)}",
                    "range": f"({lo},{hi}]",
                })
                flush_writer(writer)
                repair_log(
                    f"user repair row user_id={user_id} "
                    f"status={'success' if ok_user and ok_info else 'failed'} "
                    f"user={int(ok_user)} user_info={int(ok_info)}"
                )
        except Exception as exc:
            repair_log(f"user repair range failed ({lo},{hi}] {type(exc).__name__}: {exc}")
            for user_id in [x for x in ids if lo < int(x) <= hi]:
                writer.writerow({
                    "scope": "user",
                    "key": user_id,
                    "status": "failed",
                    "reason": type(exc).__name__ + ": " + str(exc),
                    "range": f"({lo},{hi}]",
                })
                flush_writer(writer)
        finally:
            mig._close_mysql_conn(src)
            mig._close_mysql_conn(tgt)
        repair_log(f"user repair range end ({lo},{hi}]")


def _repair_cfg(cfg: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(cfg)
    out["vt_preload"] = False
    out["lup_preload"] = False
    out["lookup_parallel"] = 1
    out["user_insert_batch"] = min(int(out.get("user_insert_batch", 5000)), 1000)
    return out


def repair_user_rows(cfg: Dict[str, Any], user_ids: Sequence[int], writer) -> None:
    ids = sorted({int(x) for x in user_ids if x is not None})
    if not ids:
        return
    repair_log(f"user direct repair start ids={len(ids)} preload=off")
    cfg = _repair_cfg(cfg)
    src = mig.connect_source(cfg)
    tgt = mig.connect_target(cfg)
    mig._session_opts(tgt)
    try:
        rows_user = []
        for lo, hi in compact_ids_to_ranges(ids):
            rows_user.extend(mig._select_user_batch_rows(src, lo, hi))
        found_ids = {int(row["user_id"]) for row in rows_user}
        keys = mig._extract_user_batch_keys(rows_user)
        dac_rows = mig._fetch_dac_by_device_ids(src, keys.get("device_ids", []))
        lookups = mig._make_user_lookups([], [], [], dac_rows)
        vt = mig.VtTokenResolver(
            src,
            enabled=cfg.get("vt_token_enable", True),
            chunk=cfg.get("vt_token_chunk", 2000),
            vt_db=cfg.get("vt_token_db", "ng_loan_market"),
        )
        for row in rows_user:
            vt.register(mig.VtTokenResolver.VT_MOBILE, row.get("mobile"))
        t0 = time.perf_counter()
        vt.prefetch()
        repair_log(f"user direct vt prefetch rows={len(rows_user)} elapsed={time.perf_counter() - t0:.2f}s {vt.summary()}")
        rows_ok = []
        token_ok_ids = set()
        for row in rows_user:
            user_id = int(row["user_id"])
            token = vt.resolve_token(
                mig.VtTokenResolver.VT_MOBILE,
                row.get("mobile"),
                context=f"user_id={user_id} field=mobile",
                row_data=row,
            )
            if not token:
                continue
            row["mobile"] = token
            rows_ok.append(row)
            token_ok_ids.add(user_id)
        mig._prepare_user_insert_rows(rows_ok, lookups)
        if rows_ok:
            tgt, inserted = mig._bulk_insert_rows(
                tgt, cfg, "target",
                "user", mig.USER_INSERT_COLS, rows_ok,
                cfg["user_insert_batch"], ignore=True,
            )
            tgt.commit()
        else:
            inserted = 0
        repair_log(f"user direct inserted={inserted} candidates={len(rows_ok)} source_rows={len(rows_user)}")
        existing_users = target_existing_user_ids(tgt, ids)
        existing_infos = target_existing_user_info_ids(tgt, ids)
        for user_id in ids:
            ok_user = user_id in existing_users
            ok_info = user_id in existing_infos
            if ok_user:
                reason = "exists_after_repair"
            elif user_id not in found_ids:
                reason = "no_source_user_row"
            elif user_id not in token_ok_ids:
                reason = "missing_mobile_token"
            else:
                reason = "insert_failed"
            writer.writerow({
                "scope": "user",
                "key": user_id,
                "status": "success" if ok_user and ok_info else "failed",
                "reason": reason if ok_user and ok_info else f"{reason}; user={int(ok_user)} user_info={int(ok_info)}",
                "range": "direct",
            })
            flush_writer(writer)
            repair_log(
                f"user direct repair row user_id={user_id} "
                f"status={'success' if ok_user and ok_info else 'failed'} "
                f"user={int(ok_user)} user_info={int(ok_info)} reason={reason}"
            )
    except Exception as exc:
        repair_log(f"user direct repair failed {type(exc).__name__}: {exc}")
        for user_id in ids:
            writer.writerow({
                "scope": "user",
                "key": user_id,
                "status": "failed",
                "reason": type(exc).__name__ + ": " + str(exc),
                "range": "direct",
            })
            flush_writer(writer)
    finally:
        mig._close_mysql_conn(src)
        mig._close_mysql_conn(tgt)


def repair_bankcard_rows(cfg: Dict[str, Any], user_ids: Sequence[int], writer) -> None:
    ids = sorted({int(x) for x in user_ids if x is not None})
    if not ids:
        return
    repair_log(f"user_bankcard repair start ids={len(ids)} preload=off")
    cfg = _repair_cfg(cfg)
    src = mig.connect_source(cfg)
    tgt = mig.connect_target(cfg)
    mig._session_opts(tgt)
    try:
        ud_rows = mig._select_ud_rows_by_user_ids(src, ids)
        lookups = mig._make_user_lookups(ud_rows, [], [], [])
        vt = mig.VtTokenResolver(
            src,
            enabled=cfg.get("vt_token_enable", True),
            chunk=cfg.get("vt_token_chunk", 2000),
            vt_db=cfg.get("vt_token_db", "ng_loan_market"),
        )
        for ud in ud_rows:
            vt.register(mig.VtTokenResolver.VT_BANK, ud.get("bankAccount"))
        t0 = time.perf_counter()
        vt.prefetch()
        repair_log(f"user_bankcard vt prefetch rows={len(ud_rows)} elapsed={time.perf_counter() - t0:.2f}s {vt.summary()}")
        rows = mig._build_bankcard_rows(lookups, vt, cfg=cfg)
        rows_by_user = {int(row["group_user_id"]): row for row in rows}
        existing_before = target_existing_bankcard_user_ids(tgt, ids)
        insert_rows = [
            row for row in rows
            if int(row["group_user_id"]) not in existing_before
        ]
        if insert_rows:
            tgt, inserted = mig._bulk_insert_rows(
                tgt, cfg, "target",
                "user_bankcard", mig.USER_BANKCARD_COLS, insert_rows,
                cfg["user_insert_batch"], ignore=True,
            )
            tgt.commit()
        else:
            inserted = 0
        repair_log(f"user_bankcard inserted={inserted} candidates={len(rows)}")
        existing_after = target_existing_bankcard_user_ids(tgt, ids)
        for user_id in ids:
            ok = user_id in existing_after
            has_source = user_id in rows_by_user
            writer.writerow({
                "scope": "user_bankcard",
                "key": user_id,
                "status": "success" if ok else "failed",
                "reason": "exists_after_repair" if ok else ("no_source_bankcard_row" if not has_source else "insert_failed"),
                "range": "direct",
            })
            flush_writer(writer)
            repair_log(f"user_bankcard repair row user_id={user_id} status={'success' if ok else 'failed'}")
    except Exception as exc:
        repair_log(f"user_bankcard repair failed {type(exc).__name__}: {exc}")
        for user_id in ids:
            writer.writerow({
                "scope": "user_bankcard",
                "key": user_id,
                "status": "failed",
                "reason": type(exc).__name__ + ": " + str(exc),
                "range": "direct",
            })
            flush_writer(writer)
    finally:
        mig._close_mysql_conn(src)
        mig._close_mysql_conn(tgt)


def repair_user_product_rows(cfg: Dict[str, Any], product_keys: Sequence[Tuple[int, str]], writer) -> None:
    keys = sorted({(int(user_id), str(product_id)) for user_id, product_id in product_keys})
    if not keys:
        return
    repair_log(f"user_product repair start keys={len(keys)}")
    cfg = _repair_cfg(cfg)
    src = mig.connect_source(cfg)
    tgt = mig.connect_target(cfg)
    mig._session_opts(tgt)
    try:
        user_ids = sorted({user_id for user_id, _ in keys})
        wanted = set(keys)
        prod_src = []
        for lo, hi in compact_ids_to_ranges(user_ids):
            prod_src.extend(
                row for row in mig._inline_mat_user_product(src, cfg, lo, hi)
                if (int(row["userId"]), str(row["productId"])) in wanted
            )
        rows = mig._build_user_product_rows(prod_src)
        rows_by_key = {
            (int(row["group_user_id"]), str(row["product_id"])): row
            for row in rows
        }
        existing_before = target_existing_user_product_keys(tgt, keys)
        insert_rows = [
            row for row in rows
            if (int(row["group_user_id"]), str(row["product_id"])) not in existing_before
        ]
        if insert_rows:
            tgt, inserted = mig._bulk_insert_rows(
                tgt, cfg, "target",
                "user_product", mig.USER_PRODUCT_COLS, insert_rows,
                cfg["user_insert_batch"], ignore=True,
            )
            tgt.commit()
        else:
            inserted = 0
        repair_log(f"user_product inserted={inserted} candidates={len(rows)}")
        existing_after = target_existing_user_product_keys(tgt, keys)
        for user_id, product_id in keys:
            ok = (user_id, product_id) in existing_after
            has_source = (user_id, product_id) in rows_by_key
            writer.writerow({
                "scope": "user_product",
                "key": f"({user_id},{product_id})",
                "status": "success" if ok else "failed",
                "reason": "exists_after_repair" if ok else ("no_source_product_row" if not has_source else "insert_failed"),
                "range": "direct",
            })
            flush_writer(writer)
            repair_log(
                f"user_product repair row user_id={user_id} product_id={product_id} "
                f"status={'success' if ok else 'failed'}"
            )
    except Exception as exc:
        repair_log(f"user_product repair failed {type(exc).__name__}: {exc}")
        for user_id, product_id in keys:
            writer.writerow({
                "scope": "user_product",
                "key": f"({user_id},{product_id})",
                "status": "failed",
                "reason": type(exc).__name__ + ": " + str(exc),
                "range": "direct",
            })
            flush_writer(writer)
    finally:
        mig._close_mysql_conn(src)
        mig._close_mysql_conn(tgt)


def source_app_ids_to_nos(src, ids: Sequence[int]) -> Dict[int, str]:
    vals = sorted({int(x) for x in ids})
    out: Dict[int, str] = {}
    for i in range(0, len(vals), 5000):
        part = vals[i:i + 5000]
        ph = ",".join(["%s"] * len(part))
        rows = q_rows(
            src,
            f"""
            SELECT id, applicationNo AS application_no
            FROM ng_loan_market.application
            WHERE id IN ({ph})
            """,
            part,
        )
        for row in rows:
            out[int(row["id"])] = str(row["application_no"])
    return out


def repair_application_ranges(cfg: Dict[str, Any], app_ids: Sequence[int], writer) -> None:
    if not app_ids:
        return
    repair_log(f"application repair start ids={len(set(app_ids))} preload=off")
    cfg = dict(cfg)
    cfg["vt_preload"] = False
    cfg["lookup_parallel"] = 1
    cfg["app_insert_batch"] = min(int(cfg.get("app_insert_batch", 5000)), 1000)
    cfg["id_mapping_insert_batch"] = min(int(cfg.get("id_mapping_insert_batch", 10000)), 2000)
    src_lookup = mig.connect_source(cfg)
    try:
        id_to_no = source_app_ids_to_nos(src_lookup, app_ids)
    finally:
        mig._close_mysql_conn(src_lookup)
    for lo, hi in compact_ids_to_ranges(app_ids):
        repair_log(f"application repair range start ({lo},{hi}]")
        src = mig.connect_source(cfg)
        tgt = mig.connect_target(cfg)
        mig._session_opts(tgt)
        try:
            in_range = [x for x in app_ids if lo < int(x) <= hi]
            ret = mig.migrate_app_batch(cfg, lo, hi, worker_id=0, src=src, tgt=tgt)
            if ret is not None:
                tgt = ret
            for app_id in in_range:
                app_no = id_to_no.get(int(app_id), "")
                ok_app = target_exists_application(tgt, app_no) if app_no else False
                ok_loan = target_exists_loan(tgt, app_no) if app_no else False
                writer.writerow({
                    "scope": "application",
                    "key": app_id,
                    "status": "success" if ok_app else "failed",
                    "reason": "exists_after_replay" if ok_app else f"application_no={app_no} loan={int(ok_loan)}",
                    "range": f"({lo},{hi}]",
                })
                flush_writer(writer)
                repair_log(
                    f"application repair row app_id={app_id} app_no={app_no} "
                    f"status={'success' if ok_app else 'failed'} loan={int(ok_loan)}"
                )
        except Exception as exc:
            repair_log(f"application repair range failed ({lo},{hi}] {type(exc).__name__}: {exc}")
            for app_id in [x for x in app_ids if lo < int(x) <= hi]:
                writer.writerow({
                    "scope": "application",
                    "key": app_id,
                    "status": "failed",
                    "reason": type(exc).__name__ + ": " + str(exc),
                    "range": f"({lo},{hi}]",
                })
                flush_writer(writer)
        finally:
            mig._close_mysql_conn(src)
            mig._close_mysql_conn(tgt)
        repair_log(f"application repair range end ({lo},{hi}]")


def send_feishu(webhook: str, text: str) -> None:
    if not webhook:
        return
    payload = json.dumps({"msg_type": "text", "content": {"text": text}}).encode("utf-8")
    req = request.Request(webhook, data=payload, headers={"Content-Type": "application/json"})
    with request.urlopen(req, timeout=20) as resp:
        resp.read()


def load_cfg(env_path: str) -> Dict[str, Any]:
    old_argv = sys.argv[:]
    try:
        sys.argv = ["ng_migration_run.py", "verify"]
        cfg = mig.load_env()
    finally:
        sys.argv = old_argv
    if env_path:
        # ng_migration_run.load_env reads only sibling ng_migration.env; keep explicit
        # argument for CLI clarity, but deployment uses the same directory.
        pass
    cfg["workers"] = 1
    cfg["app_workers"] = 1
    cfg["lookup_parallel"] = 1
    cfg["user_batch"] = max(1, int(cfg.get("user_batch", 20000)))
    cfg["app_batch"] = max(1, int(cfg.get("app_batch", 100000)))
    cfg["user_insert_batch"] = min(int(cfg.get("user_insert_batch", 20000)), 5000)
    cfg["app_insert_batch"] = min(int(cfg.get("app_insert_batch", 10000)), 5000)
    cfg["id_mapping_insert_batch"] = min(int(cfg.get("id_mapping_insert_batch", 25000)), 10000)
    cfg["drop_mat_on_start"] = False
    cfg["log_file"] = cfg.get("log_file") or "/tmp/ng_mig_repair.log"
    cfg["skip_log_file"] = cfg.get("skip_log_file") or "/tmp/ng_mig_repair.skip.log"
    mig.init_log(cfg)
    mig._global_perf = mig.GlobalPerfStats()
    return cfg


def fast_replay_application_ranges(
    cfg: Dict[str, Any],
    app_batch: int,
    start_id: int = 0,
    end_id: int = 0,
) -> None:
    cfg = dict(cfg)
    cfg["vt_preload"] = False
    cfg["lookup_parallel"] = 1
    cfg["app_insert_batch"] = min(int(cfg.get("app_insert_batch", 5000)), 1000)
    cfg["id_mapping_insert_batch"] = min(int(cfg.get("id_mapping_insert_batch", 10000)), 2000)
    batch = max(1, int(app_batch or cfg.get("app_batch", 5000)))
    src = mig.connect_source(cfg)
    tgt = mig.connect_target(cfg)
    mig._session_opts(tgt)
    try:
        max_app_id = int(end_id or 0)
        if max_app_id <= 0:
            max_app_id = q_int(src, "SELECT MAX(id) AS m FROM ng_loan_market.application")
        cur = max(0, int(start_id or 0))
        repair_log(
            f"fast replay application start range=({cur},{max_app_id}] "
            f"batch={batch} app_insert_batch={cfg['app_insert_batch']} "
            f"id_mapping_insert_batch={cfg['id_mapping_insert_batch']}"
        )
        t0 = time.perf_counter()
        total_ranges = 0
        while cur < max_app_id:
            hi = min(cur + batch, max_app_id)
            range_t0 = time.perf_counter()
            repair_log(f"fast replay application range start ({cur},{hi}]")
            tgt = mig.migrate_app_batch(cfg, cur, hi, worker_id=0, src=src, tgt=tgt) or tgt
            total_ranges += 1
            repair_log(
                f"fast replay application range done ({cur},{hi}] "
                f"elapsed={time.perf_counter() - range_t0:.1f}s"
            )
            cur = hi
        repair_log(
            f"fast replay application done ranges={total_ranges} "
            f"elapsed={time.perf_counter() - t0:.1f}s"
        )
    finally:
        mig._close_mysql_conn(src)
        mig._close_mysql_conn(tgt)


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Validate all tables and optionally repair missing rows")
    p.add_argument("--mode", choices=["validate", "repair", "fast-replay-application"], default="validate")
    p.add_argument("--apply", action="store_true", help="Actually repair missing rows")
    p.add_argument("--tables", default="all", help="Comma list: all,user,application")
    p.add_argument("--user-batch", type=int, default=100000)
    p.add_argument("--app-batch", type=int, default=100000)
    p.add_argument("--reports-dir", default=str(REPORT_DIR))
    p.add_argument("--feishu-webhook", default="")
    p.add_argument("--env", default=str(HERE / "ng_migration.env"))
    p.add_argument("--repair-log", default="/tmp/ng_mig_repair.log")
    p.add_argument("--report-base-url", default="")
    p.add_argument("--date-window", choices=["last-month"], default="")
    p.add_argument("--from-validation-csv", default="", help="Skip validation and repair from an existing validate_all_missing CSV")
    p.add_argument("--repair-lookup-chunk", type=int, default=50)
    p.add_argument("--repair-mapping-chunk", type=int, default=20)
    p.add_argument("--field-diff-chunk", type=int, default=FIELD_DIFF_LOOKUP_CHUNK)
    p.add_argument("--app-validate-batch", type=int, default=APP_VALIDATE_BATCH)
    p.add_argument("--start-id", type=int, default=0)
    p.add_argument("--end-id", type=int, default=0)
    return p.parse_args(argv)


def build_repair_plan(
    user_missing: Dict[str, List[Any]],
    app_missing: Dict[str, List[Any]],
) -> RepairPlan:
    user_ids = sorted({
        int(x)
        for x in (user_missing.get("user", []) + user_missing.get("user_info", []))
        if x is not None
    })
    bankcard_user_ids = sorted({
        int(x)
        for x in user_missing.get("user_bankcard", [])
        if x is not None
    })
    product_keys = sorted({
        (int(user_id), str(product_id))
        for user_id, product_id in user_missing.get("user_product", [])
    })
    app_ids = sorted({
        int(x)
        for x in (app_missing.get("application_ids", []) + app_missing.get("_repair_application_ids", []))
        if x is not None
    })
    return RepairPlan(
        user_ids=user_ids,
        bankcard_user_ids=bankcard_user_ids,
        product_keys=product_keys,
        app_ids=app_ids,
    )


def execute_repair_plan(cfg: Dict[str, Any], plan: RepairPlan, writer) -> None:
    repair_log(
        "repair plan "
        f"user={len(plan.user_ids)} "
        f"user_bankcard={len(plan.bankcard_user_ids)} "
        f"user_product={len(plan.product_keys)} "
        f"application={len(plan.app_ids)}"
    )
    repair_user_rows(cfg, plan.user_ids, writer)
    repair_bankcard_rows(cfg, plan.bankcard_user_ids, writer)
    repair_user_product_rows(cfg, plan.product_keys, writer)
    repair_application_ranges(cfg, plan.app_ids, writer)


def load_missing_from_csv(path: str) -> Tuple[Dict[str, List[Any]], Dict[str, List[Any]]]:
    user_missing: Dict[str, List[Any]] = {}
    app_missing: Dict[str, List[Any]] = {}
    if not path:
        return user_missing, app_missing
    with open(path, "r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            scope = row.get("scope") or ""
            typ = row.get("mismatch_type") or ""
            raw_key = row.get("key")
            if raw_key is None or raw_key == "":
                continue
            if scope == "user":
                if typ == "user_product":
                    parts = raw_key.strip("()").split(",")
                    if len(parts) >= 1:
                        key = (int(parts[0].strip()), parts[1].strip().strip("'\"") if len(parts) > 1 else "")
                    else:
                        continue
                else:
                    key = int(raw_key)
                user_missing.setdefault(typ, []).append(key)
            elif scope == "application":
                key = int(raw_key) if typ == "application_ids" else raw_key
                app_missing.setdefault(typ, []).append(key)
    return user_missing, app_missing


def main(argv: Sequence[str] = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    cfg = load_cfg(args.env)
    set_repair_log(args.repair_log)
    if args.mode == "fast-replay-application":
        fast_replay_application_ranges(
            cfg,
            app_batch=args.app_batch,
            start_id=args.start_id,
            end_id=args.end_id,
        )
        return 0
    reports_dir = Path(args.reports_dir)
    reports_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    validation_csv = reports_dir / f"validate_all_missing_{ts}.csv"
    repair_csv = reports_dir / f"repair_results_{ts}.csv"
    field_diff_csv = reports_dir / f"field_diffs_{ts}.csv"
    field_repair_csv = reports_dir / f"field_repair_results_{ts}.csv"
    summary_md = reports_dir / f"validate_repair_summary_{ts}.md"
    field_diffs: List[FieldDiff] = []

    if args.from_validation_csv:
        repair_log(f"load missing from csv {args.from_validation_csv}")
        user_missing, app_missing = load_missing_from_csv(args.from_validation_csv)
    else:
        src = mig.connect_source(cfg)
        tgt = mig.connect_target(cfg)
        try:
            table_sel = {x.strip() for x in args.tables.split(",") if x.strip()}
            do_user = "all" in table_sel or "user" in table_sel
            do_app = "all" in table_sel or "application" in table_sel
            user_missing = {}
            app_missing = {}
            window = compute_date_window(args.date_window) if args.date_window else None
            window_cache: Optional[WindowSourceCache] = None
            if window:
                cfg["app_validate_batch"] = args.app_validate_batch
                cfg["repair_lookup_chunk"] = args.repair_lookup_chunk
                cfg["repair_mapping_chunk"] = args.repair_mapping_chunk
                window_cache = load_window_source_cache(cfg, src, window)
            if do_user:
                if window:
                    user_missing = validate_user_missing_for_window(src, tgt, window, cache=window_cache)
                else:
                    max_user_id = int(cfg.get("max_user_id") or 0)
                    user_missing = validate_user_missing(src, tgt, max_user_id, args.user_batch)
            if do_app:
                if not window:
                    cfg["repair_lookup_chunk"] = args.repair_lookup_chunk
                    cfg["repair_mapping_chunk"] = args.repair_mapping_chunk
                    cfg["app_validate_batch"] = args.app_validate_batch
                if window:
                    app_missing = validate_application_missing_for_window(cfg, src, tgt, window, cache=window_cache)
                else:
                    if cfg.get("max_app_id"):
                        max_app_id = int(cfg["max_app_id"])
                    else:
                        max_app_id = q_int(src, "SELECT MAX(id) AS m FROM ng_loan_market.application")
                    app_missing = validate_application_missing(cfg, src, tgt, max_app_id, args.app_batch)
            elif not window:
                cfg["app_validate_batch"] = args.app_validate_batch
            if window:
                repair_log(
                    f"field diff start chunk_size={args.field_diff_chunk} "
                    f"app_validate_batch={args.app_validate_batch} source=cache"
                )
                field_diffs = collect_field_diffs_for_window(
                    cfg, src, tgt, window, table_sel, chunk_size=args.field_diff_chunk,
                    user_missing=user_missing, app_missing=app_missing, cache=window_cache,
                )
        finally:
            mig._close_mysql_conn(src)
            mig._close_mysql_conn(tgt)

    with validation_csv.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["scope", "mismatch_type", "key"])
        w.writeheader()
        for name, vals in user_missing.items():
            for val in vals:
                w.writerow({"scope": "user", "mismatch_type": name, "key": val})
        for name, vals in app_missing.items():
            if name.startswith("_"):
                continue
            for val in vals:
                w.writerow({"scope": "application", "mismatch_type": name, "key": val})

    with field_diff_csv.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["scope", "key", "column", "source_value", "target_value"])
        w.writeheader()
        for diff in field_diffs:
            w.writerow({
                "scope": diff.scope,
                "key": diff.key,
                "column": diff.column,
                "source_value": _json_cell(diff.expected),
                "target_value": _json_cell(diff.actual),
            })

    repair_counts = {"success": 0, "failed": 0}
    if args.mode == "repair" and args.apply:
        with repair_csv.open("w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["scope", "key", "status", "reason", "range"])
            w.flush = f.flush
            w.writeheader()
            f.flush()
            repair_log(f"repair csv initialized path={repair_csv}")
            execute_repair_plan(cfg, build_repair_plan(user_missing, app_missing), w)
        with repair_csv.open("r", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if row["status"] in repair_counts:
                    repair_counts[row["status"]] += 1
    else:
        repair_csv.write_text("scope,key,status,reason,range\n", encoding="utf-8")

    field_repair_counts = {"success": 0, "failed": 0}
    if args.mode == "repair" and args.apply and field_diffs:
        tgt = mig.connect_target(cfg)
        mig._session_opts(tgt)
        try:
            with field_repair_csv.open("w", encoding="utf-8", newline="") as f:
                w = csv.DictWriter(
                    f,
                    fieldnames=["scope", "key", "column", "status", "reason", "source_value", "target_value"],
                )
                w.flush = f.flush
                w.writeheader()
                f.flush()
                field_repair_counts = apply_field_diffs(tgt, field_diffs, w)
        finally:
            mig._close_mysql_conn(tgt)
    else:
        field_repair_csv.write_text("scope,key,column,status,reason,source_value,target_value\n", encoding="utf-8")

    lines = [
        "# Validate And Repair Summary",
        "",
        f"- mode: `{args.mode}`",
        f"- apply: `{int(args.apply)}`",
        f"- validation_csv: `{validation_csv}`",
        f"- repair_csv: `{repair_csv}`",
        f"- field_diff_csv: `{field_diff_csv}`",
        f"- field_repair_csv: `{field_repair_csv}`",
    ]
    if args.report_base_url:
        base_url = args.report_base_url.rstrip("/")
        lines.extend([
            f"- validation_url: `{base_url}/{validation_csv.name}`",
            f"- repair_url: `{base_url}/{repair_csv.name}`",
            f"- field_diff_url: `{base_url}/{field_diff_csv.name}`",
            f"- field_repair_url: `{base_url}/{field_repair_csv.name}`",
            f"- summary_url: `{base_url}/{summary_md.name}`",
        ])
    lines.extend(["", "## Missing Counts", ""])
    for name, vals in user_missing.items():
        lines.append(f"- user.{name}: {len(vals)}")
    for name, vals in app_missing.items():
        if name.startswith("_"):
            continue
        lines.append(f"- application.{name}: {len(vals)}")
    if app_missing.get("_repair_application_ids"):
        lines.append(f"- application._repair_application_ids: {len(set(app_missing['_repair_application_ids']))}")
    lines.extend([
        "",
        "## Repair Counts",
        "",
        f"- success: {repair_counts['success']}",
        f"- failed: {repair_counts['failed']}",
        "",
        "## Field Diff Counts",
        "",
        f"- field_diffs: {len(field_diffs)}",
        f"- field_repair_success: {field_repair_counts['success']}",
        f"- field_repair_failed: {field_repair_counts['failed']}",
    ])
    summary_md.write_text("\n".join(lines) + "\n", encoding="utf-8")
    text = "\n".join(lines)
    if args.feishu_webhook:
        send_feishu(args.feishu_webhook, text[:3500])
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
