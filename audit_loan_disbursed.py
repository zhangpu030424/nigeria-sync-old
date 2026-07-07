#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""以目标库已放款 application 为基准，全量核对 loan 表（内存对账）。

前提（application 表视为正确）:
  SELECT application_no, app_id, sn
  FROM application
  WHERE app_id NOT IN (567,569,568,571,572,573)
    AND disbursed_time > 0;

每个 application_no 在 loan 表应 **有且仅有 1 条** 对应行。

期望 loan_no（源库 canonical，由 --loan-no-sn 选择中间段）:
  1. 从 application_no 取 market 后缀，如 ng0562-177702748012033909 → 177702748012033909
  2. 源库查 ext_sn = market_suffix:
     plan_sn（默认）: repay_plan 最大 plan_sn 那条
     core_sn: SELECT sn FROM ng_loan_core.application WHERE ext_sn = '{market_suffix}'
  3. loan_no = ng-{sn}-{period:02d}{roll_sequence:03d}
     例 plan_sn: ng-217770275191-01000
     例 core_sn: ng-178126532212019674-01000（与 ng_migration_run 入库逻辑一致）

Usage:
  # 全量核对（默认 plan_sn 中间段）
  python3 audit_loan_disbursed.py --env ./ng_migration.env --workers 8

  # 用 core application.sn 作为 loan_no 中间段（对齐迁移入库）
  python3 audit_loan_disbursed.py --env ./ng_migration.env --workers 16 \\
    --loan-no-sn core_sn --issues-csv /tmp/loan_audit_issues_core_sn.csv

  # 抽样
  python3 audit_loan_disbursed.py --env ./ng_migration.env --work-limit 10000

  # 导出修复 SQL（仅 wrong_loan_no / missing 等可自动修的）
  python3 audit_loan_disbursed.py --env ./ng_migration.env \\
    --plan-file /tmp/loan_audit_plan.json --sql-out /tmp/loan_audit_fix.sql
"""
import argparse
import csv
import hashlib
import json
import multiprocessing
import re
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, DefaultDict, Dict, List, Optional, Set, Tuple

import pymysql
from pymysql.cursors import DictCursor

try:
    from pymysql.cursors import SSDictCursor as _StreamCursor
except ImportError:
    _StreamCursor = DictCursor  # 老 pymysql 无 SSDictCursor 时退化（占内存更多）

import ng_migration_run as mig
from repair_loan_no_from_audit import exec_with_retry

HERE = Path(__file__).resolve().parent
DEFAULT_EXCLUDE_APP_IDS = (567, 569, 568, 571, 572, 573)
# 某批导入/测试数据，不参与对账（默认跳过）
DEFAULT_EXCLUDE_LOAN_CREATED_MS = (1785340800000,)
APP_SUFFIX_RE = re.compile(r"^ng\d+-(.+)$", re.I)

ISSUE_MISSING_LOAN = "missing_loan"
ISSUE_DUPLICATE_LOAN = "duplicate_loan"
ISSUE_WRONG_LOAN_NO = "wrong_loan_no"
ISSUE_WRONG_LOAN_APP = "wrong_loan_application_no"
ISSUE_NO_MARKET_SUFFIX = "no_market_suffix"
ISSUE_NO_CORE_SN = "no_core_sn"
ISSUE_NO_REPAY_PLAN = "no_repay_plan"


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
    )


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


def market_suffix(application_no: str) -> str:
    m = APP_SUFFIX_RE.match(str(application_no or "").strip())
    return m.group(1).strip() if m else ""


def build_market_suffixes(
    apps: List[dict], excluded_only_apps: Set[str]
) -> List[str]:
    """仅为参与对账的 application 收集 market 后缀。"""
    uniq: Set[str] = set()
    n = 0
    t0 = time.time()
    for app in apps:
        app_no = str(app["application_no"]).strip()
        if app_no in excluded_only_apps:
            continue
        n += 1
        suffix = market_suffix(app_no)
        if suffix:
            uniq.add(suffix)
        if n % 500000 == 0:
            print(
                "  suffix scan apps=%s unique_suffix=%s elapsed=%.1fs"
                % (n, len(uniq), time.time() - t0),
                flush=True,
            )
    out = sorted(uniq)
    print(
        "suffix done audit_apps=%s unique_market_suffix=%s elapsed=%.1fs"
        % (n, len(out), time.time() - t0),
        flush=True,
    )
    return out


def parse_exclude_ids(raw: str) -> Tuple[int, ...]:
    if not raw.strip():
        return DEFAULT_EXCLUDE_APP_IDS
    out: List[int] = []
    for part in raw.split(","):
        part = part.strip()
        if part:
            out.append(int(part))
    return tuple(out) if out else DEFAULT_EXCLUDE_APP_IDS


def parse_exclude_created_ms(raw: str) -> Tuple[int, ...]:
    if not raw.strip():
        return DEFAULT_EXCLUDE_LOAN_CREATED_MS
    out: List[int] = []
    for part in raw.split(","):
        part = part.strip()
        if part:
            out.append(int(part))
    return tuple(out) if out else DEFAULT_EXCLUDE_LOAN_CREATED_MS


def load_disbursed_applications(
    tgt, exclude_app_ids: Tuple[int, ...], work_limit: int
) -> List[dict]:
    """目标库：老系统已放款单 application_no 列表。"""
    print("phase1: SELECT application (disbursed) ...", flush=True)
    t_load = time.time()
    ph = ",".join(["%s"] * len(exclude_app_ids))
    sql = f"""
        SELECT application_no, app_id, sn
        FROM application
        WHERE app_id NOT IN ({ph})
          AND disbursed_time > 0
          AND application_no IS NOT NULL AND application_no <> ''
        ORDER BY application_no ASC
    """
    if work_limit > 0:
        sql += " LIMIT %s"
        params: Tuple[Any, ...] = tuple(exclude_app_ids) + (work_limit,)
        with tgt.cursor() as cur:
            cur.execute(sql, params)
            rows = list(cur.fetchall())
        print(
            "loaded applications=%s elapsed=%.1fs"
            % (len(rows), time.time() - t_load),
            flush=True,
        )
        return rows

    params = tuple(exclude_app_ids)
    rows: List[dict] = []
    with tgt.cursor(_StreamCursor) as cur:
        cur.execute(sql, params)
        while True:
            batch = cur.fetchmany(50000)
            if not batch:
                break
            rows.extend(batch)
            if len(rows) == len(batch) or len(rows) % 100000 == 0:
                print(
                    "  applications loaded=%s elapsed=%.1fs"
                    % (len(rows), time.time() - t_load),
                    flush=True,
                )
    print(
        "loaded applications=%s elapsed=%.1fs"
        % (len(rows), time.time() - t_load),
        flush=True,
    )
    return rows


def load_all_loans(
    tgt, exclude_created_ms: Tuple[int, ...]
) -> Tuple[DefaultDict[str, List[dict]], Dict[str, dict], Set[str]]:
    """loan 全表进内存；跳过 exclude_created_ms 的行。

    返回 (by_app, by_loan_no, excluded_only_apps)。
    excluded_only_apps: 仅有被排除 loan、无其它 loan 的 application_no。
    """
    by_app: DefaultDict[str, List[dict]] = defaultdict(list)
    by_loan_no: Dict[str, dict] = {}
    exclude_set = set(int(x) for x in exclude_created_ms)
    apps_touched_excluded: Set[str] = set()
    print("phase2: stream loan table into memory ...", flush=True)
    t_load = time.time()
    sql = """
        SELECT loan_no, application_no, period, roll_sequence,
               status, due_date, created_time
        FROM loan
        WHERE application_no IS NOT NULL AND application_no <> ''
    """
    n = skip = scanned = 0
    last_log_scan = 0
    with tgt.cursor(_StreamCursor) as cur:
        cur.execute(sql)
        while True:
            batch = cur.fetchmany(50000)
            if not batch:
                break
            for row in batch:
                scanned += 1
                app_no = str(row["application_no"]).strip()
                ct = int(row.get("created_time") or 0)
                if ct in exclude_set:
                    apps_touched_excluded.add(app_no)
                    skip += 1
                    continue
                ln = str(row["loan_no"]).strip()
                item = {
                    "loan_no": ln,
                    "application_no": app_no,
                    "period": int(row.get("period") or 1),
                    "roll_sequence": int(row.get("roll_sequence") or 0),
                    "status": row.get("status"),
                    "due_date": row.get("due_date"),
                    "created_time": ct,
                }
                by_app[app_no].append(item)
                by_loan_no[ln] = item
                n += 1
            if scanned - last_log_scan >= 100000 or not batch:
                elapsed = time.time() - t_load
                rate = scanned / elapsed if elapsed > 0 else 0
                print(
                    "  loan scanned=%s kept=%s skipped=%s "
                    "unique_app=%s elapsed=%.1fs (%.0f rows/s)"
                    % (scanned, n, skip, len(by_app), elapsed, rate),
                    flush=True,
                )
                last_log_scan = scanned
    excluded_only_apps = apps_touched_excluded - set(by_app.keys())
    print(
        "loaded loan rows=%s skipped_created_time=%s unique_app=%s "
        "excluded_only_apps=%s elapsed=%.1fs"
        % (n, skip, len(by_app), len(excluded_only_apps), time.time() - t_load),
        flush=True,
    )
    return by_app, by_loan_no, excluded_only_apps


def fetch_source_repay_by_ext_sn(
    src, ext_sns: List[str]
) -> Dict[str, dict]:
    """market ext_sn -> {core_sn, plan_sn, rp_sn}。"""
    uniq = sorted({str(x).strip() for x in ext_sns if x})
    out: Dict[str, dict] = {}
    if not uniq:
        return out
    c = "ng_loan_core"
    total_batches = (len(uniq) + 1999) // 2000
    t0 = time.time()
    for i in range(0, len(uniq), 2000):
        bno = i // 2000 + 1
        part = uniq[i : i + 2000]
        ph = ",".join(["%s"] * len(part))
        sql = f"""
            SELECT ca.ext_sn AS ext_sn, ca.sn AS core_sn,
                   rp.sn AS rp_sn, rp.plan_sn
            FROM {c}.application ca
            INNER JOIN {c}.repay_plan rp ON rp.sn = ca.sn
            INNER JOIN (
                SELECT sn, MAX(plan_sn) AS max_plan_sn
                FROM {c}.repay_plan
                WHERE sn IN (
                    SELECT sn FROM {c}.application WHERE ext_sn IN ({ph})
                )
                GROUP BY sn
            ) pick ON rp.sn = pick.sn AND rp.plan_sn = pick.max_plan_sn
            WHERE ca.ext_sn IN ({ph})
        """
        with src.cursor() as cur:
            cur.execute(sql, part + part)
            for row in cur.fetchall():
                ext = str(row.get("ext_sn") or "").strip()
                if ext:
                    out[ext] = {
                        "core_sn": str(row.get("core_sn") or "").strip(),
                        "plan_sn": str(row.get("plan_sn") or "").strip(),
                        "rp_sn": str(row.get("rp_sn") or "").strip(),
                    }
        if bno == 1 or bno % 20 == 0 or bno == total_batches:
            print(
                "  source_repay batch %s/%s hits=%s elapsed=%.1fs"
                % (bno, total_batches, len(out), time.time() - t0),
                flush=True,
            )
    return out


def fetch_core_sn_only(src, ext_sns: List[str]) -> Dict[str, str]:
    """ext_sn -> core application.sn（SELECT sn FROM application WHERE ext_sn=...）。"""
    uniq = sorted({str(x).strip() for x in ext_sns if x})
    out: Dict[str, str] = {}
    if not uniq:
        return out
    c = "ng_loan_core"
    total_batches = (len(uniq) + 1999) // 2000
    t0 = time.time()
    for i in range(0, len(uniq), 2000):
        bno = i // 2000 + 1
        part = uniq[i : i + 2000]
        ph = ",".join(["%s"] * len(part))
        with src.cursor() as cur:
            cur.execute(
                f"""
                SELECT ext_sn, sn AS core_sn
                FROM {c}.application
                WHERE ext_sn IN ({ph})
                """,
                part,
            )
            for row in cur.fetchall():
                ext = str(row.get("ext_sn") or "").strip()
                if ext:
                    out[ext] = str(row.get("core_sn") or "").strip()
        if bno == 1 or bno % 20 == 0 or bno == total_batches:
            print(
                "  source_core batch %s/%s hits=%s elapsed=%.1fs"
                % (bno, total_batches, len(out), time.time() - t0),
                flush=True,
            )
    return out


def expected_loan_no(
    plan_sn: str, period: int = 1, roll_sequence: int = 0
) -> str:
    return mig.format_loan_no(plan_sn, period, roll_sequence)


def reconcile_one(
    app: dict,
    loans: List[dict],
    source_meta: Optional[dict],
    core_only: str,
    default_period: int,
    default_roll: int,
    loan_no_sn: str = "plan_sn",
) -> List[dict]:
    """返回该 application 的所有 issue 行（0~n）。"""
    app_no = str(app["application_no"]).strip()
    issues: List[dict] = []
    suffix = market_suffix(app_no)

    base = {
        "application_no": app_no,
        "app_id": app.get("app_id"),
        "target_sn": str(app.get("sn") or "").strip(),
        "market_suffix": suffix,
        "loan_no_sn_mode": loan_no_sn,
    }

    if not suffix:
        issues.append({**base, "issue": ISSUE_NO_MARKET_SUFFIX})
        return issues

    meta = source_meta or {}
    core_sn = str(meta.get("core_sn") or core_only or "").strip()
    plan_sn = str(meta.get("plan_sn") or "").strip()
    if not core_sn:
        issues.append({**base, "issue": ISSUE_NO_CORE_SN, "market_suffix": suffix})
        return issues

    if loan_no_sn == "core_sn":
        loan_sn = core_sn
    else:
        if not meta and core_only:
            issues.append({
                **base,
                "issue": ISSUE_NO_REPAY_PLAN,
                "core_sn": core_only,
                "market_suffix": suffix,
            })
            return issues
        loan_sn = plan_sn
        if not loan_sn:
            issues.append({
                **base,
                "issue": ISSUE_NO_REPAY_PLAN,
                "core_sn": core_sn,
                "plan_sn": plan_sn,
            })
            return issues

    exp_ln = expected_loan_no(loan_sn, default_period, default_roll)
    exp_ln_core = (
        expected_loan_no(core_sn, default_period, default_roll) if core_sn else ""
    )
    exp_ln_plan = (
        expected_loan_no(plan_sn, default_period, default_roll) if plan_sn else ""
    )
    base_exp = {
        **base,
        "expected_loan_no": exp_ln,
        "expected_loan_no_core_sn": exp_ln_core,
        "expected_loan_no_plan_sn": exp_ln_plan,
        "core_sn": core_sn,
        "plan_sn": plan_sn,
        "loan_sn_used": loan_sn,
        "expected_period": default_period,
        "expected_roll_sequence": default_roll,
    }

    if not loans:
        issues.append({**base_exp, "issue": ISSUE_MISSING_LOAN, "loan_count": 0})
        return issues

    if len(loans) > 1:
        issues.append({
            **base_exp,
            "issue": ISSUE_DUPLICATE_LOAN,
            "loan_count": len(loans),
            "actual_loan_nos": "|".join(r["loan_no"] for r in loans),
        })

    # 以 period=1 roll=0 为主核对；若仅一条则直接比
    primary = None
    for r in loans:
        if r["period"] == default_period and r["roll_sequence"] == default_roll:
            primary = r
            break
    if primary is None and len(loans) == 1:
        primary = loans[0]

    if primary is None:
        issues.append({
            **base_exp,
            "issue": ISSUE_DUPLICATE_LOAN,
            "loan_count": len(loans),
            "actual_loan_nos": "|".join(r["loan_no"] for r in loans),
            "note": "no_primary_period_roll",
        })
        return issues

    act_ln = primary["loan_no"]
    act_app = primary["application_no"]
    row = {
        **base_exp,
        "actual_loan_no": act_ln,
        "actual_application_no": act_app,
        "actual_period": primary["period"],
        "actual_roll_sequence": primary["roll_sequence"],
        "loan_count": len(loans),
    }

    if act_app != app_no:
        issues.append({**row, "issue": ISSUE_WRONG_LOAN_APP})
    if act_ln != exp_ln:
        issues.append({**row, "issue": ISSUE_WRONG_LOAN_NO})

    return issues


def reconcile_apps(
    apps: List[dict],
    loans_by_app: Dict[str, List[dict]],
    repay_meta: Dict[str, dict],
    core_only_map: Dict[str, str],
    excluded_only_apps: Set[str],
    default_period: int,
    default_roll: int,
    log_every: int = 200000,
    prefix: str = "",
    loan_no_sn: str = "plan_sn",
) -> Tuple[List[dict], int]:
    issues: List[dict] = []
    skipped = 0
    for i, app in enumerate(apps, 1):
        app_no = str(app["application_no"]).strip()
        if app_no in excluded_only_apps:
            skipped += 1
            continue
        suffix = market_suffix(app_no)
        issues.extend(
            reconcile_one(
                app,
                loans_by_app.get(app_no, []),
                repay_meta.get(suffix),
                core_only_map.get(suffix, ""),
                default_period,
                default_roll,
                loan_no_sn,
            )
        )
        if log_every > 0 and i % log_every == 0:
            print(
                "%sreconcile progress %s/%s issues=%s"
                % (prefix, i, len(apps), len(issues)),
                flush=True,
            )
    return issues, skipped


def split_chunks(rows: List[dict], workers: int) -> List[List[dict]]:
    n = max(1, int(workers))
    chunks: List[List[dict]] = [[] for _ in range(n)]
    for row in rows:
        key = str(row.get("application_no") or "")
        idx = int(hashlib.md5(key.encode("utf-8")).hexdigest(), 16) % n
        chunks[idx].append(row)
    return [c for c in chunks if c]


def _worker_reconcile(spec: dict) -> Tuple[List[dict], int]:
    label = "[%s/%s] " % (spec["worker_id"], spec["workers"])
    chunk = spec.get("apps") or []
    if not chunk:
        return [], 0
    print("%sstart apps=%s" % (label, len(chunk)), flush=True)
    issues, skipped = reconcile_apps(
        chunk,
        spec["loans_subset"],
        spec["repay_subset"],
        spec["core_subset"],
        set(spec.get("excluded_only_apps") or []),
        spec["default_period"],
        spec["default_roll"],
        spec.get("log_every", 200000),
        label,
        spec.get("loan_no_sn", "plan_sn"),
    )
    print(
        "%sdone issues=%s skipped=%s" % (label, len(issues), skipped),
        flush=True,
    )
    return issues, skipped


def run_parallel_reconcile(
    apps: List[dict],
    loans_by_app: Dict[str, List[dict]],
    repay_meta: Dict[str, dict],
    core_only_map: Dict[str, str],
    excluded_only_apps: Set[str],
    workers: int,
    default_period: int,
    default_roll: int,
    log_every: int,
    loan_no_sn: str = "plan_sn",
) -> Tuple[List[dict], int]:
    workers = min(max(1, int(workers)), 32)
    chunks = split_chunks(apps, workers)
    specs = []
    excluded_list = sorted(excluded_only_apps)
    for i, chunk in enumerate(chunks):
        if not chunk:
            continue
        app_nos = [str(a["application_no"]).strip() for a in chunk]
        loans_subset = {
            k: loans_by_app[k] for k in app_nos if k in loans_by_app
        }
        suffixes: Set[str] = set()
        for a in chunk:
            s = market_suffix(str(a["application_no"]))
            if s:
                suffixes.add(s)
        repay_subset = {s: repay_meta[s] for s in suffixes if s in repay_meta}
        core_subset = {s: core_only_map[s] for s in suffixes if s in core_only_map}
        specs.append(
            {
                "worker_id": i,
                "workers": len(chunks),
                "apps": chunk,
                "loans_subset": loans_subset,
                "repay_subset": repay_subset,
                "core_subset": core_subset,
                "excluded_only_apps": excluded_list,
                "default_period": default_period,
                "default_roll": default_roll,
                "log_every": log_every,
                "loan_no_sn": loan_no_sn,
            }
        )
    print(
        "parallel_reconcile workers=%s apps=%s (building specs ...)"
        % (len(specs), len(apps)),
        flush=True,
    )
    t_spawn = time.time()
    ctx = multiprocessing.get_context("spawn")
    with ctx.Pool(processes=len(specs)) as pool:
        print(
            "parallel_reconcile pool started elapsed=%.1fs"
            % (time.time() - t_spawn),
            flush=True,
        )
        results = pool.map(_worker_reconcile, specs)
    issues: List[dict] = []
    skipped = 0
    for iss, sk in results:
        issues.extend(iss)
        skipped += sk
    return issues, skipped


def fetch_source_repay_parallel(
    cfg: Dict[str, str], suffixes: List[str], workers: int
) -> Dict[str, dict]:
    workers = min(max(1, int(workers)), 16)
    if workers <= 1 or len(suffixes) < 4000:
        src = connect_source(cfg)
        try:
            return fetch_source_repay_by_ext_sn(src, suffixes)
        finally:
            src.close()

    n = workers
    chunks: List[List[str]] = [[] for _ in range(n)]
    for s in suffixes:
        idx = int(hashlib.md5(s.encode("utf-8")).hexdigest(), 16) % n
        chunks[idx].append(s)
    chunks = [c for c in chunks if c]

    def _fetch_part(part: List[str]) -> Dict[str, dict]:
        src = connect_source(cfg)
        try:
            return fetch_source_repay_by_ext_sn(src, part)
        finally:
            src.close()

    out: Dict[str, dict] = {}
    print(
        "parallel_source_fetch workers=%s suffixes=%s"
        % (len(chunks), len(suffixes)),
        flush=True,
    )
    with ThreadPoolExecutor(max_workers=len(chunks)) as ex:
        futs = [ex.submit(_fetch_part, c) for c in chunks]
        for fut in as_completed(futs):
            out.update(fut.result())
    return out


def fetch_core_sn_parallel(
    cfg: Dict[str, str], suffixes: List[str], workers: int
) -> Dict[str, str]:
    workers = min(max(1, int(workers)), 16)
    if workers <= 1 or len(suffixes) < 4000:
        src = connect_source(cfg)
        try:
            return fetch_core_sn_only(src, suffixes)
        finally:
            src.close()
    n = workers
    chunks: List[List[str]] = [[] for _ in range(n)]
    for s in suffixes:
        idx = int(hashlib.md5(s.encode("utf-8")).hexdigest(), 16) % n
        chunks[idx].append(s)
    chunks = [c for c in chunks if c]

    def _fetch_part(part: List[str]) -> Dict[str, str]:
        src = connect_source(cfg)
        try:
            return fetch_core_sn_only(src, part)
        finally:
            src.close()

    out: Dict[str, str] = {}
    with ThreadPoolExecutor(max_workers=len(chunks)) as ex:
        futs = [ex.submit(_fetch_part, c) for c in chunks]
        for fut in as_completed(futs):
            out.update(fut.result())
    return out


def find_orphan_loans(
    loans_by_app: DefaultDict[str, List[dict]], app_set: Set[str]
) -> List[dict]:
    out: List[dict] = []
    for app_no, rows in loans_by_app.items():
        if app_no not in app_set:
            for r in rows:
                out.append({
                    "issue": "orphan_loan",
                    "application_no": app_no,
                    "actual_loan_no": r["loan_no"],
                    "loan_count": len(rows),
                })
    return out


def _sql_escape(s: str) -> str:
    return str(s).replace("\\", "\\\\").replace("'", "''")


def build_fix_plan(issues: List[dict]) -> List[dict]:
    """从 wrong_loan_no 且 loan_count=1 生成可修 plan。"""
    plan: List[dict] = []
    for row in issues:
        if row.get("issue") != ISSUE_WRONG_LOAN_NO:
            continue
        if int(row.get("loan_count") or 0) != 1:
            continue
        if not row.get("expected_loan_no") or not row.get("actual_loan_no"):
            continue
        if row["expected_loan_no"] == row["actual_loan_no"]:
            continue
        plan.append({
            "application_no": row["application_no"],
            "from_loan_no": row["actual_loan_no"],
            "to_loan_no": row["expected_loan_no"],
            "period": row.get("actual_period", 1),
            "roll_sequence": row.get("actual_roll_sequence", 0),
        })
    return plan


def write_issues_csv(path: Path, issues: List[dict]) -> None:
    if not issues:
        path.write_text("", encoding="utf-8")
        return
    keys: List[str] = []
    seen = set()
    for row in issues:
        for k in row:
            if k not in seen:
                seen.add(k)
                keys.append(k)
    with path.open("w", encoding="utf-8", newline="") as fp:
        w = csv.DictWriter(fp, fieldnames=keys, extrasaction="ignore")
        w.writeheader()
        for row in issues:
            w.writerow(row)


def write_sql_fix(path: Path, plan: List[dict], batch: int) -> None:
    lines = ["-- audit_loan_disbursed fix loan_no, rows=%s" % len(plan)]
    for i in range(0, len(plan), max(1, batch)):
        part = plan[i : i + batch]
        lines.append("START TRANSACTION;")
        for row in part:
            lines.append(
                "UPDATE loan SET loan_no='%s' "
                "WHERE loan_no='%s' AND application_no='%s' "
                "AND period=%s AND roll_sequence=%s;"
                % (
                    _sql_escape(row["to_loan_no"]),
                    _sql_escape(row["from_loan_no"]),
                    _sql_escape(row["application_no"]),
                    int(row["period"]),
                    int(row["roll_sequence"]),
                )
            )
        lines.append("COMMIT;")
        lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def summarize(issues: List[dict]) -> Dict[str, int]:
    stats: Dict[str, int] = defaultdict(int)
    for row in issues:
        stats[str(row.get("issue") or "unknown")] += 1
    stats["total_issue_rows"] = len(issues)
    return dict(stats)


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(
        description="Audit loan vs disbursed applications (in-memory)"
    )
    p.add_argument("--env", default=str(HERE / "ng_migration.env"))
    p.add_argument(
        "--exclude-app-ids",
        default=",".join(str(x) for x in DEFAULT_EXCLUDE_APP_IDS),
        help="排除的新系统 app_id，默认 567,569,568,571,572,573",
    )
    p.add_argument("--work-limit", type=int, default=0, help="只核对前 N 条 application")
    p.add_argument(
        "--exclude-loan-created-ms",
        default=",".join(str(x) for x in DEFAULT_EXCLUDE_LOAN_CREATED_MS),
        help="跳过 loan.created_time 等于这些毫秒时间戳的行（默认 1785340800000）",
    )
    p.add_argument("--default-period", type=int, default=1)
    p.add_argument("--default-roll", type=int, default=0)
    p.add_argument("--issues-csv", default="/tmp/loan_audit_issues.csv")
    p.add_argument("--plan-file", default="/tmp/loan_audit_fix_plan.json")
    p.add_argument("--sql-out", default="", help="导出 wrong_loan_no 的 UPDATE SQL")
    p.add_argument("--sql-batch", type=int, default=50)
    p.add_argument("--skip-orphan", action="store_true", help="不扫 orphan loan")
    p.add_argument(
        "--workers",
        type=int,
        default=8,
        help="对账并发进程数（内存 reconcile），1=单进程",
    )
    p.add_argument(
        "--source-workers",
        type=int,
        default=0,
        help="源库 repay 查询并发线程数，0=与 workers 相同",
    )
    p.add_argument(
        "--log-every",
        type=int,
        default=200000,
        help="每 worker 对账进度打印间隔",
    )
    p.add_argument(
        "--loan-no-sn",
        choices=("plan_sn", "core_sn"),
        default="plan_sn",
        help="期望 loan_no 中间段：plan_sn=repay_plan 最大 plan_sn；"
        "core_sn=ng_loan_core.application.sn（ext_sn 查，对齐迁移入库）",
    )
    args = p.parse_args(argv)

    source_workers = args.source_workers or args.workers

    exclude_ids = parse_exclude_ids(args.exclude_app_ids)
    exclude_created_ms = parse_exclude_created_ms(args.exclude_loan_created_ms)
    t0 = time.time()

    issues_csv = args.issues_csv
    if issues_csv == "/tmp/loan_audit_issues.csv" and args.loan_no_sn == "core_sn":
        issues_csv = "/tmp/loan_audit_issues_core_sn.csv"

    print(
        "audit_loan_disbursed start workers=%s source_workers=%s work_limit=%s "
        "loan_no_sn=%s"
        % (
            args.workers,
            source_workers,
            args.work_limit or "all",
            args.loan_no_sn,
        ),
        flush=True,
    )

    cfg = load_env(Path(args.env))
    print("connecting target %s ..." % cfg.get("TARGET_HOST"), flush=True)
    tgt = connect_target(cfg)
    try:
        apps = load_disbursed_applications(tgt, exclude_ids, args.work_limit)
        loans_by_app, _loans_by_ln, excluded_only_apps = load_all_loans(
            tgt, exclude_created_ms
        )
    finally:
        tgt.close()

    if excluded_only_apps:
        print(
            "skip applications with only excluded loans: %s"
            % len(excluded_only_apps),
            flush=True,
        )

    app_set = {str(a["application_no"]).strip() for a in apps}
    print("phase3: build market suffix list ...", flush=True)
    suffixes = build_market_suffixes(apps, excluded_only_apps)

    t1 = time.time()
    if args.loan_no_sn == "core_sn":
        print(
            "phase3: fetch core.application.sn by ext_sn (no repay_plan) ...",
            flush=True,
        )
        core_map = fetch_core_sn_parallel(cfg, suffixes, source_workers)
        repay_meta = {
            ext: {"core_sn": sn, "plan_sn": ""} for ext, sn in core_map.items()
        }
        core_only_map = {}
        print(
            "source_core_hit=%s/%s elapsed=%.1fs"
            % (len(repay_meta), len(suffixes), time.time() - t1),
            flush=True,
        )
    else:
        print("phase3: fetch source repay_plan (may take long) ...", flush=True)
        repay_meta = fetch_source_repay_parallel(cfg, suffixes, source_workers)
        print(
            "source_repay_hit=%s/%s elapsed=%.1fs"
            % (len(repay_meta), len(suffixes), time.time() - t1),
            flush=True,
        )
        missing_ext = [s for s in suffixes if s not in repay_meta]
        core_only_map = (
            fetch_core_sn_parallel(cfg, missing_ext, source_workers)
            if missing_ext
            else {}
        )
        print("source_core_only=%s" % len(core_only_map), flush=True)

    print("phase4: reconcile in memory ...", flush=True)
    if args.workers > 1:
        issues, skipped_apps = run_parallel_reconcile(
            apps,
            dict(loans_by_app),
            repay_meta,
            core_only_map,
            excluded_only_apps,
            args.workers,
            args.default_period,
            args.default_roll,
            args.log_every,
            args.loan_no_sn,
        )
    else:
        issues, skipped_apps = reconcile_apps(
            apps,
            dict(loans_by_app),
            repay_meta,
            core_only_map,
            excluded_only_apps,
            args.default_period,
            args.default_roll,
            args.log_every,
            "",
            args.loan_no_sn,
        )

    print("skipped_apps_excluded_loan_only=%s" % skipped_apps, flush=True)

    if not args.skip_orphan:
        orphans = find_orphan_loans(loans_by_app, app_set)
        issues.extend(orphans)
        print("orphan_loan_rows=%s" % len(orphans), flush=True)

    stats = summarize(issues)
    print("issue_stats=%s" % stats, flush=True)

    write_issues_csv(Path(issues_csv), issues)
    print("wrote issues_csv=%s rows=%s" % (issues_csv, len(issues)), flush=True)

    fix_plan = build_fix_plan(issues)
    if args.plan_file:
        Path(args.plan_file).write_text(
            json.dumps(fix_plan, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        print("wrote fix_plan=%s rows=%s" % (args.plan_file, len(fix_plan)), flush=True)

    if args.sql_out:
        write_sql_fix(Path(args.sql_out), fix_plan, args.sql_batch)
        print("wrote sql_out=%s" % args.sql_out, flush=True)

    # 干净单：无任何 issue 行
    bad_apps = {
        str(r["application_no"])
        for r in issues
        if r.get("application_no")
    }
    print(
        "summary applications=%s ok_or_warn_only=%s problem_apps=%s elapsed=%.1fs"
        % (
            len(apps),
            len(apps) - len(bad_apps),
            len(bad_apps),
            time.time() - t0,
        ),
        flush=True,
    )
    for row in issues[:20]:
        print(" sample: %s" % row, flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
