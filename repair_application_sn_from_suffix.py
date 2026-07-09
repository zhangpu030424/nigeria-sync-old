#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""修复 application.sn：应与 application_no 后缀（market applicationNo）一致。

规则：
  good_sn = SUBSTRING_INDEX(application_no, '-', -1)   # ng0515-1783... → 1783...
  若 sn <> good_sn → 进修复 plan

加载：
  排除 app_id IN (567,568,569,571,572,573)，分页拉
  application_no, mobile, group_user_id, sn 到本地内存（可写 cache）

修复：
  UPDATE application SET sn=good_sn
  WHERE mobile=? AND group_user_id=? AND sn=bad_sn  （按主键定位）

Usage:
  python3 repair_application_sn_from_suffix.py --env ./ng_migration.env --dry-run \\
    --plan-file /tmp/fix_app_sn_plan.json \\
    --cache-file /tmp/application_sn_snapshot.json

  python3 repair_application_sn_from_suffix.py --env ./ng_migration.env --apply-only \\
    --plan-file /tmp/fix_app_sn_plan.json --workers 4 --batch-size 100

  # 同一 application_no 多个 sn：从 cache 出修复 plan
  python3 repair_application_sn_from_suffix.py --build-dup-app-no-plan \\
    --cache-file /tmp/application_sn_snapshot.json \\
    --dup-plan-file /tmp/fix_dup_app_no_plan.json

  python3 repair_application_sn_from_suffix.py --apply-dup-only \\
    --dup-plan-file /tmp/fix_dup_app_no_plan.json --workers 4 --batch-size 100
"""
import argparse
import json
import multiprocessing
import re
import time
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import pymysql
from pymysql.cursors import DictCursor

from repair_loan_no_from_audit import exec_with_retry

HERE = Path(__file__).resolve().parent
APP_SUFFIX_RE = re.compile(r"^ng\d+-(.+)$", re.I)
DEFAULT_EXCLUDE_APP_IDS = (567, 568, 569, 571, 572, 573)


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


def load_snapshot_cache(path: Path) -> List[dict]:
    t0 = time.time()
    print("loading cache %s ..." % path, flush=True)
    rows = json.loads(path.read_text(encoding="utf-8"))
    print(
        "loaded cache rows=%s elapsed=%.1fs" % (len(rows), time.time() - t0),
        flush=True,
    )
    return rows


def scan_snapshot_light(
    rows: List[dict], label: str = "scan"
) -> Tuple[Dict[str, Set[str]], Dict[Tuple[str, int], Set[str]], Set[Tuple[str, int, str]], int]:
    """单遍轻量扫描：只建 set，不存行列表（14M 行也快）。"""
    t0 = time.time()
    by_app_sn: Dict[str, Set[str]] = defaultdict(set)
    by_user_sn: Dict[Tuple[str, int], Set[str]] = defaultdict(set)
    pk_set: Set[Tuple[str, int, str]] = set()
    sn_mismatch = 0
    total = len(rows)
    for i, row in enumerate(rows, 1):
        app_no = row["application_no"]
        mobile = row["mobile"]
        gid = int(row["group_user_id"])
        sn = row["sn"]
        by_app_sn[app_no].add(sn)
        by_user_sn[(mobile, gid)].add(sn)
        pk_set.add((mobile, gid, sn))
        good = app_suffix(app_no)
        if good and sn != good:
            sn_mismatch += 1
        if i % 2000000 == 0:
            print(
                "%s progress %s/%s elapsed=%.1fs"
                % (label, i, total, time.time() - t0),
                flush=True,
            )
    print("%s done rows=%s elapsed=%.1fs" % (label, total, time.time() - t0), flush=True)
    return by_app_sn, by_user_sn, pk_set, sn_mismatch


def collect_dup_rows(
    rows: List[dict], dup_app_nos: Set[str]
) -> Tuple[Dict[str, List[dict]], Dict[Tuple[str, int, str], dict]]:
    """第二遍：只收集重复 application_no 的行（约几千行）。"""
    t0 = time.time()
    by_app_no: Dict[str, List[dict]] = defaultdict(list)
    pk_index: Dict[Tuple[str, int, str], dict] = {}
    for row in rows:
        app_no = row["application_no"]
        if app_no not in dup_app_nos:
            continue
        mobile = row["mobile"]
        gid = int(row["group_user_id"])
        sn = row["sn"]
        by_app_no[app_no].append(row)
        pk_index[(mobile, gid, sn)] = row
    print(
        "collect dup rows apps=%s rows=%s elapsed=%.1fs"
        % (len(by_app_no), sum(len(v) for v in by_app_no.values()), time.time() - t0),
        flush=True,
    )
    return by_app_no, pk_index


def load_applications(
    tgt,
    exclude_app_ids: Tuple[int, ...],
    page_size: int,
    disbursed_only: bool,
) -> List[dict]:
    print(
        "phase1: load application snapshot exclude_app_ids=%s page_size=%s ..."
        % (list(exclude_app_ids), page_size),
        flush=True,
    )
    t0 = time.time()
    ph = ",".join(["%s"] * len(exclude_app_ids))
    disbursed_sql = " AND disbursed_time > 0" if disbursed_only else ""
    sql = (
        """
        SELECT application_no, mobile, group_user_id, sn, app_id
        FROM application
        WHERE app_id NOT IN ("""
        + ph
        + """)
          AND application_no IS NOT NULL AND application_no <> ''
          AND mobile IS NOT NULL AND mobile <> ''
          AND sn IS NOT NULL AND sn <> ''
        """
        + disbursed_sql
        + """
          AND mobile > %s
        ORDER BY mobile ASC, group_user_id ASC, sn ASC
        LIMIT %s
        """
    )
    params_head = list(exclude_app_ids)
    after = ""
    page_no = 0
    rows: List[dict] = []
    while True:
        page_no += 1
        mobile_after = after
        lim = page_size

        def _page():
            _ping(tgt)
            with tgt.cursor() as cur:
                cur.execute(sql, tuple(params_head + [mobile_after, lim]))
                return list(cur.fetchall())

        batch = exec_with_retry(tgt, _page, "load application page=%s" % page_no)
        if not batch:
            break
        for row in batch:
            rows.append(
                {
                    "application_no": str(row.get("application_no") or "").strip(),
                    "mobile": str(row.get("mobile") or "").strip(),
                    "group_user_id": int(row["group_user_id"]),
                    "sn": str(row.get("sn") or "").strip(),
                    "app_id": row.get("app_id"),
                }
            )
        after = str(batch[-1]["mobile"])
        if page_no == 1 or len(rows) % 500000 == 0 or len(batch) < page_size:
            print(
                "  rows=%s pages=%s elapsed=%.1fs"
                % (len(rows), page_no, time.time() - t0),
                flush=True,
            )
        if len(batch) < page_size:
            break
    print("loaded application rows=%s elapsed=%.1fs" % (len(rows), time.time() - t0), flush=True)
    return rows


def build_plan(rows: List[dict], local_workers: int = 1) -> Tuple[List[dict], Dict[str, int]]:
    stats: Dict[str, int] = {"total": len(rows)}
    t0 = time.time()
    pk_seen: Set[Tuple[str, int, str]] = set()
    plan: List[dict] = []
    sn_mismatch = 0
    total = len(rows)
    for i, row in enumerate(rows, 1):
        app_no = row["application_no"]
        good_sn = app_suffix(app_no)
        if not good_sn:
            stats["skip_bad_app_no"] = stats.get("skip_bad_app_no", 0) + 1
            continue
        bad_sn = row["sn"]
        if bad_sn == good_sn:
            stats["ok"] = stats.get("ok", 0) + 1
            continue
        sn_mismatch += 1
        mobile = row["mobile"]
        gid = int(row["group_user_id"])
        target_pk = (mobile, gid, good_sn)
        if target_pk in pk_seen:
            stats["skip_pk_target_dup"] = stats.get("skip_pk_target_dup", 0) + 1
            continue
        pk_seen.add(target_pk)
        plan.append(
            {
                "action": "update_sn",
                "application_no": app_no,
                "mobile": mobile,
                "group_user_id": gid,
                "bad_sn": bad_sn,
                "good_sn": good_sn,
                "app_id": row.get("app_id"),
            }
        )
        if i % 2000000 == 0:
            print(
                "build_plan progress %s/%s elapsed=%.1fs"
                % (i, total, time.time() - t0),
                flush=True,
            )
    stats["sn_mismatch_rows"] = sn_mismatch
    stats["plan"] = len(plan)
    print(
        "phase2: plan=%s stats=%s elapsed=%.1fs"
        % (len(plan), stats, time.time() - t0),
        flush=True,
    )
    return plan, stats


def analyze_snapshot(rows: List[dict], local_workers: int = 1) -> Dict[str, int]:
    """统计 application_no 对应多个 sn 等情况（单遍轻量扫描）。"""
    del local_workers  # 保留参数兼容；多进程合并反而更慢
    t0 = time.time()
    by_app_sn, by_user_sn, _, sn_mismatch = scan_snapshot_light(rows, "analyze")
    multi_app_no = {k: v for k, v in by_app_sn.items() if len(v) > 1}
    multi_user = {k: v for k, v in by_user_sn.items() if len(v) > 1}
    stats = {
        "total": len(rows),
        "unique_application_no": len(by_app_sn),
        "application_no_multi_sn": len(multi_app_no),
        "unique_user_pk_prefix": len(by_user_sn),
        "user_multi_sn": len(multi_user),
        "sn_mismatch_rows": sn_mismatch,
    }
    print(
        "analyze snapshot stats=%s elapsed=%.1fs" % (stats, time.time() - t0),
        flush=True,
    )
    return stats


def build_dup_application_no_plan(
    rows: List[dict], local_workers: int = 1
) -> Tuple[List[dict], Dict[str, int]]:
    """同一 application_no 对应多个 sn 时的修复 plan。

    策略：
      - good_sn = application_no 后缀（market applicationNo）
      - 已有 sn=good_sn 的行 → 保留，删除其余错 sn 行
      - 全无 good_sn → 若 (mobile,gid,good_sn) 已存在则全删；否则改一条、删其余
    """
    stats: Dict[str, int] = {"total": len(rows)}
    t0 = time.time()
    del local_workers
    by_app_sn, _, pk_set, _ = scan_snapshot_light(rows, "dup_scan")
    dup_app_nos = {k for k, sns in by_app_sn.items() if len(sns) > 1}
    by_app_no, _pk_index = collect_dup_rows(rows, dup_app_nos)

    plan: List[dict] = []
    delete_keys: Set[Tuple[str, int, str, str]] = set()

    def _plan_delete(r: dict, app_no: str, good_sn: str, reason: str) -> None:
        key = (app_no, r["mobile"], int(r["group_user_id"]), r["sn"])
        if key in delete_keys:
            return
        delete_keys.add(key)
        plan.append(
            {
                "action": "delete",
                "application_no": app_no,
                "mobile": r["mobile"],
                "group_user_id": int(r["group_user_id"]),
                "bad_sn": r["sn"],
                "good_sn": good_sn,
                "reason": reason,
                "app_id": r.get("app_id"),
            }
        )

    def _plan_update(r: dict, app_no: str, good_sn: str, reason: str) -> None:
        plan.append(
            {
                "action": "update_sn",
                "application_no": app_no,
                "mobile": r["mobile"],
                "group_user_id": int(r["group_user_id"]),
                "bad_sn": r["sn"],
                "good_sn": good_sn,
                "reason": reason,
                "app_id": r.get("app_id"),
            }
        )

    for app_no in dup_app_nos:
        group = by_app_no.get(app_no) or []
        if len({r["sn"] for r in group}) <= 1:
            continue
        good_sn = app_suffix(app_no)
        if not good_sn:
            stats["skip_bad_app_no"] = stats.get("skip_bad_app_no", 0) + 1
            continue
        stats["dup_groups"] = stats.get("dup_groups", 0) + 1
        keepers = [r for r in group if r["sn"] == good_sn]
        losers = [r for r in group if r["sn"] != good_sn]

        if keepers:
            stats["groups_with_keeper"] = stats.get("groups_with_keeper", 0) + 1
            if len(keepers) > 1:
                stats["groups_multi_keeper"] = stats.get("groups_multi_keeper", 0) + 1
            for r in losers:
                _plan_delete(r, app_no, good_sn, "dup_app_no_wrong_sn")
            stats["delete"] = stats.get("delete", 0) + len(losers)
            continue

        losers_sorted = sorted(
            losers,
            key=lambda r: (len(r["sn"]), r["mobile"], r["group_user_id"], r["sn"]),
        )
        primary = losers_sorted[0]
        target_pk = (primary["mobile"], int(primary["group_user_id"]), good_sn)
        if target_pk in pk_set:
            ppk = (
                primary["mobile"],
                int(primary["group_user_id"]),
                primary["sn"],
            )
            if target_pk != ppk:
                stats["groups_good_pk_exists"] = stats.get(
                    "groups_good_pk_exists", 0
                ) + 1
                for r in losers:
                    _plan_delete(r, app_no, good_sn, "dup_app_no_good_pk_exists")
                stats["delete"] = stats.get("delete", 0) + len(losers)
                continue

        _plan_update(primary, app_no, good_sn, "dup_app_no_no_keeper")
        stats["update_sn"] = stats.get("update_sn", 0) + 1
        for r in losers_sorted[1:]:
            _plan_delete(r, app_no, good_sn, "dup_app_no_extra_row")
        stats["delete"] = stats.get("delete", 0) + max(0, len(losers) - 1)

    stats["plan"] = len(plan)
    print(
        "dup_app_no plan=%s stats=%s elapsed=%.1fs"
        % (len(plan), stats, time.time() - t0),
        flush=True,
    )
    return plan, stats


def apply_delete_batch(tgt, rows: List[dict]) -> Tuple[int, int, Dict[str, int]]:
    if not rows:
        return 0, 0, {}
    parts: List[str] = []
    params: List = []
    for r in rows:
        parts.append(
            "SELECT %s AS mobile, %s AS gid, %s AS bad_sn, %s AS app_no"
        )
        params.extend(
            [
                r["mobile"],
                int(r["group_user_id"]),
                r["bad_sn"],
                r["application_no"],
            ]
        )
    sql = (
        """
        DELETE a FROM application a
        INNER JOIN (
        """
        + " UNION ALL ".join(parts)
        + """
        ) x ON a.mobile = x.mobile
           AND a.group_user_id = x.gid
           AND a.sn = x.bad_sn
           AND a.application_no = x.app_no
        """
    )

    def _run():
        _ping(tgt)
        with tgt.cursor() as cur:
            cur.execute(sql, tuple(params))
            return int(cur.rowcount or 0)

    ok = int(exec_with_retry(tgt, _run, "delete batch size=%s" % len(rows)) or 0)
    skip = max(0, len(rows) - ok)
    stats: Dict[str, int] = {}
    if skip:
        stats["skip_missing"] = skip
    return ok, skip, stats


def apply_delete_one(tgt, row: dict) -> str:
    def _run():
        _ping(tgt)
        with tgt.cursor() as cur:
            cur.execute(
                """
                DELETE FROM application
                WHERE mobile=%s AND group_user_id=%s AND sn=%s
                  AND application_no=%s
                """,
                (
                    row["mobile"],
                    int(row["group_user_id"]),
                    row["bad_sn"],
                    row["application_no"],
                ),
            )
            return "ok" if int(cur.rowcount or 0) > 0 else "skip_missing"

    return exec_with_retry(
        tgt,
        _run,
        "delete sn=%s app=%s" % (row["bad_sn"], row["application_no"]),
    )


def dup_worker_run(spec: dict) -> Tuple[int, int, Dict[str, int]]:
    label = "[%s/%s] " % (spec["worker_id"], spec["workers"])
    chunk = spec.get("plan_chunk") or []
    stats: Dict[str, int] = {}
    if not chunk:
        return 0, 0, stats
    cfg = load_env(Path(spec["env"]))
    batch_size = max(1, int(spec.get("batch_size") or 100))
    print("%sstart delete rows=%s batch=%s" % (label, len(chunk), batch_size), flush=True)
    tgt = connect_target(cfg, for_apply=True)
    ok = skip = 0
    try:
        if batch_size <= 1:
            for i, row in enumerate(chunk, 1):
                try:
                    result = apply_delete_one(tgt, row)
                    tgt.commit()
                except Exception as exc:
                    print("%srow %s err=%s reconnect" % (label, i, exc), flush=True)
                    try:
                        tgt.rollback()
                    except Exception:
                        pass
                    tgt = _reconnect(cfg, tgt)
                    result = apply_delete_one(tgt, row)
                    tgt.commit()
                if result == "ok":
                    ok += 1
                else:
                    skip += 1
                    stats[result] = stats.get(result, 0) + 1
                if i == 1 or i % 200 == 0 or i == len(chunk):
                    print(
                        "%sprogress %s/%s ok=%s skip=%s"
                        % (label, i, len(chunk), ok, skip),
                        flush=True,
                    )
        else:
            total_batches = (len(chunk) + batch_size - 1) // batch_size
            for bi in range(0, len(chunk), batch_size):
                part = chunk[bi : bi + batch_size]
                bno = bi // batch_size + 1
                try:
                    bok, bskip, bstats = apply_delete_batch(tgt, part)
                    tgt.commit()
                except Exception as exc:
                    print("%sbatch %s err=%s reconnect" % (label, bno, exc), flush=True)
                    try:
                        tgt.rollback()
                    except Exception:
                        pass
                    tgt = _reconnect(cfg, tgt)
                    bok, bskip, bstats = apply_delete_batch(tgt, part)
                    tgt.commit()
                ok += bok
                skip += bskip
                for k, v in bstats.items():
                    stats[k] = stats.get(k, 0) + v
                print(
                    "%sbatch %s/%s deleted=%s skip=%s total_ok=%s"
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


def run_parallel_dup(
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
        "apply dup delete workers=%s rows=%s batch_size=%s"
        % (len(specs), len(plan), batch_size),
        flush=True,
    )
    if len(specs) == 1:
        return dup_worker_run(specs[0])
    ctx = multiprocessing.get_context("spawn")
    with ctx.Pool(processes=len(specs)) as pool:
        results = pool.map(dup_worker_run, specs)
    ok = sum(r[0] for r in results)
    skip = sum(r[1] for r in results)
    stats: Dict[str, int] = {}
    for _, _, s in results:
        for k, v in s.items():
            stats[k] = stats.get(k, 0) + v
    print("dup delete done ok=%s skip=%s stats=%s" % (ok, skip, stats), flush=True)
    return ok, skip, stats


def apply_batch(tgt, rows: List[dict]) -> Tuple[int, int, Dict[str, int]]:
    if not rows:
        return 0, 0, {}
    parts: List[str] = []
    params: List = []
    for r in rows:
        parts.append(
            "SELECT %s AS good_sn, %s AS mobile, %s AS gid, %s AS bad_sn"
        )
        params.extend(
            [r["good_sn"], r["mobile"], int(r["group_user_id"]), r["bad_sn"]]
        )
    sql = (
        """
        UPDATE application a
        INNER JOIN (
        """
        + " UNION ALL ".join(parts)
        + """
        ) x ON a.mobile = x.mobile
           AND a.group_user_id = x.gid
           AND a.sn = x.bad_sn
        SET a.sn = x.good_sn
        """
    )

    def _run():
        _ping(tgt)
        with tgt.cursor() as cur:
            cur.execute(sql, tuple(params))
            return int(cur.rowcount or 0)

    try:
        ok = int(exec_with_retry(tgt, _run, "apply batch size=%s" % len(rows)) or 0)
        skip = max(0, len(rows) - ok)
        stats: Dict[str, int] = {}
        if skip:
            stats["skip_missing_or_pk"] = skip
        return ok, skip, stats
    except pymysql.err.IntegrityError as e:
        if getattr(e, "args", None) and e.args and e.args[0] == 1062:
            try:
                tgt.rollback()
            except Exception:
                pass
            return apply_rows_fallback(tgt, rows)
        raise


def apply_one(tgt, row: dict) -> str:
    def _run():
        _ping(tgt)
        with tgt.cursor() as cur:
            cur.execute(
                """
                UPDATE application
                SET sn=%s
                WHERE mobile=%s AND group_user_id=%s AND sn=%s
                """,
                (
                    row["good_sn"],
                    row["mobile"],
                    int(row["group_user_id"]),
                    row["bad_sn"],
                ),
            )
            return "ok" if int(cur.rowcount or 0) > 0 else "skip_missing"

    try:
        return exec_with_retry(
            tgt,
            _run,
            "update sn %s->%s" % (row["bad_sn"], row["good_sn"]),
        )
    except pymysql.err.IntegrityError as e:
        if getattr(e, "args", None) and e.args and e.args[0] == 1062:
            return "skip_pk_exists"
        raise


def apply_rows_fallback(tgt, rows: List[dict]) -> Tuple[int, int, Dict[str, int]]:
    ok = skip = 0
    stats: Dict[str, int] = {"fallback_row": len(rows)}
    for r in rows:
        result = apply_one(tgt, r)
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
    batch_size = max(1, int(spec.get("batch_size") or 100))
    print("%sstart rows=%s batch_size=%s" % (label, len(chunk), batch_size), flush=True)
    tgt = connect_target(cfg, for_apply=True)
    ok = skip = 0
    try:
        if batch_size <= 1:
            for i, row in enumerate(chunk, 1):
                try:
                    result = apply_one(tgt, row)
                    tgt.commit()
                except Exception as exc:
                    print("%srow %s err=%s reconnect" % (label, i, exc), flush=True)
                    try:
                        tgt.rollback()
                    except Exception:
                        pass
                    tgt = _reconnect(cfg, tgt)
                    result = apply_one(tgt, row)
                    tgt.commit()
                if result == "ok":
                    ok += 1
                else:
                    skip += 1
                    stats[result] = stats.get(result, 0) + 1
                if i == 1 or i % 200 == 0 or i == len(chunk):
                    print(
                        "%sprogress %s/%s ok=%s skip=%s"
                        % (label, i, len(chunk), ok, skip),
                        flush=True,
                    )
        else:
            total_batches = (len(chunk) + batch_size - 1) // batch_size
            for bi in range(0, len(chunk), batch_size):
                part = chunk[bi : bi + batch_size]
                bno = bi // batch_size + 1
                try:
                    bok, bskip, bstats = apply_batch(tgt, part)
                    tgt.commit()
                except Exception as exc:
                    print("%sbatch %s err=%s reconnect" % (label, bno, exc), flush=True)
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
        "apply workers=%s rows=%s batch_size=%s"
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
    print("parallel done ok=%s skip=%s stats=%s" % (ok, skip, stats), flush=True)
    return ok, skip, stats


def parse_exclude_ids(raw: str) -> Tuple[int, ...]:
    if not raw or not raw.strip():
        return DEFAULT_EXCLUDE_APP_IDS
    out = []
    for part in raw.split(","):
        part = part.strip()
        if part:
            out.append(int(part))
    return tuple(sorted(set(out)))


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(
        description="Fix application.sn to match application_no suffix (market applicationNo)"
    )
    p.add_argument("--env", default=str(HERE / "ng_migration.env"))
    p.add_argument("--apply", action="store_true")
    p.add_argument("--apply-only", action="store_true")
    p.add_argument(
        "--apply-dup-only",
        action="store_true",
        help="只执行 dup-plan-file 中的 delete（删错 sn 重复行）",
    )
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--plan-file", default="/tmp/fix_app_sn_plan.json")
    p.add_argument(
        "--cache-file",
        default="/tmp/application_sn_snapshot.json",
        help="全量快照缓存（仅 application_no/mobile/group_user_id/sn）",
    )
    p.add_argument("--page-size", type=int, default=50000)
    p.add_argument("--workers", type=int, default=4, help="DB apply 并行进程数")
    p.add_argument(
        "--local-workers",
        type=int,
        default=1,
        help="已弃用：本地扫描固定单遍，该参数仅保留兼容",
    )
    p.add_argument("--batch-size", type=int, default=100)
    p.add_argument(
        "--exclude-app-ids",
        default=",".join(str(x) for x in DEFAULT_EXCLUDE_APP_IDS),
    )
    p.add_argument(
        "--disbursed-only",
        action="store_true",
        help="只处理 disbursed_time>0（默认全表）",
    )
    p.add_argument(
        "--analyze-cache",
        action="store_true",
        help="只分析 cache-file，不连库、不写 plan",
    )
    p.add_argument(
        "--build-dup-app-no-plan",
        action="store_true",
        help="从 cache 生成 application_no_multi_sn 修复 plan",
    )
    p.add_argument(
        "--dup-plan-file",
        default="/tmp/fix_dup_app_no_plan.json",
        help="application_no 重复 sn 的 plan 输出路径",
    )
    p.add_argument("--work-limit", type=int, default=0)
    args = p.parse_args(argv)
    if args.apply and args.dry_run:
        p.error("use either --apply or --dry-run")
    if args.apply_only:
        args.apply = True
    if args.apply_dup_only:
        args.apply = True
    dry_run = not args.apply

    plan_path = Path(args.plan_file)
    cache_path = Path(args.cache_file)
    dup_path = Path(args.dup_plan_file)
    cfg = load_env(Path(args.env))
    env_path = str(Path(args.env).resolve())
    exclude_ids = parse_exclude_ids(args.exclude_app_ids)

    if args.apply_dup_only:
        if not dup_path.is_file():
            p.error("--apply-dup-only requires dup plan: %s" % dup_path)
        dup_plan = json.loads(dup_path.read_text(encoding="utf-8"))
        print("apply-dup-only loaded plan=%s" % len(dup_plan), flush=True)
        if args.work_limit > 0:
            dup_plan = dup_plan[: args.work_limit]
        if not dup_plan:
            return 0
        if dry_run:
            print("dry-run only (use --apply-dup-only without --dry-run)", flush=True)
            return 0
        ok, skip, skip_stats = run_parallel_dup(
            dup_plan, args.workers, env_path, max(1, args.batch_size)
        )
        print("done ok=%s skip=%s stats=%s" % (ok, skip, skip_stats), flush=True)
        return 0 if ok or skip else 1

    if args.analyze_cache:
        if not cache_path.is_file():
            p.error("cache not found: %s" % cache_path)
        rows = load_snapshot_cache(cache_path)
        analyze_snapshot(rows, args.local_workers)
        return 0

    if args.build_dup_app_no_plan:
        if not cache_path.is_file():
            p.error("cache not found: %s (run dry-run first or pass --cache-file)" % cache_path)
        rows = load_snapshot_cache(cache_path)
        dup_plan, dup_stats = build_dup_application_no_plan(rows, args.local_workers)
        if args.work_limit > 0:
            dup_plan = dup_plan[: args.work_limit]
        dup_path.write_text(
            json.dumps(dup_plan, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        print("wrote dup_plan_file=%s rows=%s" % (dup_path, len(dup_plan)), flush=True)
        return 0

    if args.apply_only:
        if not plan_path.is_file():
            p.error("--apply-only requires plan file: %s" % plan_path)
        plan = json.loads(plan_path.read_text(encoding="utf-8"))
        print("apply-only loaded plan=%s" % len(plan), flush=True)
    else:
        tgt = connect_target(cfg)
        try:
            rows = load_applications(
                tgt,
                exclude_ids,
                max(1000, args.page_size),
                args.disbursed_only,
            )
        finally:
            tgt.close()
        cache_path.write_text(
            json.dumps(rows, ensure_ascii=False), encoding="utf-8"
        )
        print("wrote cache_file=%s rows=%s" % (cache_path, len(rows)), flush=True)
        plan, stats = build_plan(rows, args.local_workers)
        if args.work_limit > 0:
            plan = plan[: args.work_limit]
        plan_path.write_text(
            json.dumps(plan, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        print("wrote plan_file=%s rows=%s" % (plan_path, len(plan)), flush=True)

    for row in plan[:15]:
        print(
            "  %s  sn %s -> %s  mobile=%s gid=%s"
            % (
                row["application_no"],
                row["bad_sn"],
                row["good_sn"],
                row["mobile"],
                row["group_user_id"],
            ),
            flush=True,
        )
    if len(plan) > 15:
        print("  ... and %s more" % (len(plan) - 15), flush=True)
    if not plan:
        return 0
    if dry_run and not args.apply_only:
        print("dry-run only (use --apply or --apply-only)", flush=True)
        return 0

    ok, skip, skip_stats = run_parallel(
        plan, args.workers, env_path, max(1, args.batch_size)
    )
    print("done ok=%s skip=%s skip_stats=%s" % (ok, skip, skip_stats), flush=True)
    return 0 if ok or skip else 1


if __name__ == "__main__":
    raise SystemExit(main())
