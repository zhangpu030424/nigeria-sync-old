#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""修复目标库 application 短号脏数据（application_no 后缀误用 core.sn）。

流程（与手工一致）:
  1. 目标库查出短号:
       application_no LIKE 'ng%-%'
       AND CHAR_LENGTH(SUBSTRING_INDEX(application_no,'-',-1)) < min_len
  2. 用短号去源库:
       SELECT sn, ext_sn FROM ng_loan_core.application WHERE sn IN (...)
     拿到贷超长号 ext_sn
  3. 拼 good_application_no = ng{app_id:04d}-{ext_sn}
  4. 目标库已有 sn=ext_sn（贷超长号）→ DELETE 短号脏行
     没有 → UPDATE application_no + sn 为正确值
  5. 同时把 loan.application_no 从短号改到长号（避免借据悬空）

Usage:
  # 只出 plan
  python3 repair_short_application_no_from_core.py --env ./ng_migration.env \\
    --plan-file /tmp/repair_short_app_no_plan.jsonl

  # 写库
  python3 repair_short_application_no_from_core.py --env ./ng_migration.env --apply \\
    --plan-file /tmp/repair_short_app_no_plan.jsonl --workers 8 --batch 200

  # 已有 plan 只 apply
  python3 repair_short_application_no_from_core.py --env ./ng_migration.env --apply-only \\
    --plan-file /tmp/repair_short_app_no_plan.jsonl
"""
from __future__ import print_function

import argparse
import json
import sys
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pymysql
from pymysql.cursors import DictCursor

HERE = Path(__file__).resolve().parent
DEFAULT_MIN_SUFFIX_LEN = 15
_TRANSIENT_ERRNOS = frozenset({0, 1159, 1205, 2006, 2013, 2014, 2055})


def _is_transient_mysql_error(exc: BaseException) -> bool:
    if isinstance(exc, pymysql.err.InterfaceError):
        return True
    if isinstance(exc, pymysql.err.OperationalError):
        if not exc.args:
            return True
        return exc.args[0] in _TRANSIENT_ERRNOS
    return False


def load_env(path: Path) -> Dict[str, str]:
    cfg = {}  # type: Dict[str, str]
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        cfg[k.strip()] = v.strip().strip("'\"")
    return cfg


def _cfg_get(cfg: Dict[str, str], *keys: str, default: str = "") -> str:
    for k in keys:
        if k in cfg and str(cfg[k]).strip():
            return str(cfg[k]).strip()
        up = k.upper()
        if up in cfg and str(cfg[up]).strip():
            return str(cfg[up]).strip()
    return default


def connect_target(cfg: Dict[str, str]):
    return pymysql.connect(
        host=_cfg_get(cfg, "target_host", "TARGET_HOST"),
        port=int(_cfg_get(cfg, "target_port", "TARGET_PORT", default="3306")),
        user=_cfg_get(cfg, "target_user", "TARGET_USER"),
        password=_cfg_get(cfg, "target_password", "TARGET_PASSWORD"),
        database=_cfg_get(cfg, "target_db", "TARGET_DB", default="ng"),
        charset="utf8mb4",
        cursorclass=DictCursor,
        connect_timeout=30,
        read_timeout=600,
        write_timeout=600,
        autocommit=False,
    )


def connect_source(cfg: Dict[str, str]):
    return pymysql.connect(
        host=_cfg_get(cfg, "source_host", "SOURCE_HOST"),
        port=int(_cfg_get(cfg, "source_port", "SOURCE_PORT", default="3306")),
        user=_cfg_get(cfg, "source_user", "SOURCE_USER"),
        password=_cfg_get(cfg, "source_password", "SOURCE_PASSWORD"),
        charset="utf8mb4",
        cursorclass=DictCursor,
        connect_timeout=30,
        read_timeout=600,
        write_timeout=600,
        autocommit=False,
    )


def format_application_no(app_id: Any, market_no: Any) -> str:
    tail = str(market_no or "").strip()
    if not tail:
        return ""
    try:
        app_id_int = int(app_id or 0)
    except (TypeError, ValueError):
        app_id_int = 0
    return "ng%04d-%s" % (app_id_int, tail)


def app_suffix(application_no: str) -> str:
    s = str(application_no or "").strip()
    if "-" not in s:
        return ""
    return s.rsplit("-", 1)[-1].strip()


def load_short_applications(tgt, min_suffix_len: int) -> List[dict]:
    sql = """
        SELECT application_no, mobile, group_user_id, sn, app_id,
               SUBSTRING_INDEX(application_no, '-', -1) AS short_sn
        FROM application
        WHERE application_no LIKE 'ng%%-%%'
          AND CHAR_LENGTH(SUBSTRING_INDEX(application_no, '-', -1)) < %s
        ORDER BY mobile, group_user_id, sn
    """
    with tgt.cursor() as cur:
        cur.execute(sql, (int(min_suffix_len),))
        rows = list(cur.fetchall())
    out = []
    for r in rows:
        out.append({
            "application_no": str(r.get("application_no") or "").strip(),
            "mobile": str(r.get("mobile") or "").strip(),
            "group_user_id": r.get("group_user_id"),
            "sn": str(r.get("sn") or "").strip(),
            "app_id": r.get("app_id"),
            "short_sn": str(r.get("short_sn") or "").strip(),
        })
    return out


def _chunk_list(items: Sequence[str], size: int) -> List[List[str]]:
    n = max(1, int(size))
    return [list(items[i:i + n]) for i in range(0, len(items), n)]


def _lookup_target_sns_with_conn(tgt, sns: List[str]) -> set:
    found = set()
    if not sns:
        return found
    ph = ",".join(["%s"] * len(sns))
    with tgt.cursor() as cur:
        cur.execute(
            "SELECT sn FROM application WHERE sn IN ({0})".format(ph),
            sns,
        )
        for row in cur.fetchall():
            found.add(str(row.get("sn") or "").strip())
    return found


def _lookup_target_sns_batch(cfg: Dict[str, str], sns: List[str]) -> set:
    """单批按 sn 查；断线重试，仍失败则对半拆。"""
    if not sns:
        return set()
    last_exc = None  # type: Optional[BaseException]
    for attempt in range(4):
        tgt = connect_target(cfg)
        try:
            return _lookup_target_sns_with_conn(tgt, sns)
        except Exception as exc:
            last_exc = exc
            try:
                tgt.ping(reconnect=True)
            except Exception:
                pass
            if attempt < 3 and _is_transient_mysql_error(exc):
                time.sleep(1.0 + attempt * 2.0)
                continue
            break
        finally:
            try:
                tgt.close()
            except Exception:
                pass
    if len(sns) > 1:
        mid = len(sns) // 2
        left = _lookup_target_sns_batch(cfg, sns[:mid])
        right = _lookup_target_sns_batch(cfg, sns[mid:])
        left.update(right)
        return left
    if last_exc is not None:
        raise last_exc
    return set()


def _lookup_sns_worker_chunks(cfg: Dict[str, str], chunks: List[List[str]], wid: int) -> set:
    """单 worker 单连接顺序按 sn 查多批。"""
    found = set()
    if not chunks:
        return found
    tgt = connect_target(cfg)
    total = len(chunks)
    try:
        for i, chunk in enumerate(chunks, 1):
            for attempt in range(4):
                try:
                    tgt.ping(reconnect=True)
                    found.update(_lookup_target_sns_with_conn(tgt, chunk))
                    break
                except Exception as exc:
                    try:
                        tgt.rollback()
                    except Exception:
                        pass
                    if attempt < 3 and _is_transient_mysql_error(exc):
                        time.sleep(1.0 + attempt * 2.0)
                        try:
                            tgt.ping(reconnect=True)
                        except Exception:
                            tgt = connect_target(cfg)
                        continue
                    part = _lookup_target_sns_batch(cfg, chunk)
                    found.update(part)
                    break
            if i == 1 or i % 10 == 0 or i == total:
                print(
                    "  [w%s] progress batches=%s/%s found_sn=%s"
                    % (wid, i, total, len(found)),
                    flush=True,
                )
    finally:
        try:
            tgt.close()
        except Exception:
            pass
    return found


def target_existing_sns(
    cfg: Dict[str, str],
    sns: Sequence[str],
    workers: int = 8,
    batch_size: int = 200,
) -> set:
    """目标库分批查 sn 是否已存在（贷超长号）。"""
    uniq = sorted({str(x).strip() for x in sns if str(x).strip()})
    if not uniq:
        return set()
    workers = max(1, min(int(workers), 16))
    batch_size = max(50, min(int(batch_size), 500))
    chunks = _chunk_list(uniq, batch_size)
    found = set()
    t0 = time.time()
    print(
        "lookup target existing sn: uniq=%s batches=%s workers=%s batch=%s ..."
        % (len(uniq), len(chunks), workers, batch_size),
        flush=True,
    )
    if workers == 1:
        found.update(_lookup_sns_worker_chunks(cfg, chunks, 0))
    else:
        worker_chunks = [[] for _ in range(workers)]  # type: List[List[List[str]]]
        for i, chunk in enumerate(chunks):
            worker_chunks[i % workers].append(chunk)
        worker_chunks = [c for c in worker_chunks if c]
        with ThreadPoolExecutor(max_workers=len(worker_chunks)) as pool:
            futs = [
                pool.submit(_lookup_sns_worker_chunks, cfg, wc, i)
                for i, wc in enumerate(worker_chunks)
            ]
            for fut in as_completed(futs):
                found.update(fut.result())
    print(
        "  lookup done existing_sn=%s elapsed=%.1fs"
        % (len(found), time.time() - t0),
        flush=True,
    )
    return found


def fetch_ext_sn_by_core_sn(
    cfg: Dict[str, str],
    core_sns: Sequence[str],
    workers: int = 8,
    batch_size: int = 500,
) -> Dict[str, List[str]]:
    """core.sn -> [ext_sn, ...]；多线程分批查源库。"""
    uniq = sorted({str(x).strip() for x in core_sns if str(x).strip()})
    if not uniq:
        return {}
    workers = max(1, min(int(workers), 16))
    batch_size = max(50, int(batch_size))
    chunks = _chunk_list(uniq, batch_size)
    c = "ng_loan_core"

    def _one_batch(part: List[str]) -> Dict[str, List[str]]:
        local = {}  # type: Dict[str, List[str]]
        if not part:
            return local
        src = connect_source(cfg)
        try:
            ph = ",".join(["%s"] * len(part))
            sql = (
                "SELECT sn, ext_sn FROM `{0}`.application "
                "WHERE sn IN ({1}) "
                "AND ext_sn IS NOT NULL AND ext_sn <> ''"
            ).format(c, ph)
            with src.cursor() as cur:
                cur.execute(sql, part)
                for row in cur.fetchall():
                    sn = str(row.get("sn") or "").strip()
                    ext = str(row.get("ext_sn") or "").strip()
                    if not sn or not ext:
                        continue
                    local.setdefault(sn, [])
                    if ext not in local[sn]:
                        local[sn].append(ext)
        finally:
            try:
                src.close()
            except Exception:
                pass
        return local

    out = {}  # type: Dict[str, List[str]]
    t0 = time.time()
    print(
        "lookup source ext_sn: uniq=%s batches=%s workers=%s ..."
        % (len(uniq), len(chunks), workers),
        flush=True,
    )
    done = 0
    if workers == 1 or len(chunks) <= 1:
        for chunk in chunks:
            part = _one_batch(chunk)
            for k, v in part.items():
                out.setdefault(k, [])
                for x in v:
                    if x not in out[k]:
                        out[k].append(x)
            done += 1
    else:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            for part in pool.map(_one_batch, chunks):
                for k, v in part.items():
                    out.setdefault(k, [])
                    for x in v:
                        if x not in out[k]:
                            out[k].append(x)
                done += 1
                if done == 1 or done % 20 == 0 or done == len(chunks):
                    print(
                        "  source progress batches=%s/%s mapped=%s elapsed=%.1fs"
                        % (done, len(chunks), len(out), time.time() - t0),
                        flush=True,
                    )
    return out


def build_plan(
    short_rows: List[dict],
    ext_map: Dict[str, List[str]],
    existing_sns: set,
) -> Tuple[List[dict], Counter]:
    plan = []  # type: List[dict]
    stats = Counter()  # type: Counter
    total = len(short_rows)
    t0 = time.time()
    for i, row in enumerate(short_rows, 1):
        stats["short_total"] += 1
        short_sn = row["short_sn"] or app_suffix(row["application_no"])
        if not short_sn:
            stats["skip_no_short_sn"] += 1
            continue
        exts = ext_map.get(short_sn) or []
        if not exts:
            stats["skip_no_ext_sn"] += 1
            plan.append({
                "action": "skip",
                "reason": "no_ext_sn",
                "application_no": row["application_no"],
                "mobile": row["mobile"],
                "group_user_id": row["group_user_id"],
                "sn": row["sn"],
                "short_sn": short_sn,
                "app_id": row["app_id"],
            })
            continue
        if len(exts) > 1:
            stats["skip_multi_ext_sn"] += 1
            plan.append({
                "action": "skip",
                "reason": "multi_ext_sn",
                "application_no": row["application_no"],
                "mobile": row["mobile"],
                "group_user_id": row["group_user_id"],
                "sn": row["sn"],
                "short_sn": short_sn,
                "ext_sns": exts,
                "app_id": row["app_id"],
            })
            continue
        ext_sn = exts[0]
        good_app_no = format_application_no(row.get("app_id"), ext_sn)
        if not good_app_no:
            stats["skip_bad_app_id"] += 1
            continue
        if good_app_no == row["application_no"]:
            stats["skip_already_good"] += 1
            continue
        if ext_sn in existing_sns:
            action = "delete"
            stats["plan_delete"] += 1
        else:
            action = "update"
            stats["plan_update"] += 1
            existing_sns.add(ext_sn)
        plan.append({
            "action": action,
            "reason": "long_sn_exists" if action == "delete" else "long_sn_missing",
            "application_no": row["application_no"],
            "mobile": row["mobile"],
            "group_user_id": row["group_user_id"],
            "sn": row["sn"],
            "short_sn": short_sn,
            "ext_sn": ext_sn,
            "good_application_no": good_app_no,
            "good_sn": ext_sn,
            "app_id": row["app_id"],
        })
        if i == 1 or i % 10000 == 0 or i == total:
            print(
                "  build_plan progress %s/%s elapsed=%.1fs"
                % (i, total, time.time() - t0),
                flush=True,
            )
    return plan, stats


def write_jsonl(path: Path, rows: Sequence[dict]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    n = 0
    with path.open("w", encoding="utf-8") as fp:
        for row in rows:
            fp.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")
            n += 1
    return n


def read_jsonl(path: Path) -> List[dict]:
    rows = []
    with path.open("r", encoding="utf-8") as fp:
        for line in fp:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def _ping(conn) -> None:
    try:
        conn.ping(reconnect=True)
    except Exception:
        pass


def session_opts_apply(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("SET SESSION unique_checks=0")
        cur.execute("SET SESSION foreign_key_checks=0")
        try:
            cur.execute("SET SESSION sql_log_bin=0")
        except Exception:
            pass
    conn.commit()


def apply_delete_batch_by_pk(tgt, rows: List[dict]) -> Tuple[int, int]:
    """按主键 (mobile, group_user_id, sn) 一批 DELETE。"""
    if not rows:
        return 0, 0
    parts = []
    params = []  # type: List
    for r in rows:
        parts.append("SELECT %s AS mobile, %s AS gid, %s AS sn")
        params.extend([r["mobile"], int(r["group_user_id"]), r["sn"]])
    sql = (
        "DELETE a FROM application a INNER JOIN ("
        + " UNION ALL ".join(parts)
        + ") x ON a.mobile=x.mobile AND a.group_user_id=x.gid AND a.sn=x.sn"
    )
    last_exc = None  # type: Optional[BaseException]
    for attempt in range(4):
        try:
            _ping(tgt)
            with tgt.cursor() as cur:
                cur.execute(sql, tuple(params))
                n = int(cur.rowcount or 0)
            return n, max(0, len(rows) - n)
        except Exception as exc:
            last_exc = exc
            try:
                tgt.rollback()
            except Exception:
                pass
            if attempt < 3 and _is_transient_mysql_error(exc):
                time.sleep(1.0 + attempt * 2.0)
                continue
            break
    if len(rows) > 1:
        mid = len(rows) // 2
        n1, s1 = apply_delete_batch_by_pk(tgt, rows[:mid])
        n2, s2 = apply_delete_batch_by_pk(tgt, rows[mid:])
        return n1 + n2, s1 + s2
    if last_exc is not None:
        raise last_exc
    return 0, len(rows)


def apply_loan_fix_batch(tgt, rows: List[dict]) -> int:
    """loan.application_no 短号 → 长号（一批）。"""
    pairs = []
    seen = set()
    for r in rows:
        old_app = str(r.get("application_no") or "").strip()
        good_app = str(r.get("good_application_no") or "").strip()
        if not old_app or not good_app or old_app == good_app:
            continue
        key = (old_app, good_app)
        if key in seen:
            continue
        seen.add(key)
        pairs.append(key)
    if not pairs:
        return 0
    parts = []
    params = []  # type: List
    for bad_app, good_app in pairs:
        parts.append("SELECT %s AS bad_app, %s AS good_app")
        params.extend([bad_app, good_app])
    sql = (
        "UPDATE loan l INNER JOIN ("
        + " UNION ALL ".join(parts)
        + ") x ON l.application_no=x.bad_app SET l.application_no=x.good_app"
    )
    _ping(tgt)
    with tgt.cursor() as cur:
        cur.execute(sql, tuple(params))
        return int(cur.rowcount or 0)


def apply_one(tgt, item: dict, fix_loan: bool) -> str:
    action = item.get("action")
    if action == "skip":
        return "skip"
    mobile = item["mobile"]
    gid = item["group_user_id"]
    old_sn = item["sn"]
    old_app = item["application_no"]
    good_app = item["good_application_no"]
    good_sn = item["good_sn"]

    with tgt.cursor() as cur:
        if action == "delete":
            cur.execute(
                """
                DELETE FROM application
                WHERE mobile=%s AND group_user_id=%s AND sn=%s
                """,
                (mobile, gid, old_sn),
            )
            n = int(cur.rowcount or 0)
            if n <= 0:
                return "skip_missing"
            if fix_loan:
                # 短号借据改挂到已存在的长号
                cur.execute(
                    """
                    UPDATE loan SET application_no=%s
                    WHERE application_no=%s
                    """,
                    (good_app, old_app),
                )
            return "deleted"

        if action == "update":
            # 再确认一次长 sn 未出现（并发）
            cur.execute(
                "SELECT 1 AS ok FROM application WHERE sn=%s LIMIT 1",
                (good_sn,),
            )
            if cur.fetchone():
                cur.execute(
                    """
                    DELETE FROM application
                    WHERE mobile=%s AND group_user_id=%s AND sn=%s
                      AND application_no=%s
                    """,
                    (mobile, gid, old_sn, old_app),
                )
                n = int(cur.rowcount or 0)
                if fix_loan and n > 0:
                    cur.execute(
                        "UPDATE loan SET application_no=%s WHERE application_no=%s",
                        (good_app, old_app),
                    )
                return "deleted_race" if n > 0 else "skip_missing"

            # PK 含 sn：先看 (mobile,gid,good_sn) 是否已占
            cur.execute(
                """
                SELECT application_no FROM application
                WHERE mobile=%s AND group_user_id=%s AND sn=%s
                LIMIT 1
                """,
                (mobile, gid, good_sn),
            )
            pk_hit = cur.fetchone()
            if pk_hit:
                # 主键已存在 → 删脏行，loan 改挂
                cur.execute(
                    """
                    DELETE FROM application
                    WHERE mobile=%s AND group_user_id=%s AND sn=%s
                      AND application_no=%s
                    """,
                    (mobile, gid, old_sn, old_app),
                )
                n = int(cur.rowcount or 0)
                if fix_loan and n > 0:
                    cur.execute(
                        "UPDATE loan SET application_no=%s WHERE application_no=%s",
                        (good_app, old_app),
                    )
                return "deleted_pk_exists" if n > 0 else "skip_missing"

            cur.execute(
                """
                UPDATE application
                SET application_no=%s, sn=%s
                WHERE mobile=%s AND group_user_id=%s AND sn=%s
                  AND application_no=%s
                """,
                (good_app, good_sn, mobile, gid, old_sn, old_app),
            )
            n = int(cur.rowcount or 0)
            if n <= 0:
                return "skip_missing"
            if fix_loan:
                cur.execute(
                    "UPDATE loan SET application_no=%s WHERE application_no=%s",
                    (good_app, old_app),
                )
            return "updated"

    return "skip"


def apply_chunk(
    cfg: Dict[str, str],
    chunk: List[dict],
    fix_loan: bool,
    wid: int,
    batch_size: int,
) -> Counter:
    stats = Counter()
    if not chunk:
        return stats
    batch_size = max(1, int(batch_size))
    deletes = [x for x in chunk if x.get("action") == "delete"]
    updates = [x for x in chunk if x.get("action") == "update"]

    tgt = connect_target(cfg)
    session_opts_apply(tgt)
    try:
        total_del = len(deletes)
        for bi in range(0, total_del, batch_size):
            batch = deletes[bi:bi + batch_size]
            bno = bi // batch_size + 1
            total_batches = (total_del + batch_size - 1) // batch_size
            try:
                n, skip = apply_delete_batch_by_pk(tgt, batch)
                if fix_loan and n > 0:
                    apply_loan_fix_batch(tgt, batch)
                tgt.commit()
                stats["deleted"] += n
                stats["skip_missing"] += skip
            except Exception as exc:
                try:
                    tgt.rollback()
                except Exception:
                    pass
                stats["error"] += 1
                print(
                    "[w%s] delete batch %s/%s failed: %s"
                    % (wid, bno, total_batches, exc),
                    flush=True,
                )
            if bno == 1 or bno % 10 == 0 or bno == total_batches:
                print(
                    "[w%s] delete progress batches=%s/%s rows=%s/%s %s"
                    % (
                        wid, bno, total_batches,
                        min(bi + len(batch), total_del), total_del,
                        dict(stats),
                    ),
                    flush=True,
                )

        for i, item in enumerate(updates, 1):
            try:
                result = apply_one(tgt, item, fix_loan)
                tgt.commit()
                stats[result] += 1
            except Exception as exc:
                try:
                    tgt.rollback()
                except Exception:
                    pass
                stats["error"] += 1
                print(
                    "[w%s] update error app=%s: %s"
                    % (wid, item.get("application_no"), exc),
                    flush=True,
                )
            if i % 50 == 0 or i == len(updates):
                print(
                    "[w%s] update progress %s/%s %s"
                    % (wid, i, len(updates), dict(stats)),
                    flush=True,
                )
    finally:
        try:
            tgt.close()
        except Exception:
            pass
    return stats


def main(argv=None) -> int:
    p = argparse.ArgumentParser(
        description="Repair short application_no using core.application.ext_sn",
    )
    p.add_argument("--env", default=str(HERE / "ng_migration.env"))
    p.add_argument("--plan-file", default="/tmp/repair_short_app_no_plan.jsonl")
    p.add_argument("--min-suffix-len", type=int, default=DEFAULT_MIN_SUFFIX_LEN)
    p.add_argument("--apply", action="store_true", help="生成 plan 后写库")
    p.add_argument("--apply-only", action="store_true", help="只用已有 plan 写库")
    p.add_argument("--workers", type=int, default=8, help="apply 写库并发")
    p.add_argument(
        "--lookup-workers", type=int, default=8,
        help="plan 阶段查目标库并发 worker（每 worker 单连接顺序查），默认 8",
    )
    p.add_argument(
        "--lookup-batch", type=int, default=200,
        help="plan 阶段每批 IN 条数，默认 200",
    )
    p.add_argument("--batch", type=int, default=500, help="apply 每批 DELETE 行数，默认 500")
    p.add_argument(
        "--no-fix-loan",
        action="store_true",
        help="不改 loan.application_no（默认会改）",
    )
    args = p.parse_args(argv)

    env_path = Path(args.env)
    if not env_path.is_file():
        print("env not found: %s" % env_path, file=sys.stderr)
        return 2
    cfg = load_env(env_path)
    plan_path = Path(args.plan_file)
    fix_loan = not args.no_fix_loan

    if not args.apply_only:
        tgt = connect_target(cfg)
        try:
            print(
                "scan short application_no suffix_len < %s ..."
                % args.min_suffix_len,
                flush=True,
            )
            t0 = time.time()
            short_rows = load_short_applications(tgt, args.min_suffix_len)
            print(
                "short rows=%s elapsed=%.1fs" % (len(short_rows), time.time() - t0),
                flush=True,
            )
            core_sns = [r["short_sn"] for r in short_rows]
            t1 = time.time()
            ext_map = fetch_ext_sn_by_core_sn(
                cfg, core_sns,
                workers=args.lookup_workers,
                batch_size=args.lookup_batch,
            )
            print(
                "mapped core_sn=%s elapsed=%.1fs"
                % (len(ext_map), time.time() - t1),
                flush=True,
            )

            print("collect ext_sn from source map ...", flush=True)
            t2 = time.time()
            ext_sns = []
            for row in short_rows:
                exts = ext_map.get(row["short_sn"]) or []
                if len(exts) == 1:
                    ext_sns.append(exts[0])
            print(
                "ext_sn candidates=%s elapsed=%.1fs"
                % (len(ext_sns), time.time() - t2),
                flush=True,
            )
            existing_sns = target_existing_sns(
                cfg, ext_sns,
                workers=args.lookup_workers,
                batch_size=args.lookup_batch,
            )
            print("existing long sn in target=%s" % len(existing_sns), flush=True)

            print("build plan ...", flush=True)
            t3 = time.time()
            plan, stats = build_plan(short_rows, ext_map, existing_sns)
            print("build plan done elapsed=%.1fs" % (time.time() - t3), flush=True)
            n = write_jsonl(plan_path, plan)
            print("plan written %s rows=%s stats=%s" % (plan_path, n, dict(stats)), flush=True)
        finally:
            try:
                tgt.close()
            except Exception:
                pass
    else:
        if not plan_path.is_file():
            print("plan not found: %s" % plan_path, file=sys.stderr)
            return 2
        plan = read_jsonl(plan_path)
        print("loaded plan rows=%s from %s" % (len(plan), plan_path), flush=True)

    if not (args.apply or args.apply_only):
        print("dry-run only (add --apply / --apply-only to write)", flush=True)
        return 0

    if args.apply_only:
        plan = read_jsonl(plan_path)
    else:
        plan = read_jsonl(plan_path)

    work = [x for x in plan if x.get("action") in ("delete", "update")]
    apply_batch = max(50, int(args.batch))
    print(
        "apply start work=%s (delete/update) fix_loan=%s workers=%s batch=%s"
        % (len(work), fix_loan, args.workers, apply_batch),
        flush=True,
    )
    if not work:
        print("nothing to apply", flush=True)
        return 0

    workers = max(1, min(int(args.workers), 32))
    # 均分给 worker
    chunks = [[] for _ in range(workers)]  # type: List[List[dict]]
    for i, item in enumerate(work):
        chunks[i % workers].append(item)
    chunks = [c for c in chunks if c]

    total = Counter()
    t0 = time.time()
    if workers == 1:
        total.update(apply_chunk(cfg, chunks[0], fix_loan, 0, apply_batch))
    else:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futs = [
                pool.submit(apply_chunk, cfg, chunk, fix_loan, i, apply_batch)
                for i, chunk in enumerate(chunks)
            ]
            for fut in as_completed(futs):
                total.update(fut.result())
    print(
        "apply done %s elapsed=%.1fs" % (dict(total), time.time() - t0),
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
