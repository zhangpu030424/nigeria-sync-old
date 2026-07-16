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
  4. 目标库已有该长号 → DELETE 短号脏行
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


def _lookup_target_app_nos_batch(cfg: Dict[str, str], app_nos: List[str]) -> set:
    """单连接查一批 application_no 是否存在。"""
    found = set()
    if not app_nos:
        return found
    tgt = connect_target(cfg)
    try:
        ph = ",".join(["%s"] * len(app_nos))
        with tgt.cursor() as cur:
            cur.execute(
                "SELECT application_no FROM application WHERE application_no IN ({0})".format(ph),
                app_nos,
            )
            for row in cur.fetchall():
                found.add(str(row.get("application_no") or "").strip())
    finally:
        try:
            tgt.close()
        except Exception:
            pass
    return found


def target_has_application_nos(
    cfg: Dict[str, str],
    app_nos: Sequence[str],
    workers: int = 16,
    batch_size: int = 500,
) -> set:
    """多线程分批查目标库长号是否已存在。"""
    uniq = sorted({str(x).strip() for x in app_nos if str(x).strip()})
    if not uniq:
        return set()
    workers = max(1, min(int(workers), 32))
    batch_size = max(50, int(batch_size))
    chunks = _chunk_list(uniq, batch_size)
    found = set()
    done_batches = 0
    t0 = time.time()
    print(
        "lookup target existing long application_no: uniq=%s batches=%s workers=%s batch=%s ..."
        % (len(uniq), len(chunks), workers, batch_size),
        flush=True,
    )
    if workers == 1 or len(chunks) <= 1:
        for i, chunk in enumerate(chunks, 1):
            found.update(_lookup_target_app_nos_batch(cfg, chunk))
            if i == 1 or i % 20 == 0 or i == len(chunks):
                print(
                    "  lookup progress batches=%s/%s found=%s elapsed=%.1fs"
                    % (i, len(chunks), len(found), time.time() - t0),
                    flush=True,
                )
        return found

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(_lookup_target_app_nos_batch, cfg, chunk): len(chunk)
            for chunk in chunks
        }
        for fut in as_completed(futures):
            found.update(fut.result())
            done_batches += 1
            if done_batches == 1 or done_batches % 20 == 0 or done_batches == len(chunks):
                print(
                    "  lookup progress batches=%s/%s found=%s elapsed=%.1fs"
                    % (done_batches, len(chunks), len(found), time.time() - t0),
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
    existing_long: set,
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
        if good_app_no in existing_long:
            action = "delete"
            stats["plan_delete"] += 1
        else:
            action = "update"
            stats["plan_update"] += 1
            existing_long.add(good_app_no)
        plan.append({
            "action": action,
            "reason": "long_exists" if action == "delete" else "long_missing",
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
                  AND application_no=%s
                """,
                (mobile, gid, old_sn, old_app),
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
            # 再确认一次长号未出现（并发）
            cur.execute(
                "SELECT 1 AS ok FROM application WHERE application_no=%s LIMIT 1",
                (good_app,),
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


def apply_chunk(cfg: Dict[str, str], chunk: List[dict], fix_loan: bool, wid: int) -> Counter:
    stats = Counter()
    if not chunk:
        return stats
    tgt = connect_target(cfg)
    try:
        for i, item in enumerate(chunk, 1):
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
                    "[w%s] error app=%s short=%s: %s"
                    % (wid, item.get("application_no"), item.get("short_sn"), exc),
                    flush=True,
                )
            if i % 50 == 0:
                print(
                    "[w%s] progress %s/%s %s"
                    % (wid, i, len(chunk), dict(stats)),
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
        "--lookup-workers", type=int, default=16,
        help="plan 阶段查源/目标库并发，默认 16",
    )
    p.add_argument(
        "--lookup-batch", type=int, default=500,
        help="plan 阶段每批 IN 条数，默认 500",
    )
    p.add_argument("--batch", type=int, default=200, help="apply 日志切分粒度")
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

            print("build candidate long application_no ...", flush=True)
            t2 = time.time()
            candidates = []
            for row in short_rows:
                exts = ext_map.get(row["short_sn"]) or []
                if len(exts) == 1:
                    candidates.append(format_application_no(row.get("app_id"), exts[0]))
            print(
                "candidates=%s elapsed=%.1fs"
                % (len(candidates), time.time() - t2),
                flush=True,
            )
            existing_long = target_has_application_nos(
                cfg, candidates,
                workers=args.lookup_workers,
                batch_size=args.lookup_batch,
            )
            print("existing long application_no=%s" % len(existing_long), flush=True)

            print("build plan ...", flush=True)
            t3 = time.time()
            plan, stats = build_plan(short_rows, ext_map, existing_long)
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
    print(
        "apply start work=%s (delete/update) fix_loan=%s workers=%s"
        % (len(work), fix_loan, args.workers),
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
        total.update(apply_chunk(cfg, chunks[0], fix_loan, 0))
    else:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futs = [
                pool.submit(apply_chunk, cfg, chunk, fix_loan, i)
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
