#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""以目标库已放款 application 为基准，全量核对 loan 表（内存对账）。

前提（application 表视为正确）:
  SELECT application_no, app_id, sn
  FROM application
  WHERE app_id NOT IN (567,569,568,571,572,573)
    AND disbursed_time > 0;

每个 application_no 在 loan 表应 **有且仅有 1 条** 对应行。

期望 loan_no（源库 canonical）:
  1. 从 application_no 取 market 后缀，如 ng0562-177702748012033909 → 177702748012033909
  2. 源库:
       SELECT ca.sn, rp.plan_sn
       FROM ng_loan_core.application ca
       JOIN ng_loan_core.repay_plan rp ON rp.sn = ca.sn AND rp.plan_sn = (
         SELECT MAX(plan_sn) FROM ng_loan_core.repay_plan WHERE sn = ca.sn
       )
       WHERE ca.ext_sn = '{market_suffix}';
  3. loan_no = ng-{plan_sn}-{period:02d}{roll_sequence:03d}
     例: ng-217770275191-01000（plan_sn=217770275191, period=1, roll=0）

     **已确认：loan_no 中间段必须用 repay_plan.plan_sn**（不是 core application.sn）。
     ng_migration_run 曾误用 rp.sn，以此脚本核对结果为准。

Usage:
  # 全量核对（只读，写 CSV/JSON）
  python3 audit_loan_disbursed.py --env ./ng_migration.env

  # 抽样
  python3 audit_loan_disbursed.py --env ./ng_migration.env --work-limit 10000

  # 导出修复 SQL（仅 wrong_loan_no / missing 等可自动修的）
  python3 audit_loan_disbursed.py --env ./ng_migration.env \\
    --plan-file /tmp/loan_audit_plan.json --sql-out /tmp/loan_audit_fix.sql
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, DefaultDict, Dict, List, Optional, Set, Tuple

import pymysql
from pymysql.cursors import DictCursor, SSCursor

import ng_migration_run as mig
from repair_loan_no_from_audit import exec_with_retry

HERE = Path(__file__).resolve().parent
DEFAULT_EXCLUDE_APP_IDS = (567, 569, 568, 571, 572, 573)
APP_SUFFIX_RE = re.compile(r"^ng\d+-(.+)$", re.I)

ISSUE_MISSING_LOAN = "missing_loan"
ISSUE_DUPLICATE_LOAN = "duplicate_loan"
ISSUE_WRONG_LOAN_NO = "wrong_loan_no"
ISSUE_WRONG_LOAN_APP = "wrong_loan_application_no"
ISSUE_NO_MARKET_SUFFIX = "no_market_suffix"
ISSUE_NO_CORE_SN = "no_core_sn"
ISSUE_NO_REPAY_PLAN = "no_repay_plan"
ISSUE_SN_PLAN_MISMATCH = "migration_core_sn_loan_no"  # 当前 loan_no 按 core sn 拼的，与 plan_sn 不符


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


def parse_exclude_ids(raw: str) -> Tuple[int, ...]:
    if not raw.strip():
        return DEFAULT_EXCLUDE_APP_IDS
    out: List[int] = []
    for part in raw.split(","):
        part = part.strip()
        if part:
            out.append(int(part))
    return tuple(out) if out else DEFAULT_EXCLUDE_APP_IDS


def load_disbursed_applications(
    tgt, exclude_app_ids: Tuple[int, ...], work_limit: int
) -> List[dict]:
    """目标库：老系统已放款单 application_no 列表。"""
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
    else:
        params = tuple(exclude_app_ids)
    with tgt.cursor() as cur:
        cur.execute(sql, params)
        rows = list(cur.fetchall())
    print("loaded applications=%s" % len(rows), flush=True)
    return rows


def load_all_loans(tgt) -> Tuple[DefaultDict[str, List[dict]], Dict[str, dict]]:
    """loan 全表进内存：application_no -> [rows], loan_no -> row。"""
    by_app: DefaultDict[str, List[dict]] = defaultdict(list)
    by_loan_no: Dict[str, dict] = {}
    sql = """
        SELECT loan_no, application_no, period, roll_sequence, status, due_date
        FROM loan
        WHERE application_no IS NOT NULL AND application_no <> ''
    """
    n = 0
    with tgt.cursor(SSCursor) as cur:
        cur.execute(sql)
        while True:
            batch = cur.fetchmany(50000)
            if not batch:
                break
            for row in batch:
                app_no = str(row["application_no"]).strip()
                ln = str(row["loan_no"]).strip()
                item = {
                    "loan_no": ln,
                    "application_no": app_no,
                    "period": int(row.get("period") or 1),
                    "roll_sequence": int(row.get("roll_sequence") or 0),
                    "status": row.get("status"),
                    "due_date": row.get("due_date"),
                }
                by_app[app_no].append(item)
                by_loan_no[ln] = item
                n += 1
            if n % 200000 == 0:
                print("  loan rows loaded=%s" % n, flush=True)
    print("loaded loan rows=%s unique_app=%s unique_loan_no=%s"
          % (n, len(by_app), len(by_loan_no)), flush=True)
    return by_app, by_loan_no


def fetch_source_repay_by_ext_sn(
    src, ext_sns: List[str]
) -> Dict[str, dict]:
    """market ext_sn -> {core_sn, plan_sn, rp_sn}。"""
    uniq = sorted({str(x).strip() for x in ext_sns if x})
    out: Dict[str, dict] = {}
    if not uniq:
        return out
    c = "ng_loan_core"
    for i in range(0, len(uniq), 2000):
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
    return out


def fetch_core_sn_only(src, ext_sns: List[str]) -> Dict[str, str]:
    """无 repay_plan 时仍可知 core sn。"""
    uniq = sorted({str(x).strip() for x in ext_sns if x})
    out: Dict[str, str] = {}
    if not uniq:
        return out
    c = "ng_loan_core"
    for i in range(0, len(uniq), 2000):
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
    }

    if not suffix:
        issues.append({**base, "issue": ISSUE_NO_MARKET_SUFFIX})
        return issues

    meta = source_meta or {}
    core_sn = meta.get("core_sn") or core_only or ""
    plan_sn = meta.get("plan_sn") or ""
    if not core_sn and not core_only:
        issues.append({**base, "issue": ISSUE_NO_CORE_SN, "market_suffix": suffix})
        return issues
    if not meta and core_only:
        issues.append({
            **base,
            "issue": ISSUE_NO_REPAY_PLAN,
            "core_sn": core_only,
            "market_suffix": suffix,
        })
        return issues

    loan_sn = str(meta.get("plan_sn") or "").strip()
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
    base_exp = {
        **base,
        "expected_loan_no": exp_ln,
        "expected_loan_no_core_sn": exp_ln_core,
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
    elif exp_ln_core and act_ln == exp_ln_core and act_ln != exp_ln:
        issues.append({**row, "issue": ISSUE_SN_PLAN_MISMATCH})

    return issues


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
    p.add_argument("--default-period", type=int, default=1)
    p.add_argument("--default-roll", type=int, default=0)
    p.add_argument("--issues-csv", default="/tmp/loan_audit_issues.csv")
    p.add_argument("--plan-file", default="/tmp/loan_audit_fix_plan.json")
    p.add_argument("--sql-out", default="", help="导出 wrong_loan_no 的 UPDATE SQL")
    p.add_argument("--sql-batch", type=int, default=50)
    p.add_argument("--skip-orphan", action="store_true", help="不扫 orphan loan")
    args = p.parse_args(argv)

    loan_no_sn_field = "plan_sn"  # 已确认 canonical
    exclude_ids = parse_exclude_ids(args.exclude_app_ids)
    t0 = time.time()

    cfg = load_env(Path(args.env))
    tgt = connect_target(cfg)
    try:
        apps = load_disbursed_applications(tgt, exclude_ids, args.work_limit)
        loans_by_app, _loans_by_ln = load_all_loans(tgt)
    finally:
        tgt.close()

    app_set = {str(a["application_no"]).strip() for a in apps}
    suffixes = sorted({market_suffix(str(a["application_no"])) for a in apps})
    suffixes = [s for s in suffixes if s]
    print("unique_market_suffix=%s" % len(suffixes), flush=True)

    src = connect_source(cfg)
    try:
        t1 = time.time()
        repay_meta = fetch_source_repay_by_ext_sn(src, suffixes)
        print(
            "source_repay_hit=%s/%s elapsed=%.1fs"
            % (len(repay_meta), len(suffixes), time.time() - t1),
            flush=True,
        )
        missing_ext = [s for s in suffixes if s not in repay_meta]
        core_only_map = fetch_core_sn_only(src, missing_ext) if missing_ext else {}
        print("source_core_only=%s" % len(core_only_map), flush=True)
    finally:
        src.close()

    issues: List[dict] = []
    for i, app in enumerate(apps, 1):
        app_no = str(app["application_no"]).strip()
        suffix = market_suffix(app_no)
        meta = repay_meta.get(suffix)
        core_only = core_only_map.get(suffix, "")
        issues.extend(
            reconcile_one(
                app,
                loans_by_app.get(app_no, []),
                meta,
                core_only,
                default_period,
                args.default_roll,
            )
        )
        if i % 200000 == 0:
            print("reconcile progress %s/%s" % (i, len(apps)), flush=True)

    if not args.skip_orphan:
        orphans = find_orphan_loans(loans_by_app, app_set)
        issues.extend(orphans)
        print("orphan_loan_rows=%s" % len(orphans), flush=True)

    stats = summarize(issues)
    print("issue_stats=%s" % stats, flush=True)

    write_issues_csv(Path(args.issues_csv), issues)
    print("wrote issues_csv=%s rows=%s" % (args.issues_csv, len(issues)), flush=True)

    fix_plan = build_fix_plan(issues)
    if args.plan_file:
        Path(args.plan_file).write_text(
            json.dumps(fix_plan, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        print("wrote fix_plan=%s rows=%s" % (args.plan_file, len(fix_plan)), flush=True)

    if args.sql_out:
        write_sql_fix(Path(args.sql_out), fix_plan, args.sql_batch)
        print("wrote sql_out=%s" % args.sql_out, flush=True)

    # 干净单：无 issue 或仅有 sn_plan_sn_differ 告警
    bad_apps = {
        str(r["application_no"])
        for r in issues
        if r.get("issue")
        not in (ISSUE_SN_PLAN_MISMATCH,)  # 信息性
        and r.get("application_no")
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
