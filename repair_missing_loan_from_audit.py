#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""修复 audit 报告的 missing_loan（已放款 application 在 loan 表「找不到」）。

常见两类（后者是 delete_dup / 错前缀导致）:
  A. relink: loan 行存在，loan_no 已对，但 application_no 挂错（如 ng20570931-178...）
     → UPDATE loan.application_no 为正确的 ng{appId}-{market}
  B. insert: 目标库完全没有该 application_no / loan_no 的行
     → 从源库 repay_plan INSERT（同 window_upsert）

输入:
  --issues-csv /tmp/loan_audit_issues_after_repair.csv

Usage:
  python3 repair_missing_loan_from_audit.py --env ./ng_migration.env \\
    --issues-csv /tmp/loan_audit_issues_after_repair.csv --preview 10

  python3 repair_missing_loan_from_audit.py --env ./ng_migration.env --dry-run \\
    --issues-csv /tmp/loan_audit_issues_after_repair.csv

  python3 repair_missing_loan_from_audit.py --env ./ng_migration.env --apply-only \\
    --plan-file /tmp/repair_missing_loan_plan.json
"""
import argparse
import csv
import json
import re
import time
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import pymysql
from pymysql.cursors import DictCursor

import ng_migration_run as mig
from repair_loan_no_from_audit import CommitTracker, exec_with_retry, loan_exists

HERE = Path(__file__).resolve().parent
APP_SUFFIX_RE = re.compile(r"^ng\d+-(.+)$", re.I)


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
    return pymysql.connect(
        host=cfg["TARGET_HOST"],
        port=int(cfg.get("TARGET_PORT", "3306")),
        user=cfg["TARGET_USER"],
        password=cfg["TARGET_PASSWORD"],
        database=cfg.get("TARGET_DB", "ng"),
        charset="utf8mb4",
        cursorclass=DictCursor,
        connect_timeout=10,
        read_timeout=120 if for_apply else 3600,
        write_timeout=120 if for_apply else 3600,
        autocommit=False,
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


def load_missing_from_csv(path: Path) -> List[dict]:
    out: List[dict] = []
    with path.open(encoding="utf-8", newline="") as fp:
        for row in csv.DictReader(fp):
            if str(row.get("issue") or "").strip() != "missing_loan":
                continue
            app_no = str(row.get("application_no") or "").strip()
            core_sn = str(row.get("core_sn") or row.get("loan_sn_used") or "").strip()
            exp_ln = str(row.get("expected_loan_no") or "").strip()
            if not app_no or not core_sn:
                continue
            if not exp_ln:
                exp_ln = mig.format_loan_no(core_sn, 1, 0)
            out.append(
                {
                    "application_no": app_no,
                    "core_sn": core_sn,
                    "expected_loan_no": exp_ln,
                    "market_suffix": market_suffix(app_no),
                    "app_id": row.get("app_id"),
                }
            )
    return out


def dedupe_jobs(rows: List[dict]) -> List[dict]:
    seen: Set[str] = set()
    out: List[dict] = []
    for row in rows:
        app_no = row["application_no"]
        if app_no in seen:
            continue
        seen.add(app_no)
        out.append(row)
    out.sort(key=lambda r: r["application_no"])
    return out


def fetch_loan_rows_for_jobs(src, jobs: List[dict]) -> Dict[str, dict]:
    sn_to_app_no = {j["core_sn"]: j["application_no"] for j in jobs if j.get("core_sn")}
    if not sn_to_app_no:
        return {}
    app_to_sn = {v: k for k, v in sn_to_app_no.items()}
    rows = mig._fetch_loan_rows_from_source(src, sn_to_app_no)
    out: Dict[str, dict] = {}
    for row in rows:
        app_no = str(row.get("application_no") or "").strip()
        sn = app_to_sn.get(app_no)
        if sn:
            out[sn] = row
    return out


def application_has_loan(tgt, application_no: str) -> bool:
    with tgt.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM loan WHERE application_no=%s LIMIT 1",
            (application_no,),
        )
        return cur.fetchone() is not None


def application_exists(tgt, application_no: str) -> bool:
    with tgt.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM application WHERE application_no=%s LIMIT 1",
            (application_no,),
        )
        return cur.fetchone() is not None


def find_mislinked_loan(tgt, job: dict) -> Optional[dict]:
    """loan 存在但 application_no 挂错（delete_dup / 错前缀遗留）。"""
    good_app = job["application_no"]
    exp_ln = str(job.get("expected_loan_no") or "").strip()
    suffix = str(job.get("market_suffix") or "").strip()

    if exp_ln:
        with tgt.cursor() as cur:
            cur.execute(
                "SELECT loan_no, application_no FROM loan WHERE loan_no=%s LIMIT 1",
                (exp_ln,),
            )
            row = cur.fetchone()
            if row:
                bad_app = str(row.get("application_no") or "").strip()
                if bad_app and bad_app != good_app:
                    return {
                        "action": "relink",
                        "loan_no": exp_ln,
                        "good_application_no": good_app,
                        "bad_application_no": bad_app,
                        "reason": "loan_no_match",
                    }

    if suffix:
        with tgt.cursor() as cur:
            cur.execute(
                """
                SELECT loan_no, application_no FROM loan
                WHERE application_no LIKE %s AND application_no <> %s
                ORDER BY loan_no ASC
                LIMIT 10
                """,
                ("%%-%s" % suffix, good_app),
            )
            rows = list(cur.fetchall())
        if not rows:
            return None
        if exp_ln:
            for row in rows:
                if str(row.get("loan_no") or "").strip() == exp_ln:
                    bad_app = str(row.get("application_no") or "").strip()
                    return {
                        "action": "relink",
                        "loan_no": exp_ln,
                        "good_application_no": good_app,
                        "bad_application_no": bad_app,
                        "reason": "suffix_and_loan_no",
                    }
        if len(rows) == 1:
            row = rows[0]
            return {
                "action": "relink",
                "loan_no": str(row.get("loan_no") or "").strip(),
                "good_application_no": good_app,
                "bad_application_no": str(row.get("application_no") or "").strip(),
                "reason": "suffix_unique",
            }
    return None


def print_preview(
    jobs: List[dict],
    loan_by_sn: Dict[str, dict],
    relink_plan: List[dict],
    insert_plan: List[dict],
    limit: int,
) -> None:
    n = min(max(1, limit), len(jobs))
    relink_by_app = {r["good_application_no"]: r for r in relink_plan}
    insert_by_app = {r.get("application_no"): r for r in insert_plan}
    print("preview %s/%s missing_loan jobs:" % (n, len(jobs)), flush=True)
    for job in jobs[:n]:
        app = job["application_no"]
        if app in relink_by_app:
            r = relink_by_app[app]
            print(
                "  [relink] %s loan=%s bad_app=%s -> good_app=%s (%s)"
                % (app, r["loan_no"], r["bad_application_no"], r["good_application_no"], r["reason"]),
                flush=True,
            )
        elif app in insert_by_app:
            print(
                "  [insert] %s loan=%s"
                % (app, insert_by_app[app].get("loan_no")),
                flush=True,
            )
        else:
            sn = job["core_sn"]
            row = loan_by_sn.get(sn)
            print(
                "  [skip?] %s core_sn=%s expected=%s source=%s"
                % (
                    app,
                    sn,
                    job.get("expected_loan_no"),
                    row.get("loan_no") if row else "(no repay_plan)",
                ),
                flush=True,
            )


def build_plan(
    jobs: List[dict], loan_by_sn: Dict[str, dict], tgt
) -> Tuple[List[dict], List[dict], Dict[str, int]]:
    relink_plan: List[dict] = []
    insert_plan: List[dict] = []
    skipped: Dict[str, int] = {}

    for job in jobs:
        app_no = job["application_no"]
        sn = job["core_sn"]
        if not application_exists(tgt, app_no):
            skipped["no_application"] = skipped.get("no_application", 0) + 1
            continue
        if application_has_loan(tgt, app_no):
            skipped["loan_exists"] = skipped.get("loan_exists", 0) + 1
            continue

        mislinked = find_mislinked_loan(tgt, job)
        if mislinked:
            relink_plan.append(mislinked)
            continue

        row = loan_by_sn.get(sn)
        if not row:
            skipped["no_source_repay"] = skipped.get("no_source_repay", 0) + 1
            continue
        loan_no = str(row.get("loan_no") or "").strip()
        if not loan_no:
            skipped["empty_loan_no"] = skipped.get("empty_loan_no", 0) + 1
            continue
        if loan_exists(tgt, loan_no):
            skipped["loan_no_orphan"] = skipped.get("loan_no_orphan", 0) + 1
            continue
        insert_plan.append(row)

    return relink_plan, insert_plan, skipped


def apply_relink_batch(tgt, rows: List[dict], dry_run: bool) -> int:
    if not rows:
        return 0
    if dry_run:
        return len(rows)
    parts: List[str] = []
    params: List = []
    for r in rows:
        parts.append("SELECT %s AS loan_no, %s AS bad_app, %s AS good_app")
        params.extend([r["loan_no"], r["bad_application_no"], r["good_application_no"]])
    sql = (
        """
        UPDATE loan l
        INNER JOIN (
        """
        + " UNION ALL ".join(parts)
        + """
        ) x ON l.loan_no = x.loan_no AND l.application_no = x.bad_app
        SET l.application_no = x.good_app
        """
    )
    with tgt.cursor() as cur:
        cur.execute(sql, tuple(params))
        return int(cur.rowcount or 0)


def apply_plan(
    cfg: Dict[str, str],
    relink_plan: List[dict],
    insert_plan: List[dict],
    dry_run: bool,
    batch_size: int,
    commit_every: int,
) -> Tuple[int, int]:
    tgt = connect_target(cfg, for_apply=True)
    ok = skip = 0
    tracker = CommitTracker(tgt, commit_every, dry_run)
    batch_size = max(1, int(batch_size))
    try:
        if relink_plan:
            print("phase relink rows=%s" % len(relink_plan), flush=True)
            for i in range(0, len(relink_plan), batch_size):
                part = relink_plan[i : i + batch_size]
                bno = i // batch_size + 1
                if dry_run:
                    ok += len(part)
                    print(
                        "relink batch %s would_update=%s sample=%s"
                        % (bno, len(part), part[0]),
                        flush=True,
                    )
                    continue
                n = exec_with_retry(
                    tgt,
                    lambda p=part: apply_relink_batch(tgt, p, False),
                    "relink batch %s" % bno,
                )
                tgt.commit()
                ok += n
                skip += len(part) - n
                print(
                    "relink batch %s updated=%s/%s total_ok=%s"
                    % (bno, n, len(part), ok),
                    flush=True,
                )

        if insert_plan:
            print("phase insert rows=%s" % len(insert_plan), flush=True)
            for i in range(0, len(insert_plan), batch_size):
                part = insert_plan[i : i + batch_size]
                bno = i // batch_size + 1
                if dry_run:
                    ok += len(part)
                    print(
                        "insert batch %s would_insert=%s sample=%s"
                        % (bno, len(part), part[0].get("loan_no")),
                        flush=True,
                    )
                    continue
                try:
                    tgt, n = mig._bulk_insert_rows(
                        tgt,
                        cfg,
                        "target",
                        "loan",
                        mig.LOAN_INSERT_COLS,
                        part,
                        batch_size,
                    )
                    tgt.commit()
                    ok += n
                    skip += len(part) - n
                    print(
                        "insert batch %s inserted=%s/%s total_ok=%s"
                        % (bno, n, len(part), ok),
                        flush=True,
                    )
                except pymysql.err.IntegrityError as exc:
                    tgt.rollback()
                    print("insert batch %s integrity_error=%s" % (bno, exc), flush=True)
                    skip += len(part)
        tracker.flush()
    finally:
        tgt.close()
    return ok, skip


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(description="Fix missing_loan: relink wrong app or INSERT")
    p.add_argument("--env", default=str(HERE / "ng_migration.env"))
    p.add_argument("--apply", action="store_true")
    p.add_argument("--apply-only", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument(
        "--issues-csv",
        default="/tmp/loan_audit_issues_after_repair.csv",
    )
    p.add_argument("--plan-file", default="", help="写出/读取 plan json")
    p.add_argument("--preview", type=int, default=0, metavar="N")
    p.add_argument("--work-limit", type=int, default=0)
    p.add_argument("--batch-size", type=int, default=100)
    p.add_argument("--commit-every", type=int, default=20)
    args = p.parse_args(argv)

    if args.apply and args.dry_run:
        p.error("use either --apply or --dry-run")
    if args.apply_only:
        args.apply = True
    dry_run = not args.apply

    path = Path(args.issues_csv)
    if not path.exists():
        print("issues csv not found: %s" % path, flush=True)
        return 1

    jobs = dedupe_jobs(load_missing_from_csv(path))
    print("missing_loan jobs=%s (from %s)" % (len(jobs), path), flush=True)
    if args.work_limit > 0:
        jobs = jobs[: args.work_limit]

    plan_path = Path(args.plan_file) if args.plan_file.strip() else None
    if not plan_path and (args.apply_only or args.dry_run or args.apply):
        plan_path = Path("/tmp/repair_missing_loan_plan.json")

    relink_plan: List[dict] = []
    insert_plan: List[dict] = []

    if args.apply_only and plan_path and plan_path.exists():
        loaded = json.loads(plan_path.read_text(encoding="utf-8"))
        if isinstance(loaded, dict):
            relink_plan = list(loaded.get("relink") or [])
            insert_plan = list(loaded.get("insert") or [])
        else:
            insert_plan = list(loaded)
        print(
            "apply-only loaded relink=%s insert=%s from %s"
            % (len(relink_plan), len(insert_plan), plan_path),
            flush=True,
        )
        if args.work_limit > 0:
            relink_plan = relink_plan[: args.work_limit]
            insert_plan = insert_plan[: args.work_limit]
    else:
        cfg = load_env(Path(args.env))
        src = connect_source(cfg)
        tgt = connect_target(cfg)
        try:
            loan_by_sn = fetch_loan_rows_for_jobs(src, jobs)
            print(
                "source_repay_hit=%s/%s" % (len(loan_by_sn), len(jobs)),
                flush=True,
            )
            relink_plan, insert_plan, skipped = build_plan(jobs, loan_by_sn, tgt)
            if skipped:
                print(
                    "plan_skipped %s"
                    % " ".join("%s=%s" % (k, v) for k, v in sorted(skipped.items())),
                    flush=True,
                )
            if args.preview > 0:
                print_preview(jobs, loan_by_sn, relink_plan, insert_plan, args.preview)
                return 0
        finally:
            src.close()
            tgt.close()

        if plan_path:
            payload = {"relink": relink_plan, "insert": insert_plan}
            plan_path.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
            )
            print(
                "wrote plan_file=%s relink=%s insert=%s"
                % (plan_path, len(relink_plan), len(insert_plan)),
                flush=True,
            )

    print("plan relink=%s insert=%s" % (len(relink_plan), len(insert_plan)), flush=True)
    for row in relink_plan[:3]:
        print(
            "  relink loan=%s %s -> %s (%s)"
            % (
                row["loan_no"],
                row["bad_application_no"],
                row["good_application_no"],
                row.get("reason"),
            ),
            flush=True,
        )
    for row in insert_plan[:3]:
        print(
            "  insert %s -> %s"
            % (row.get("application_no"), row.get("loan_no")),
            flush=True,
        )

    if not relink_plan and not insert_plan:
        return 1

    if not (dry_run or args.apply):
        print("use --dry-run or --apply", flush=True)
        return 0

    print(
        "start mode=%s batch_size=%s"
        % ("DRY_RUN" if dry_run else "APPLY", args.batch_size),
        flush=True,
    )
    t0 = time.time()
    cfg = load_env(Path(args.env))
    ok, skip = apply_plan(
        cfg, relink_plan, insert_plan, dry_run, args.batch_size, args.commit_every
    )
    print(
        "finished ok=%s skip=%s elapsed=%.1fs"
        % (ok, skip, time.time() - t0),
        flush=True,
    )
    return 0 if ok or skip else 1


if __name__ == "__main__":
    raise SystemExit(main())
