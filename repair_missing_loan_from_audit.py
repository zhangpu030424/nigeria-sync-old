#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""补 INSERT audit 报告的 missing_loan（已放款 application 无 loan 行）。

输入:
  --issues-csv /tmp/loan_audit_issues_after_repair.csv
    仅处理 issue=missing_loan 的行（含 application_no、core_sn、expected_loan_no）

逻辑:
  1. 从源库 repay_plan（max plan_sn）拉 loan 行，规则同 ng_migration_run / window_upsert
  2. 目标库该 application_no 无任何 loan 行时 INSERT
  3. loan_no 已存在则 skip（幂等）

Usage:
  python3 repair_missing_loan_from_audit.py --env ./ng_migration.env \\
    --issues-csv /tmp/loan_audit_issues_after_repair.csv --preview 10

  python3 repair_missing_loan_from_audit.py --env ./ng_migration.env \\
    --issues-csv /tmp/loan_audit_issues_after_repair.csv --dry-run

  python3 repair_missing_loan_from_audit.py --env ./ng_migration.env \\
    --apply-only --issues-csv /tmp/loan_audit_issues_after_repair.csv \\
    --batch-size 100 --commit-every 20
"""
import argparse
import csv
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import pymysql
from pymysql.cursors import DictCursor

import ng_migration_run as mig
from repair_loan_no_from_audit import CommitTracker, exec_with_retry, loan_exists

HERE = Path(__file__).resolve().parent


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
            out.append(
                {
                    "application_no": app_no,
                    "core_sn": core_sn,
                    "expected_loan_no": exp_ln,
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
    """core_sn -> loan row dict（application_no 来自 job）。"""
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


def print_preview(jobs: List[dict], loan_by_sn: Dict[str, dict], limit: int) -> None:
    n = min(max(1, limit), len(jobs))
    print("preview %s/%s missing_loan jobs:" % (n, len(jobs)), flush=True)
    for job in jobs[:n]:
        sn = job["core_sn"]
        row = loan_by_sn.get(sn)
        print(
            "  app=%s core_sn=%s expected=%s source_loan=%s"
            % (
                job["application_no"],
                sn,
                job.get("expected_loan_no") or "",
                row.get("loan_no") if row else "(no repay_plan)",
            ),
            flush=True,
        )


def build_plan(
    jobs: List[dict], loan_by_sn: Dict[str, dict], tgt
) -> Tuple[List[dict], Dict[str, int]]:
    plan: List[dict] = []
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
        row = loan_by_sn.get(sn)
        if not row:
            skipped["no_source_repay"] = skipped.get("no_source_repay", 0) + 1
            continue
        loan_no = str(row.get("loan_no") or "").strip()
        if not loan_no:
            skipped["empty_loan_no"] = skipped.get("empty_loan_no", 0) + 1
            continue
        if loan_exists(tgt, loan_no):
            skipped["loan_no_exists"] = skipped.get("loan_no_exists", 0) + 1
            continue
        exp = str(job.get("expected_loan_no") or "").strip()
        if exp and exp != loan_no:
            skipped["loan_no_mismatch"] = skipped.get("loan_no_mismatch", 0) + 1
            print(
                "warn loan_no mismatch app=%s audit=%s source=%s (use source)"
                % (app_no, exp, loan_no),
                flush=True,
            )
        plan.append(row)
    return plan, skipped


def apply_plan(
    cfg: Dict[str, str],
    plan: List[dict],
    dry_run: bool,
    batch_size: int,
    commit_every: int,
) -> Tuple[int, int]:
    if not plan:
        return 0, 0
    tgt = connect_target(cfg, for_apply=True)
    ok = skip = 0
    tracker = CommitTracker(tgt, commit_every, dry_run)
    batch_size = max(1, int(batch_size))
    try:
        for i in range(0, len(plan), batch_size):
            part = plan[i : i + batch_size]
            bno = i // batch_size + 1
            if dry_run:
                ok += len(part)
                print(
                    "batch %s would_insert=%s sample=%s"
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
                ok += n
                tracker.note_write()
                if tracker.pending >= commit_every:
                    tracker.flush()
                print(
                    "batch %s inserted=%s batch_rows=%s total_ok=%s"
                    % (bno, n, len(part), ok),
                    flush=True,
                )
            except pymysql.err.IntegrityError as exc:
                tgt.rollback()
                print(
                    "batch %s integrity_error=%s fallback row-by-row"
                    % (bno, exc),
                    flush=True,
                )
                for row in part:
                    ln = row["loan_no"]
                    if loan_exists(tgt, ln):
                        skip += 1
                        continue
                    try:
                        tgt, n = mig._bulk_insert_rows(
                            tgt,
                            cfg,
                            "target",
                            "loan",
                            mig.LOAN_INSERT_COLS,
                            [row],
                            1,
                        )
                        if n:
                            ok += 1
                            tracker.note_write()
                        else:
                            skip += 1
                    except pymysql.err.IntegrityError:
                        tgt.rollback()
                        skip += 1
                tracker.flush()
        tracker.flush()
    finally:
        tgt.close()
    return ok, skip


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(description="INSERT missing loan rows from audit CSV")
    p.add_argument("--env", default=str(HERE / "ng_migration.env"))
    p.add_argument("--apply", action="store_true")
    p.add_argument("--apply-only", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument(
        "--issues-csv",
        default="/tmp/loan_audit_issues_after_repair.csv",
    )
    p.add_argument("--plan-file", default="", help="写出/读取 insert plan json")
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
    default_plan = Path("/tmp/repair_missing_loan_plan.json")
    if not plan_path and (args.apply_only or args.dry_run or args.apply):
        plan_path = default_plan

    plan: List[dict] = []
    if args.apply_only and plan_path and plan_path.exists():
        plan = json.loads(plan_path.read_text(encoding="utf-8"))
        print("apply-only loaded plan_file=%s rows=%s" % (plan_path, len(plan)), flush=True)
        if args.work_limit > 0:
            plan = plan[: args.work_limit]
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
            if args.preview > 0:
                print_preview(jobs, loan_by_sn, args.preview)
                return 0
            plan, skipped = build_plan(jobs, loan_by_sn, tgt)
            if skipped:
                print(
                    "plan_skipped %s"
                    % " ".join("%s=%s" % (k, v) for k, v in sorted(skipped.items())),
                    flush=True,
                )
        finally:
            src.close()
            tgt.close()

        if plan_path:
            plan_path.write_text(
                json.dumps(plan, ensure_ascii=False, indent=2), encoding="utf-8"
            )
            print("wrote plan_file=%s rows=%s" % (plan_path, len(plan)), flush=True)

    print("insert_plan=%s" % len(plan), flush=True)
    for row in plan[:5]:
        print(
            "  %s -> %s" % (row.get("application_no"), row.get("loan_no")),
            flush=True,
        )
    if len(plan) > 5:
        print("  ... and %s more" % (len(plan) - 5), flush=True)

    if not plan:
        return 1

    if not (dry_run or args.apply):
        print("use --dry-run or --apply", flush=True)
        return 0

    print(
        "start mode=%s batch_size=%s commit_every=%s"
        % ("DRY_RUN" if dry_run else "APPLY", args.batch_size, args.commit_every),
        flush=True,
    )
    t0 = time.time()
    cfg = load_env(Path(args.env))
    ok, skip = apply_plan(cfg, plan, dry_run, args.batch_size, args.commit_every)
    print(
        "finished ok=%s skip=%s elapsed=%.1fs"
        % (ok, skip, time.time() - t0),
        flush=True,
    )
    return 0 if ok or skip else 1


if __name__ == "__main__":
    raise SystemExit(main())
