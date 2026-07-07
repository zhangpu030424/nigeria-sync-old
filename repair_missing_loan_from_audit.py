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

  # 只分析（默认跳过源库，最快）
  python3 repair_missing_loan_from_audit.py --env ./ng_migration.env \\
    --issues-csv /tmp/loan_audit_issues.csv --analyze

  # 分析时也要 insert 清单（会查源库 repay_plan，较慢）
  python3 repair_missing_loan_from_audit.py --env ./ng_migration.env \\
    --issues-csv /tmp/loan_audit_issues.csv --analyze --with-source
"""
import argparse
import csv
import json
import re
import subprocess
import time
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple

import pymysql
from pymysql.cursors import DictCursor

import ng_migration_run as mig
from repair_loan_no_from_audit import CommitTracker, exec_with_retry

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
        connect_timeout=int(cfg.get("mysql_connect_timeout") or 60),
        read_timeout=int(cfg.get("mysql_read_timeout") or 3600),
        write_timeout=int(cfg.get("mysql_write_timeout") or 3600),
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


def _job_from_issue_row(row: dict) -> Optional[dict]:
    app_no = str(row.get("application_no") or "").strip()
    core_sn = str(row.get("core_sn") or row.get("loan_sn_used") or "").strip()
    exp_ln = str(row.get("expected_loan_no") or "").strip()
    if not app_no or not core_sn:
        return None
    if not exp_ln:
        exp_ln = mig.format_loan_no(core_sn, 1, 0)
    return {
        "application_no": app_no,
        "core_sn": core_sn,
        "expected_loan_no": exp_ln,
        "market_suffix": market_suffix(app_no),
        "app_id": row.get("app_id"),
    }


def _grep_missing_loan_lines(path: Path) -> Iterable[str]:
    """大 CSV 用 grep 先筛 missing_loan 行，避免解析 250 万行。"""
    proc = subprocess.run(
        ["grep", "-F", "missing_loan", str(path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
        check=False,
    )
    if proc.returncode not in (0, 1):
        raise RuntimeError("grep failed: %s" % (proc.stderr or proc.stdout))
    for line in (proc.stdout or "").splitlines():
        if line.strip():
            yield line


def load_missing_from_csv(path: Path, use_grep: bool = True) -> List[dict]:
    out: List[dict] = []
    t0 = time.perf_counter()

    if use_grep and path.stat().st_size > 5_000_000:
        try:
            lines = list(_grep_missing_loan_lines(path))
            if not lines:
                return out
            with path.open(encoding="utf-8", newline="") as fp:
                header = fp.readline().strip()
            fieldnames = next(csv.reader([header]))
            for line in lines:
                if line == header:
                    continue
                row = dict(zip(fieldnames, next(csv.reader([line]))))
                if str(row.get("issue") or "").strip() != "missing_loan":
                    continue
                job = _job_from_issue_row(row)
                if job:
                    out.append(job)
            print(
                "load_missing_from_csv grep lines=%s jobs=%s elapsed=%.1fs"
                % (len(lines), len(out), time.time() - t0),
                flush=True,
            )
            return out
        except (FileNotFoundError, OSError, RuntimeError) as exc:
            print("grep fast-path failed (%s), fallback stream" % exc, flush=True)

    with path.open(encoding="utf-8", newline="") as fp:
        reader = csv.DictReader(fp)
        for row in reader:
            if str(row.get("issue") or "").strip() != "missing_loan":
                continue
            job = _job_from_issue_row(row)
            if job:
                out.append(job)
    print(
        "load_missing_from_csv stream jobs=%s elapsed=%.1fs"
        % (len(out), time.time() - t0),
        flush=True,
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


def _chunks(items: List[str], size: int):
    for i in range(0, len(items), size):
        yield items[i : i + size]


def _ping_tgt(tgt):
    try:
        tgt.ping(reconnect=True)
    except Exception:
        pass


def _mysql_retry(conn, fn, what: str, retries: int = 5):
    for attempt in range(retries):
        try:
            _ping_tgt(conn)
            return fn()
        except pymysql.Error as exc:
            if attempt >= retries - 1:
                raise
            print(
                "%s retry %s/%s err=%s" % (what, attempt + 1, retries, exc),
                flush=True,
            )
            try:
                conn.ping(reconnect=True)
            except Exception:
                pass
            time.sleep(min(2 * (attempt + 1), 10))
    return None


def batch_query_in(
    tgt, table: str, col: str, values: List[str], chunk: int = 500, label: str = ""
) -> Set[str]:
    """SELECT col FROM table WHERE col IN (...)，返回存在的值集合。"""
    uniq = sorted({str(v).strip() for v in values if v})
    found: Set[str] = set()
    if not uniq:
        return found
    total = (len(uniq) + chunk - 1) // chunk
    tag = label or "%s.%s" % (table, col)
    for i, part in enumerate(_chunks(uniq, chunk), 1):
        ph = ",".join(["%s"] * len(part))
        sql = "SELECT `%s` AS v FROM `%s` WHERE `%s` IN (%s)" % (col, table, col, ph)

        def _one_chunk(sql=sql, part=part):
            with tgt.cursor() as cur:
                cur.execute(sql, part)
                return cur.fetchall()

        rows = _mysql_retry(tgt, _one_chunk, "%s %s/%s" % (tag, i, total))
        for row in rows or []:
            found.add(str(row["v"]).strip())
        if i == 1 or i % 20 == 0 or i == total:
            print(
                "  batch_query %s %s/%s hits=%s"
                % (tag, i, total, len(found)),
                flush=True,
            )
    return found


def preload_loan_by_nos(
    tgt, loan_nos: List[str], chunk: int = 500
) -> Dict[str, dict]:
    by_loan_no: Dict[str, dict] = {}
    total_ln = (len(loan_nos) + chunk - 1) // chunk if loan_nos else 0
    for i, part in enumerate(_chunks(loan_nos, chunk), 1):
        ph = ",".join(["%s"] * len(part))

        def _one_chunk(ph=ph, part=part):
            with tgt.cursor() as cur:
                cur.execute(
                    "SELECT loan_no, application_no FROM loan WHERE loan_no IN (%s)" % ph,
                    part,
                )
                return cur.fetchall()

        rows = _mysql_retry(tgt, _one_chunk, "preload loan_no %s/%s" % (i, total_ln))
        for row in rows or []:
            ln = str(row.get("loan_no") or "").strip()
            if ln:
                by_loan_no[ln] = row
        if total_ln and (i == 1 or i % 20 == 0 or i == total_ln):
            print(
                "  preload loan_no %s/%s hits=%s" % (i, total_ln, len(by_loan_no)),
                flush=True,
            )
    return by_loan_no


def preload_loan_by_suffixes(
    tgt, suffixes: List[str], chunk: int = 200
) -> Dict[str, List[dict]]:
    """按 market 后缀查错挂 loan（较慢，仅对 loan_no 未命中的 job 使用）。"""
    by_suffix: Dict[str, List[dict]] = {}
    if not suffixes:
        return by_suffix
    total_sfx = (len(suffixes) + chunk - 1) // chunk
    for i, part in enumerate(_chunks(suffixes, chunk), 1):
        ph = ",".join(["%s"] * len(part))

        def _one_chunk(ph=ph, part=part):
            with tgt.cursor() as cur:
                cur.execute(
                    """
                    SELECT loan_no, application_no,
                           SUBSTRING_INDEX(application_no, '-', -1) AS sfx
                    FROM loan
                    WHERE SUBSTRING_INDEX(application_no, '-', -1) IN (%s)
                    """ % ph,
                    part,
                )
                return cur.fetchall()

        rows = _mysql_retry(tgt, _one_chunk, "preload suffix %s/%s" % (i, total_sfx))
        for row in rows or []:
            sfx = str(row.get("sfx") or "").strip()
            if sfx:
                by_suffix.setdefault(sfx, []).append(row)
        if i == 1 or i % 10 == 0 or i == total_sfx:
            n = sum(len(v) for v in by_suffix.values())
            print(
                "  preload suffix %s/%s rows=%s" % (i, total_sfx, n),
                flush=True,
            )
    return by_suffix


def preload_target_indexes(
    tgt,
    app_nos: List[str],
    loan_nos: List[str],
    chunk: int = 500,
) -> Tuple[Set[str], Set[str], Dict[str, dict]]:
    """串行预加载（单连接，带重试；3787 条规模足够快）。"""
    existing_apps = batch_query_in(
        tgt, "application", "application_no", app_nos, chunk, "application"
    )
    apps_with_loan = batch_query_in(
        tgt, "loan", "application_no", app_nos, chunk, "loan.application_no"
    )
    by_loan_no = preload_loan_by_nos(tgt, loan_nos, chunk)
    return existing_apps, apps_with_loan, by_loan_no


def find_mislinked_in_memory(job: dict, by_loan_no: Dict[str, dict], by_suffix: Dict[str, List[dict]]) -> Optional[dict]:
    """loan 存在但 application_no 挂错（内存索引）。"""
    good_app = job["application_no"]
    exp_ln = str(job.get("expected_loan_no") or "").strip()
    suffix = str(job.get("market_suffix") or "").strip()

    if exp_ln and exp_ln in by_loan_no:
        row = by_loan_no[exp_ln]
        bad_app = str(row.get("application_no") or "").strip()
        if bad_app and bad_app != good_app:
            return {
                "action": "relink",
                "loan_no": exp_ln,
                "good_application_no": good_app,
                "bad_application_no": bad_app,
                "reason": "loan_no_match",
            }

    rows = [r for r in by_suffix.get(suffix, []) if str(r.get("application_no") or "").strip() != good_app]
    if not rows:
        return None
    if exp_ln:
        for row in rows:
            if str(row.get("loan_no") or "").strip() == exp_ln:
                return {
                    "action": "relink",
                    "loan_no": exp_ln,
                    "good_application_no": good_app,
                    "bad_application_no": str(row.get("application_no") or "").strip(),
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
    jobs: List[dict],
    loan_by_sn: Dict[str, dict],
    tgt,
    query_chunk: int = 500,
    suffix_scan: bool = True,
    insert_from_source: bool = True,
) -> Tuple[List[dict], List[dict], Dict[str, int]]:
    relink_plan: List[dict] = []
    insert_plan: List[dict] = []
    skipped: Dict[str, int] = {}

    app_nos = [j["application_no"] for j in jobs]
    loan_nos = sorted(
        {str(j.get("expected_loan_no") or "").strip() for j in jobs if j.get("expected_loan_no")}
    )
    print(
        "preload target indexes (chunk=%s) ..."
        % query_chunk,
        flush=True,
    )
    existing_apps, apps_with_loan, by_loan_no = preload_target_indexes(
        tgt, app_nos, loan_nos, query_chunk
    )
    existing_loan_nos = set(by_loan_no.keys())
    by_suffix: Dict[str, List[dict]] = {}
    need_suffix: List[dict] = []

    for job in jobs:
        app_no = job["application_no"]
        sn = job["core_sn"]
        if app_no not in existing_apps:
            skipped["no_application"] = skipped.get("no_application", 0) + 1
            continue
        if app_no in apps_with_loan:
            skipped["loan_exists"] = skipped.get("loan_exists", 0) + 1
            continue

        mislinked = find_mislinked_in_memory(job, by_loan_no, by_suffix)
        if mislinked:
            relink_plan.append(mislinked)
            continue

        if suffix_scan:
            need_suffix.append(job)
            continue

        if not insert_from_source:
            skipped["pending_insert"] = skipped.get("pending_insert", 0) + 1
            continue

        row = loan_by_sn.get(sn)
        if not row:
            skipped["no_source_repay"] = skipped.get("no_source_repay", 0) + 1
            continue
        loan_no = str(row.get("loan_no") or "").strip()
        if not loan_no:
            skipped["empty_loan_no"] = skipped.get("empty_loan_no", 0) + 1
            continue
        if loan_no in existing_loan_nos:
            skipped["loan_no_orphan"] = skipped.get("loan_no_orphan", 0) + 1
            continue
        insert_plan.append(row)

    if need_suffix:
        suffixes = sorted(
            {str(j.get("market_suffix") or "").strip() for j in need_suffix if j.get("market_suffix")}
        )
        print(
            "lazy suffix scan jobs=%s suffixes=%s ..."
            % (len(need_suffix), len(suffixes)),
            flush=True,
        )
        by_suffix = preload_loan_by_suffixes(tgt, suffixes)
        for job in need_suffix:
            app_no = job["application_no"]
            sn = job["core_sn"]
            mislinked = find_mislinked_in_memory(job, by_loan_no, by_suffix)
            if mislinked:
                relink_plan.append(mislinked)
                continue

            if not insert_from_source:
                skipped["pending_insert"] = skipped.get("pending_insert", 0) + 1
                continue

            row = loan_by_sn.get(sn)
            if not row:
                skipped["no_source_repay"] = skipped.get("no_source_repay", 0) + 1
                continue
            loan_no = str(row.get("loan_no") or "").strip()
            if not loan_no:
                skipped["empty_loan_no"] = skipped.get("empty_loan_no", 0) + 1
                continue
            if loan_no in existing_loan_nos:
                skipped["loan_no_orphan"] = skipped.get("loan_no_orphan", 0) + 1
                continue
            insert_plan.append(row)

    return relink_plan, insert_plan, skipped


def write_relink_csv(path: Path, rows: List[dict]) -> None:
    cols = [
        "good_application_no",
        "bad_application_no",
        "loan_no",
        "reason",
        "core_sn",
        "market_suffix",
    ]
    with path.open("w", encoding="utf-8", newline="") as fp:
        w = csv.DictWriter(fp, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        for row in rows:
            w.writerow(row)


def write_insert_csv(path: Path, rows: List[dict]) -> None:
    cols = ["application_no", "loan_no", "period", "roll_sequence"]
    with path.open("w", encoding="utf-8", newline="") as fp:
        w = csv.DictWriter(fp, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        for row in rows:
            w.writerow(
                {
                    "application_no": row.get("application_no"),
                    "loan_no": row.get("loan_no"),
                    "period": row.get("period"),
                    "roll_sequence": row.get("roll_sequence"),
                }
            )


def enrich_relink_rows(relink_plan: List[dict], jobs: List[dict]) -> List[dict]:
    job_by_app = {j["application_no"]: j for j in jobs}
    out: List[dict] = []
    for row in relink_plan:
        job = job_by_app.get(row.get("good_application_no") or "")
        out.append(
            {
                **row,
                "core_sn": (job or {}).get("core_sn", ""),
                "market_suffix": (job or {}).get("market_suffix", ""),
            }
        )
    return out


def print_analyze_summary(
    relink_plan: List[dict],
    insert_plan: List[dict],
    skipped: Dict[str, int],
    relink_csv: Path,
    insert_csv: Path,
) -> None:
    print("=" * 60, flush=True)
    print("missing_loan analyze summary", flush=True)
    print("  relink (loan存在, application挂错): %s" % len(relink_plan), flush=True)
    print("  insert (确实无loan行):             %s" % len(insert_plan), flush=True)
    pending = int(skipped.get("pending_insert") or 0)
    if pending:
        print(
            "  pending_insert (未查源库):         %s  (加 --with-source 生成 insert 清单)"
            % pending,
            flush=True,
        )
    if skipped:
        print(
            "  skipped: %s"
            % " ".join("%s=%s" % (k, v) for k, v in sorted(skipped.items())),
            flush=True,
        )
    print("  relink_csv:  %s" % relink_csv, flush=True)
    print("  insert_csv:  %s" % insert_csv, flush=True)
    print("-" * 60, flush=True)
    by_reason: Dict[str, int] = {}
    for row in relink_plan:
        r = str(row.get("reason") or "unknown")
        by_reason[r] = by_reason.get(r, 0) + 1
    if by_reason:
        print("relink by reason:", flush=True)
        for k, v in sorted(by_reason.items(), key=lambda x: -x[1]):
            print("  %s: %s" % (k, v), flush=True)
    print("-" * 60, flush=True)
    print("relink samples (first 20):", flush=True)
    for row in relink_plan[:20]:
        print(
            "  loan=%s | bad_app=%s -> good_app=%s (%s)"
            % (
                row.get("loan_no"),
                row.get("bad_application_no"),
                row.get("good_application_no"),
                row.get("reason"),
            ),
            flush=True,
        )
    print("=" * 60, flush=True)


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
        default="/tmp/loan_audit_issues.csv",
        help="audit 输出的 issues CSV（自动筛 issue=missing_loan）",
    )
    p.add_argument("--plan-file", default="", help="写出/读取 plan json")
    p.add_argument("--preview", type=int, default=0, metavar="N")
    p.add_argument(
        "--analyze",
        action="store_true",
        help="只分析：导出 relink（loan在但application挂错）与 insert 清单，不写库",
    )
    p.add_argument(
        "--relink-csv",
        default="/tmp/missing_loan_relink.csv",
        help="--analyze 时写出挂错 application 的 loan 清单",
    )
    p.add_argument(
        "--insert-csv",
        default="/tmp/missing_loan_insert.csv",
        help="--analyze 时写出需 INSERT 的清单",
    )
    p.add_argument("--work-limit", type=int, default=0)
    p.add_argument("--query-chunk", type=int, default=500, help="目标库 IN 查询分批大小")
    p.add_argument(
        "--with-source",
        action="store_true",
        help="分析时也查源库 repay_plan（生成 insert 清单，较慢）",
    )
    p.add_argument(
        "--no-suffix-scan",
        action="store_true",
        help="跳过 suffix 全表扫描（最快，仅 loan_no 精确匹配 relink）",
    )
    p.add_argument(
        "--no-grep-csv",
        action="store_true",
        help="大 CSV 不用 grep 预筛",
    )
    p.add_argument("--batch-size", type=int, default=100)
    p.add_argument("--commit-every", type=int, default=20)
    args = p.parse_args(argv)

    if args.apply and args.dry_run:
        p.error("use either --apply or --dry-run")
    if args.analyze:
        args.dry_run = True
    if args.apply_only:
        args.apply = True
    dry_run = not args.apply

    path = Path(args.issues_csv)
    if not path.exists():
        print("issues csv not found: %s" % path, flush=True)
        return 1

    jobs = dedupe_jobs(load_missing_from_csv(path, use_grep=not args.no_grep_csv))
    print("missing_loan jobs=%s (from %s)" % (len(jobs), path), flush=True)
    if args.work_limit > 0:
        jobs = jobs[: args.work_limit]

    plan_path = Path(args.plan_file) if args.plan_file.strip() else None
    if not plan_path and (args.apply_only or args.dry_run or args.apply or args.analyze):
        plan_path = Path("/tmp/repair_missing_loan_plan.json")

    relink_plan: List[dict] = []
    insert_plan: List[dict] = []
    skipped: Dict[str, int] = {}

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
        insert_from_source = args.with_source or (not args.analyze)
        suffix_scan = not args.no_suffix_scan
        loan_by_sn: Dict[str, dict] = {}
        src = None
        if insert_from_source:
            src = connect_source(cfg)
        tgt = connect_target(cfg)
        try:
            if insert_from_source and src is not None:
                loan_by_sn = fetch_loan_rows_for_jobs(src, jobs)
                print(
                    "source_repay_hit=%s/%s" % (len(loan_by_sn), len(jobs)),
                    flush=True,
                )
            elif args.analyze:
                print("analyze fast-path: skip source DB", flush=True)
            relink_plan, insert_plan, skipped = build_plan(
                jobs,
                loan_by_sn,
                tgt,
                query_chunk=max(50, min(args.query_chunk, 1000)),
                suffix_scan=suffix_scan,
                insert_from_source=insert_from_source,
            )
            if skipped:
                print(
                    "plan_skipped %s"
                    % " ".join("%s=%s" % (k, v) for k, v in sorted(skipped.items())),
                    flush=True,
                )
            if args.preview > 0:
                print_preview(jobs, loan_by_sn, relink_plan, insert_plan, args.preview)
                return 0
            relink_plan = enrich_relink_rows(relink_plan, jobs)
            if args.analyze:
                relink_csv = Path(args.relink_csv)
                insert_csv = Path(args.insert_csv)
                write_relink_csv(relink_csv, relink_plan)
                write_insert_csv(insert_csv, insert_plan)
                if plan_path:
                    payload = {"relink": relink_plan, "insert": insert_plan}
                    plan_path.write_text(
                        json.dumps(payload, ensure_ascii=False, indent=2),
                        encoding="utf-8",
                    )
                print_analyze_summary(
                    relink_plan, insert_plan, skipped, relink_csv, insert_csv
                )
                return 0
        finally:
            if src is not None:
                src.close()
            tgt.close()

        if plan_path and not args.analyze:
            payload = {"relink": enrich_relink_rows(relink_plan, jobs), "insert": insert_plan}
            relink_plan = payload["relink"]
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
