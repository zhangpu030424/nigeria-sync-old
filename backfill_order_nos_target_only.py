#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Target-only backfill for application_no / loan_no (no source DB).

New format (suffix = old numeric application_no):
  application_no = ng{app_id:04d}-{old_application_no}
  loan_no        = ng-{old_application_no}-01000

Strategies:
  insert-delete (default): SELECT row -> INSERT new PK -> DELETE old row (faster/safer for PK change)
  update:                  UPDATE loan_no / application_no in place

Usage:
  python3 backfill_order_nos_target_only.py --env ./ng_migration.env --dry-run --tables loan
  python3 backfill_order_nos_target_only.py --env ./ng_migration.env --apply --tables loan
  python3 backfill_order_nos_target_only.py --env ./ng_migration.env --apply --tables application
"""
import argparse
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, TextIO, Tuple

import pymysql
from pymysql.cursors import DictCursor

import ng_migration_run as mig

HERE = Path(__file__).resolve().parent
LOAN_COLS = mig.LOAN_INSERT_COLS
APP_COLS = mig.APPLICATION_INSERT_COLS


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
        autocommit=False,
    )


def new_application_no(app_id, old_no: str) -> str:
    return "ng%04d-%s" % (int(app_id), old_no)


def new_loan_no(old_app_no: str) -> str:
    return "ng-%s-01000" % old_app_no


def needs_backfill_app_no(app_no: str) -> bool:
    return bool(app_no) and not str(app_no).startswith("ng")


def cols_sql(cols: List[str]) -> str:
    return ", ".join("`%s`" % c for c in cols)


class DeleteAuditLog(object):
    """Log every old row removed (or would be removed) with application_no details."""

    HEADER = (
        "ts,action,table,old_application_no,new_application_no,"
        "old_loan_no,new_loan_no,app_id"
    )

    def __init__(self, path: Optional[str], enabled: bool = True):
        self.enabled = enabled
        self.path = path
        self._fp = None  # type: Optional[TextIO]
        if enabled and path:
            self._fp = open(path, "a", encoding="utf-8")
            if self._fp.tell() == 0:
                self._fp.write(self.HEADER + "\n")
                self._fp.flush()

    def close(self):
        if self._fp:
            self._fp.close()
            self._fp = None

    def record(
        self,
        action: str,
        table: str,
        old_application_no: str,
        new_application_no: str,
        app_id,
        old_loan_no: str = "",
        new_loan_no: str = "",
    ):
        if not self.enabled:
            return
        line = "%s,%s,%s,%s,%s,%s,%s,%s" % (
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            action,
            table,
            old_application_no,
            new_application_no,
            old_loan_no,
            new_loan_no,
            app_id,
        )
        print("DELETE_AUDIT %s" % line, flush=True)
        if self._fp:
            self._fp.write(line + "\n")
            self._fp.flush()


def exec_with_retry(tgt, fn, what: str):
    for attempt in range(5):
        try:
            return fn()
        except pymysql.Error as exc:
            try:
                tgt.rollback()
            except Exception:
                pass
            if attempt >= 4:
                raise
            print("%s retry err=%s" % (what, exc), flush=True)
            tgt.ping(reconnect=True)
            time.sleep(2)
    return None


def scan_loan_rows(tgt, after_loan_no: str, scan_limit: int) -> List[dict]:
    """loan + application 都是旧 application_no（按旧号 JOIN）。"""
    sql = """
        SELECT l.loan_no, l.application_no, a.app_id
        FROM loan l
        INNER JOIN application a ON l.application_no = a.application_no
        WHERE l.loan_no > %s
          AND l.application_no NOT LIKE 'ng%%'
          AND a.application_no NOT LIKE 'ng%%'
          AND a.app_id IS NOT NULL
        ORDER BY l.loan_no ASC
        LIMIT %s
    """
    with tgt.cursor() as cur:
        cur.execute(sql, (after_loan_no or "", scan_limit))
        return list(cur.fetchall())


def scan_loan_rows_app_already_new(tgt, after_loan_no: str, scan_limit: int) -> List[dict]:
    """loan 仍旧号，application 已是 ng{app_id:04d}-{旧号}。"""
    sql = """
        SELECT l.loan_no, l.application_no, a.app_id,
               a.application_no AS new_application_no
        FROM loan l
        INNER JOIN application a ON a.application_no = CONCAT('ng', LPAD(a.app_id, 4, '0'), '-', l.application_no)
        WHERE l.loan_no > %s
          AND l.application_no NOT LIKE 'ng%%'
          AND a.app_id IS NOT NULL
        ORDER BY l.loan_no ASC
        LIMIT %s
    """
    with tgt.cursor() as cur:
        cur.execute(sql, (after_loan_no or "", scan_limit))
        return list(cur.fetchall())


def scan_application_rows(tgt, after_app_no: str, scan_limit: int) -> List[dict]:
    sql = """
        SELECT application_no, app_id
        FROM application
        WHERE application_no > %s AND app_id IS NOT NULL
        ORDER BY application_no ASC
        LIMIT %s
    """
    with tgt.cursor() as cur:
        cur.execute(sql, (after_app_no or "", scan_limit))
        return list(cur.fetchall())


def loan_exists(tgt, loan_no: str) -> bool:
    with tgt.cursor() as cur:
        cur.execute("SELECT 1 FROM loan WHERE loan_no=%s LIMIT 1", (loan_no,))
        return cur.fetchone() is not None


def application_exists(tgt, app_no: str) -> bool:
    with tgt.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM application WHERE application_no=%s LIMIT 1", (app_no,)
        )
        return cur.fetchone() is not None


def fetch_loan_row(tgt, loan_no: str) -> Optional[dict]:
    sql = "SELECT %s FROM loan WHERE loan_no=%%s" % cols_sql(LOAN_COLS)
    with tgt.cursor() as cur:
        cur.execute(sql, (loan_no,))
        return cur.fetchone()


def fetch_application_row(tgt, app_no: str) -> Optional[dict]:
    sql = "SELECT %s FROM application WHERE application_no=%%s" % cols_sql(APP_COLS)
    with tgt.cursor() as cur:
        cur.execute(sql, (app_no,))
        return cur.fetchone()


def insert_row(tgt, table: str, cols: List[str], row: dict) -> None:
    placeholders = ", ".join(["%s"] * len(cols))
    sql = "INSERT INTO %s (%s) VALUES (%s)" % (table, cols_sql(cols), placeholders)
    with tgt.cursor() as cur:
        cur.execute(sql, [row[c] for c in cols])


def update_one_loan(
    tgt, row: dict, dry_run: bool, strategy: str, audit: Optional[DeleteAuditLog]
) -> str:
    old_loan_no = str(row["loan_no"])
    old_app_no = str(row["application_no"])
    if row.get("new_application_no"):
        new_app = str(row["new_application_no"])
    else:
        new_app = new_application_no(row["app_id"], old_app_no)
    new_loan = new_loan_no(old_app_no)
    if new_app == old_app_no and new_loan == old_loan_no:
        return "skip"
    if loan_exists(tgt, new_loan):
        if not loan_exists(tgt, old_loan_no):
            return "skip"
        return "conflict_loan_no"
    audit_action = "would_delete" if dry_run else "delete"
    if dry_run:
        if audit:
            audit.record(
                audit_action,
                "loan",
                old_app_no,
                new_app,
                row["app_id"],
                old_loan_no,
                new_loan,
            )
        return "ok"
    if strategy == "insert-delete":
        full = fetch_loan_row(tgt, old_loan_no)
        if not full:
            return "missing"
        full["loan_no"] = new_loan
        full["application_no"] = new_app
        insert_row(tgt, "loan", LOAN_COLS, full)
        with tgt.cursor() as cur:
            cur.execute("DELETE FROM loan WHERE loan_no=%s", (old_loan_no,))
            if not cur.rowcount:
                raise RuntimeError("delete loan failed loan_no=%s" % old_loan_no)
        if audit:
            audit.record(
                "delete",
                "loan",
                old_app_no,
                new_app,
                row["app_id"],
                old_loan_no,
                new_loan,
            )
    else:
        with tgt.cursor() as cur:
            cur.execute(
                "UPDATE loan SET loan_no=%s, application_no=%s WHERE loan_no=%s",
                (new_loan, new_app, old_loan_no),
            )
            if not cur.rowcount:
                return "missing"
        if audit:
            audit.record(
                "update",
                "loan",
                old_app_no,
                new_app,
                row["app_id"],
                old_loan_no,
                new_loan,
            )
    tgt.commit()
    return "ok"


def update_one_application(
    tgt, row: dict, dry_run: bool, strategy: str, audit: Optional[DeleteAuditLog]
) -> str:
    old_no = str(row["application_no"])
    new_no = new_application_no(row["app_id"], old_no)
    if new_no == old_no:
        return "skip"
    if application_exists(tgt, new_no):
        if not application_exists(tgt, old_no):
            return "skip"
        return "conflict_app_no"
    if dry_run:
        if audit:
            audit.record(
                "would_delete" if dry_run else "delete",
                "application",
                old_no,
                new_no,
                row["app_id"],
            )
        return "ok"
    if strategy == "insert-delete":
        full = fetch_application_row(tgt, old_no)
        if not full:
            return "missing"
        full["application_no"] = new_no
        insert_row(tgt, "application", APP_COLS, full)
        with tgt.cursor() as cur:
            cur.execute("DELETE FROM application WHERE application_no=%s", (old_no,))
            if not cur.rowcount:
                raise RuntimeError("delete application failed app_no=%s" % old_no)
        if audit:
            audit.record(
                "delete",
                "application",
                old_no,
                new_no,
                row["app_id"],
            )
    else:
        with tgt.cursor() as cur:
            cur.execute(
                "UPDATE application SET application_no=%s WHERE application_no=%s",
                (new_no, old_no),
            )
            if not cur.rowcount:
                return "missing"
        if audit:
            audit.record(
                "update",
                "application",
                old_no,
                new_no,
                row["app_id"],
            )
    tgt.commit()
    return "ok"


def _run_loan_pass(
    tgt,
    scan_fn,
    phase: str,
    dry_run: bool,
    strategy: str,
    scan_size: int,
    work_limit: int,
    log_every: int,
    audit: Optional[DeleteAuditLog],
    ok_so_far: int,
) -> Tuple[int, int, int]:
    ok = skip = scanned = 0
    after = ""
    batch_no = 0
    while True:
        batch_no += 1
        rows = exec_with_retry(
            tgt,
            lambda: scan_fn(tgt, after, scan_size),
            "%s scan after=%s" % (phase, after),
        )
        if not rows:
            break
        after = str(rows[-1]["loan_no"])
        scanned += len(rows)
        todo = [r for r in rows if needs_backfill_app_no(r["application_no"])]
        for row in todo:
            loan_no = str(row["loan_no"])
            status = exec_with_retry(
                tgt,
                lambda r=row: update_one_loan(tgt, r, dry_run, strategy, audit),
                "%s %s loan_no=%s" % (phase, strategy, loan_no),
            )
            if status == "ok":
                ok += 1
            else:
                skip += 1
            if work_limit and (ok_so_far + ok) >= work_limit:
                print("%s stop work_limit=%s" % (phase, work_limit), flush=True)
                return ok, skip, ok_so_far + ok
            if (ok + skip) and (ok + skip) % log_every == 0:
                print(
                    "%s progress ok=%s skip=%s scanned=%s last_loan_no=%s"
                    % (phase, ok, skip, scanned, loan_no),
                    flush=True,
                )
        print(
            "%s scan_batch=%s scanned=%s todo=%s ok=%s skip=%s after=%s"
            % (phase, batch_no, len(rows), len(todo), ok, skip, after),
            flush=True,
        )
        if len(rows) < scan_size:
            break
    return ok, skip, ok_so_far + ok


def run_loans(
    tgt,
    dry_run: bool,
    strategy: str,
    scan_size: int,
    work_limit: int,
    log_every: int,
    audit: Optional[DeleteAuditLog],
) -> Tuple[int, int]:
    total_ok = total_skip = 0
    ok_limit = 0

    print("loan phase1: loan+application 均为旧 application_no", flush=True)
    ok, skip, ok_limit = _run_loan_pass(
        tgt,
        scan_loan_rows,
        "loan_phase1",
        dry_run,
        strategy,
        scan_size,
        work_limit,
        log_every,
        audit,
        ok_limit,
    )
    total_ok += ok
    total_skip += skip
    if work_limit and ok_limit >= work_limit:
        return total_ok, total_skip

    print("loan phase2: application 已是新号，loan 仍旧号", flush=True)
    ok, skip, ok_limit = _run_loan_pass(
        tgt,
        scan_loan_rows_app_already_new,
        "loan_phase2",
        dry_run,
        strategy,
        scan_size,
        work_limit,
        log_every,
        audit,
        ok_limit,
    )
    total_ok += ok
    total_skip += skip
    return total_ok, total_skip


def count_scan_pass(tgt, scan_fn, phase: str, scan_size: int) -> int:
    """Paginated count (no writes); avoids full-table COUNT + CONCAT join."""
    total = 0
    after = ""
    batch_no = 0
    while True:
        batch_no += 1
        rows = exec_with_retry(
            tgt,
            lambda: scan_fn(tgt, after, scan_size),
            "%s count after=%s" % (phase, after),
        )
        if not rows:
            break
        after = str(rows[-1]["loan_no"])
        todo = [r for r in rows if needs_backfill_app_no(r["application_no"])]
        total += len(todo)
        print(
            "%s count_batch=%s batch=%s total=%s after=%s"
            % (phase, batch_no, len(todo), total, after),
            flush=True,
        )
        if len(rows) < scan_size:
            break
    return total


def count_application_pass(tgt, scan_size: int) -> int:
    total = 0
    after = ""
    batch_no = 0
    while True:
        batch_no += 1
        rows = exec_with_retry(
            tgt,
            lambda: scan_application_rows(tgt, after, scan_size),
            "application count after=%s" % after,
        )
        if not rows:
            break
        after = str(rows[-1]["application_no"])
        todo = [r for r in rows if needs_backfill_app_no(r["application_no"])]
        total += len(todo)
        print(
            "application count_batch=%s batch=%s total=%s after=%s"
            % (batch_no, len(todo), total, after),
            flush=True,
        )
        if len(rows) < scan_size:
            break
    return total


def run_loan_stats(tgt, scan_size: int, orphan_loan_hint: int) -> int:
    print("=== loan stats (paginated, no full-table COUNT) ===", flush=True)
    p1 = count_scan_pass(tgt, scan_loan_rows, "loan_phase1", scan_size)
    p2 = count_scan_pass(tgt, scan_loan_rows_app_already_new, "loan_phase2", scan_size)
    est_true_orphan = max(0, orphan_loan_hint - p2) if orphan_loan_hint else None
    print(
        "loan_phase1(双旧)=%s loan_phase2(app已新)=%s orphan_loan_hint=%s est_true_orphan=%s"
        % (p1, p2, orphan_loan_hint or "?", est_true_orphan if est_true_orphan is not None else "?"),
        flush=True,
    )
    return p1 + p2


def run_applications(
    tgt,
    dry_run: bool,
    strategy: str,
    scan_size: int,
    work_limit: int,
    log_every: int,
    audit: Optional[DeleteAuditLog],
) -> Tuple[int, int]:
    ok = skip = scanned = 0
    after = ""
    batch_no = 0
    while True:
        batch_no += 1
        rows = exec_with_retry(
            tgt,
            lambda: scan_application_rows(tgt, after, scan_size),
            "application scan after=%s" % after,
        )
        if not rows:
            break
        after = str(rows[-1]["application_no"])
        scanned += len(rows)
        todo = [r for r in rows if needs_backfill_app_no(r["application_no"])]
        for row in todo:
            app_no = str(row["application_no"])
            status = exec_with_retry(
                tgt,
                lambda r=row: update_one_application(tgt, r, dry_run, strategy, audit),
                "application %s app_no=%s" % (strategy, app_no),
            )
            if status == "ok":
                ok += 1
            else:
                skip += 1
            if work_limit and ok >= work_limit:
                print("application stop work_limit=%s" % work_limit, flush=True)
                return ok, skip
            if (ok + skip) and (ok + skip) % log_every == 0:
                print(
                    "application progress ok=%s skip=%s scanned=%s last_app_no=%s"
                    % (ok, skip, scanned, app_no),
                    flush=True,
                )
        print(
            "application scan_batch=%s scanned=%s todo=%s ok=%s skip=%s after=%s"
            % (batch_no, len(rows), len(todo), ok, skip, after),
            flush=True,
        )
        if len(rows) < scan_size:
            break
    return ok, skip


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(description="Target-only order no backfill (row by row)")
    p.add_argument("--env", default=str(HERE / "ng_migration.env"))
    p.add_argument("--apply", action="store_true", help="write changes (default: dry-run)")
    p.add_argument("--dry-run", action="store_true", help="preview only (default)")
    p.add_argument(
        "--strategy",
        choices=["insert-delete", "update"],
        default="insert-delete",
        help="insert-delete: SELECT+INSERT+DELETE (default); update: in-place UPDATE",
    )
    p.add_argument(
        "--tables",
        choices=["loan", "application", "all"],
        default="all",
        help="loan must run before application when using all",
    )
    p.add_argument("--scan-size", type=int, default=500)
    p.add_argument("--work-limit", type=int, default=0)
    p.add_argument("--log-every", type=int, default=100)
    p.add_argument(
        "--delete-log",
        default="",
        help="CSV audit log for deleted/replaced rows (default: /tmp/backfill_delete_audit_YYYYMMDD_HHMMSS.csv)",
    )
    p.add_argument(
        "--no-delete-log",
        action="store_true",
        help="do not print/write delete audit lines",
    )
    p.add_argument(
        "--count-only",
        action="store_true",
        help="paginated stats only (no writes); use with --orphan-loan-hint 13753",
    )
    p.add_argument(
        "--orphan-loan-hint",
        type=int,
        default=0,
        help="optional orphan_loan COUNT from SQL for est_true_orphan=hint-phase2",
    )
    args = p.parse_args(argv)
    if args.apply and args.dry_run:
        p.error("use either --apply or --dry-run, not both")
    dry_run = not args.apply

    delete_log_path = args.delete_log
    if args.count_only:
        audit = DeleteAuditLog(None, enabled=False)
    else:
        if not args.no_delete_log and not delete_log_path:
            delete_log_path = "/tmp/backfill_delete_audit_%s.csv" % datetime.now().strftime(
                "%Y%m%d_%H%M%S"
            )
        audit = DeleteAuditLog(delete_log_path or None, enabled=not args.no_delete_log)

    cfg = load_env(Path(args.env))
    tgt = connect_target(cfg)
    try:
        if args.count_only:
            print("count_only scan_size=%s" % args.scan_size, flush=True)
            if args.tables in ("loan", "all"):
                run_loan_stats(tgt, args.scan_size, args.orphan_loan_hint)
            if args.tables in ("application", "all"):
                app_n = count_application_pass(tgt, args.scan_size)
                print("application_old_format=%s" % app_n, flush=True)
            print("count_only finished", flush=True)
            return 0

        print(
            "start dry_run=%s strategy=%s tables=%s scan_size=%s work_limit=%s delete_log=%s"
            % (
                dry_run,
                args.strategy,
                args.tables,
                args.scan_size,
                args.work_limit,
                delete_log_path if not args.no_delete_log else "(disabled)",
            ),
            flush=True,
        )
        if args.tables in ("loan", "all"):
            ok, skip = run_loans(
                tgt,
                dry_run,
                args.strategy,
                args.scan_size,
                args.work_limit,
                args.log_every,
                audit,
            )
            print("loan done ok=%s skip=%s" % (ok, skip), flush=True)
        if args.tables in ("application", "all"):
            ok, skip = run_applications(
                tgt,
                dry_run,
                args.strategy,
                args.scan_size,
                args.work_limit,
                args.log_every,
                audit,
            )
            print("application done ok=%s skip=%s" % (ok, skip), flush=True)
        print("finished delete_log=%s" % (delete_log_path or ""), flush=True)
        return 0
    finally:
        audit.close()
        tgt.close()


if __name__ == "__main__":
    raise SystemExit(main())
