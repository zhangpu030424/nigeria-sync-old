#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Target-only backfill for application_no / loan_no (no source DB).

Loan 逻辑（单遍）:
  1. SELECT loan WHERE application_no REGEXP '^[0-9]+$'  （纯数字 = 待改）
  2. 先 =旧号，再 LIKE 'ng____-旧号' 查 application，有则改，无则 skip

New format:
  application_no = ng{app_id:04d}-{old_application_no}   (market 旧单号)
  loan_no        = ng-{core_sn}-01000                    (与 ng_migration_run.format_loan_no 一致)

  core_sn 来源（优先级）:
    1. application.sn
    2. 旧 loan_no 形如 NG-217809955941 / ng-217809955941

  --repair-wrong-loan: 修复曾用 market 号拼 loan_no 的行（ng-{18位}-01000 → ng-{core_sn}-01000）

  --from-audit-log: 读取 backfill_delete_audit_*.csv / backfill_loan.log 中的删除记录，
    按 old_loan_no(NG-xxx) 还原为 ng-{core_sn}-01000；同样输出 DELETE_AUDIT 到新日志。

Performance:
  --scan-size 150  --commit-every 20  --workers 4  (按 loan_no 分段并行)
"""
import argparse
import multiprocessing
import re
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
LEGACY_LOAN_NO_RE = re.compile(r"^[Nn][Gg]-(\d+)$")
NEW_LOAN_NO_RE = re.compile(r"^[Nn][Gg]-(\d+)-(\d{5})$")
DELETE_AUDIT_LINE_RE = re.compile(r"^DELETE_AUDIT\s+(.+)$")
AUDIT_HEADER = "ts,action,table,old_application_no,new_application_no,old_loan_no,new_loan_no,app_id"


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


def extract_core_sn_from_legacy_loan_no(loan_no: str) -> str:
    """旧目标库 loan_no 多为 NG-{core_sn}，不含 -01000 后缀。"""
    m = LEGACY_LOAN_NO_RE.match(str(loan_no or "").strip())
    return m.group(1) if m else ""


def parse_loan_no_middle_sn(loan_no: str) -> str:
    m = NEW_LOAN_NO_RE.match(str(loan_no or "").strip())
    return m.group(1) if m else ""


def resolve_core_sn(row: dict, old_loan_no: str) -> str:
    cs = str(row.get("core_sn") or "").strip()
    if cs:
        return cs
    return extract_core_sn_from_legacy_loan_no(old_loan_no)


def new_loan_no_for_row(row: dict, old_loan_no: str) -> str:
    core_sn = resolve_core_sn(row, old_loan_no)
    if not core_sn:
        return ""
    return mig.format_loan_no(core_sn, 1, 0)


def is_numeric_application_no(app_no: str) -> bool:
    s = str(app_no or "")
    return bool(s) and s.isdigit()


def needs_backfill_app_no(app_no: str) -> bool:
    return is_numeric_application_no(app_no)


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


def parse_audit_record_line(line: str) -> Optional[dict]:
    """Parse one CSV audit row or DELETE_AUDIT log line."""
    raw = str(line or "").strip()
    if not raw or raw == AUDIT_HEADER or raw.endswith(AUDIT_HEADER):
        return None
    m = DELETE_AUDIT_LINE_RE.match(raw)
    if m:
        raw = m.group(1).strip()
    if raw.startswith("ts,action,"):
        return None
    parts = raw.split(",")
    if len(parts) < 8:
        return None
    return {
        "ts": parts[0].strip(),
        "action": parts[1].strip(),
        "table": parts[2].strip(),
        "old_application_no": parts[3].strip(),
        "new_application_no": parts[4].strip(),
        "old_loan_no": parts[5].strip(),
        "new_loan_no": parts[6].strip(),
        "app_id": parts[7].strip(),
    }


def _is_worker_audit_path(path: Path) -> bool:
    return bool(re.search(r"\.w\d+$", path.stem))


def expand_audit_log_paths(paths: List[str], merge_worker_logs: bool = True) -> List[str]:
    """Expand base audit csv to sibling .w0.csv ... if present."""
    out = []  # type: List[str]
    seen = set()
    for raw in paths:
        base = Path(raw).expanduser()
        candidates = [base]
        if merge_worker_logs and base.suffix.lower() == ".csv" and not _is_worker_audit_path(base):
            stem, suffix = base.stem, base.suffix
            parent = base.parent
            for i in range(32):
                wp = parent / ("%s.w%s%s" % (stem, i, suffix))
                if wp.exists():
                    candidates.append(wp)
        for cand in candidates:
            if not cand.exists():
                continue
            key = str(cand.resolve())
            if key in seen:
                continue
            seen.add(key)
            out.append(key)
    return out


def load_audit_records(paths: List[str]) -> List[dict]:
    records = []  # type: List[dict]
    for path in paths:
        p = Path(path)
        text = p.read_text(encoding="utf-8", errors="replace")
        for line in text.splitlines():
            rec = parse_audit_record_line(line)
            if rec:
                rec["_source"] = str(p)
                records.append(rec)
    return records


def build_repair_rows_from_audit(records: List[dict]) -> List[dict]:
    """Turn delete audit rows into loan repair jobs (wrong new_loan_no -> core sn loan)."""
    out = []  # type: List[dict]
    seen_wrong = set()
    skipped = {}  # type: Dict[str, int]

    for rec in records:
        if rec.get("table") != "loan":
            skipped["not_loan"] = skipped.get("not_loan", 0) + 1
            continue
        if rec.get("action") not in ("delete", "would_delete"):
            skipped["not_delete"] = skipped.get("not_delete", 0) + 1
            continue
        wrong_loan = str(rec.get("new_loan_no") or "").strip()
        legacy_loan = str(rec.get("old_loan_no") or "").strip()
        if not wrong_loan or not legacy_loan:
            skipped["missing_loan_no"] = skipped.get("missing_loan_no", 0) + 1
            continue
        core_sn = extract_core_sn_from_legacy_loan_no(legacy_loan)
        if not core_sn:
            skipped["no_core_sn"] = skipped.get("no_core_sn", 0) + 1
            continue
        correct_loan = mig.format_loan_no(core_sn, 1, 0)
        if not correct_loan or correct_loan == wrong_loan:
            skipped["already_correct"] = skipped.get("already_correct", 0) + 1
            continue
        if wrong_loan in seen_wrong:
            skipped["dup_wrong_loan"] = skipped.get("dup_wrong_loan", 0) + 1
            continue
        seen_wrong.add(wrong_loan)
        out.append(
            {
                "loan_no": wrong_loan,
                "application_no": rec.get("new_application_no") or rec.get("old_application_no"),
                "new_application_no": rec.get("new_application_no") or "",
                "app_id": rec.get("app_id"),
                "core_sn": core_sn,
                "new_loan_no": correct_loan,
                "audit_old_loan_no": legacy_loan,
                "audit_source": rec.get("_source", ""),
            }
        )
    out.sort(key=lambda r: str(r["loan_no"]))
    if skipped:
        print(
            "audit_repair_skipped %s"
            % " ".join("%s=%s" % (k, v) for k, v in sorted(skipped.items())),
            flush=True,
        )
    return out


def summarize_audit_repair_plan(rows: List[dict], paths: List[str]) -> None:
    print("=== audit repair plan ===", flush=True)
    print("audit_files=%s" % len(paths), flush=True)
    for p in paths:
        print("  %s" % p, flush=True)
    print("repair_rows=%s" % len(rows), flush=True)
    for row in rows[:10]:
        print(
            "  sample wrong=%s -> correct=%s legacy=%s app=%s"
            % (
                row["loan_no"],
                row["new_loan_no"],
                row.get("audit_old_loan_no", ""),
                row.get("new_application_no", ""),
            ),
            flush=True,
        )
    if len(rows) > 10:
        print("  ... and %s more" % (len(rows) - 10), flush=True)


def run_loans_from_audit(
    tgt,
    repair_rows: List[dict],
    dry_run: bool,
    strategy: str,
    work_limit: int,
    log_every: int,
    audit: Optional[DeleteAuditLog],
    commit_every: int,
) -> Tuple[int, int]:
    ok = skip = 0
    tracker = CommitTracker(tgt, commit_every, dry_run)
    status_counts = {}  # type: Dict[str, int]
    for row in repair_rows:
        loan_no = str(row["loan_no"])
        status = exec_with_retry(
            tgt,
            lambda r=row: update_one_loan(tgt, r, dry_run, strategy, audit, tracker),
            "loan audit-repair loan_no=%s" % loan_no,
        )
        status_counts[status] = status_counts.get(status, 0) + 1
        if status == "ok":
            ok += 1
        else:
            skip += 1
        if work_limit and ok >= work_limit:
            tracker.flush()
            print("loan audit-repair stop work_limit=%s" % work_limit, flush=True)
            break
        if (ok + skip) and (ok + skip) % log_every == 0:
            print(
                "loan audit-repair progress ok=%s skip=%s last=%s"
                % (ok, skip, loan_no),
                flush=True,
            )
    tracker.flush()
    reasons = " ".join("%s=%s" % (k, v) for k, v in sorted(status_counts.items()))
    print("loan audit-repair done ok=%s skip=%s reasons=[%s]" % (ok, skip, reasons or "-"), flush=True)
    return ok, skip


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


class CommitTracker(object):
    """Batch commit to reduce round-trips through proxy."""

    def __init__(self, conn, every: int, dry_run: bool):
        self.conn = conn
        self.every = max(0, int(every))
        self.dry_run = dry_run
        self.pending = 0

    def note_write(self):
        if self.dry_run or self.every <= 0:
            return
        self.pending += 1
        if self.pending >= self.every:
            self.flush()

    def flush(self):
        if self.dry_run or self.pending <= 0:
            return
        self.conn.commit()
        self.pending = 0


def scan_loan_numeric_batch(
    tgt,
    after_loan_no: str,
    scan_limit: int,
    loan_no_max: Optional[str] = None,
) -> List[dict]:
    """loan.application_no 纯数字 = 待改（等价 SELECT ... REGEXP '^[0-9]+$'）。"""
    sql = """
        SELECT loan_no, application_no
        FROM loan
        WHERE loan_no > %s
          AND application_no REGEXP '^[0-9]+$'
    """
    params = [after_loan_no or ""]
    if loan_no_max:
        sql += " AND loan_no <= %s"
        params.append(loan_no_max)
    sql += " ORDER BY loan_no ASC LIMIT %s"
    params.append(scan_limit)
    with tgt.cursor() as cur:
        cur.execute(sql, params)
        return list(cur.fetchall())


def scan_repair_wrong_loan_batch(
    tgt,
    after_loan_no: str,
    scan_limit: int,
    wrong_sn_min_len: int,
    loan_no_max: Optional[str] = None,
) -> List[dict]:
    """曾错误 backfill：loan_no 中间段用了 market 长号，需按 application.sn 改回。"""
    sql = """
        SELECT l.loan_no, l.application_no, a.app_id, a.sn AS core_sn
        FROM loan l
        INNER JOIN application a ON a.application_no = l.application_no
        WHERE l.loan_no > %s
          AND l.loan_no REGEXP '^[Nn][Gg]-[0-9]+-[0-9]{5}$'
          AND a.sn IS NOT NULL AND a.sn <> ''
    """
    params = [after_loan_no or ""]
    if loan_no_max:
        sql += " AND l.loan_no <= %s"
        params.append(loan_no_max)
    sql += " ORDER BY l.loan_no ASC LIMIT %s"
    params.append(scan_limit * 3)
    with tgt.cursor() as cur:
        cur.execute(sql, params)
        rows = list(cur.fetchall())
    out = []
    for row in rows:
        old_loan = str(row["loan_no"])
        middle = parse_loan_no_middle_sn(old_loan)
        if wrong_sn_min_len > 0 and len(middle) < wrong_sn_min_len:
            continue
        core_sn = str(row.get("core_sn") or "").strip()
        if not core_sn:
            continue
        want = mig.format_loan_no(core_sn, 1, 0)
        if not want or want == old_loan:
            continue
        out.append(
            {
                "loan_no": old_loan,
                "application_no": row["application_no"],
                "app_id": row["app_id"],
                "new_application_no": row["application_no"],
                "core_sn": core_sn,
                "new_loan_no": want,
            }
        )
        if len(out) >= scan_limit:
            break
    return out


def enrich_repair_loan_candidates(rows: List[dict]) -> List[dict]:
    return [r for r in rows if r.get("new_loan_no")]


def lookup_application_for_old_loan(tgt, old_app_no: str) -> Optional[dict]:
    """先精确查旧号，再 LIKE 'ng____-旧号'（app_id 用 _ 占位，从结果行读取）。"""
    with tgt.cursor() as cur:
        cur.execute(
            """
            SELECT application_no, app_id, sn
            FROM application
            WHERE application_no = %s
            LIMIT 1
            """,
            (old_app_no,),
        )
        row = cur.fetchone()
    if row and row["app_id"] is not None:
        return {
            "new_application_no": new_application_no(row["app_id"], old_app_no),
            "app_id": row["app_id"],
            "core_sn": str(row.get("sn") or "").strip(),
        }

    pattern = "ng____-%s" % old_app_no
    with tgt.cursor() as cur:
        cur.execute(
            """
            SELECT application_no, app_id, sn
            FROM application
            WHERE application_no LIKE %s
            LIMIT 5
            """,
            (pattern,),
        )
        rows = list(cur.fetchall())
    for row in rows:
        app_no = str(row["application_no"])
        if row["app_id"] is not None and new_application_no(row["app_id"], old_app_no) == app_no:
            return {
                "new_application_no": app_no,
                "app_id": row["app_id"],
                "core_sn": str(row.get("sn") or "").strip(),
            }
    return None


def enrich_loan_candidates(tgt, loans: List[dict]) -> List[dict]:
    out = []
    for row in loans:
        old_app = str(row["application_no"])
        match = lookup_application_for_old_loan(tgt, old_app)
        if not match:
            continue
        out.append(
            {
                "loan_no": row["loan_no"],
                "application_no": row["application_no"],
                "app_id": match["app_id"],
                "new_application_no": match["new_application_no"],
                "core_sn": match.get("core_sn")
                or extract_core_sn_from_legacy_loan_no(row["loan_no"]),
            }
        )
    return out


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


def fetch_loan_application_no(tgt, loan_no: str) -> Optional[str]:
    with tgt.cursor() as cur:
        cur.execute(
            "SELECT application_no FROM loan WHERE loan_no=%s LIMIT 1", (loan_no,)
        )
        row = cur.fetchone()
        return str(row["application_no"]) if row else None


def update_one_loan(
    tgt,
    row: dict,
    dry_run: bool,
    strategy: str,
    audit: Optional[DeleteAuditLog],
    commit_tracker: Optional[CommitTracker] = None,
) -> str:
    old_loan_no = str(row["loan_no"])
    old_app_no = str(row["application_no"])
    if row.get("new_application_no"):
        new_app = str(row["new_application_no"])
    else:
        new_app = new_application_no(row["app_id"], old_app_no)
    if row.get("new_loan_no"):
        new_loan = str(row["new_loan_no"])
    else:
        new_loan = new_loan_no_for_row(row, old_loan_no)
    if not new_loan:
        return "skip_no_core_sn"
    if new_app == old_app_no and new_loan == old_loan_no:
        return "skip"

    # loan_no 已是新格式，仅 application_no 仍为旧号（phase2 常见）
    if new_loan == old_loan_no and new_app != old_app_no:
        if dry_run:
            if audit:
                audit.record(
                    "would_update",
                    "loan",
                    old_app_no,
                    new_app,
                    row["app_id"],
                    old_loan_no,
                    new_loan,
                )
            return "ok"
        with tgt.cursor() as cur:
            cur.execute(
                "UPDATE loan SET application_no=%s WHERE loan_no=%s",
                (new_app, old_loan_no),
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
        if commit_tracker:
            commit_tracker.note_write()
        else:
            tgt.commit()
        return "ok"

    if loan_exists(tgt, new_loan):
        if not loan_exists(tgt, old_loan_no):
            return "skip_done"
        if new_loan != old_loan_no:
            existing_app = fetch_loan_application_no(tgt, new_loan)
            if existing_app == new_app:
                if dry_run:
                    if audit:
                        audit.record(
                            "would_delete",
                            "loan",
                            old_app_no,
                            new_app,
                            row["app_id"],
                            old_loan_no,
                            new_loan,
                        )
                    return "ok"
                with tgt.cursor() as cur:
                    cur.execute("DELETE FROM loan WHERE loan_no=%s", (old_loan_no,))
                    if not cur.rowcount:
                        return "missing"
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
                if commit_tracker:
                    commit_tracker.note_write()
                else:
                    tgt.commit()
                return "ok"
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
    if commit_tracker:
        commit_tracker.note_write()
    else:
        tgt.commit()
    return "ok"


def update_one_application(
    tgt,
    row: dict,
    dry_run: bool,
    strategy: str,
    audit: Optional[DeleteAuditLog],
    commit_tracker: Optional[CommitTracker] = None,
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
    if commit_tracker:
        commit_tracker.note_write()
    else:
        tgt.commit()
    return "ok"


def _log(prefix: str, msg: str):
    if prefix:
        print("[%s] %s" % (prefix, msg), flush=True)
    else:
        print(msg, flush=True)


def list_numeric_loan_nos(tgt, scan_size: int) -> List[str]:
    nos = []
    after = ""
    while True:
        rows = exec_with_retry(
            tgt,
            lambda: scan_loan_numeric_batch(tgt, after, scan_size),
            "list_numeric_loan_nos after=%s" % after,
        )
        if not rows:
            break
        nos.extend(str(r["loan_no"]) for r in rows)
        after = str(rows[-1]["loan_no"])
        if len(rows) < scan_size:
            break
    return nos


def compute_worker_ranges(
    tgt, workers: int, scan_size: int
) -> List[Optional[Tuple[str, str]]]:
    """Return per-worker (loan_no_min_exclusive, loan_no_max_inclusive); None = no rows."""
    nos = list_numeric_loan_nos(tgt, scan_size)
    if not nos:
        return [None] * workers
    n = len(nos)
    ranges = []  # type: List[Optional[Tuple[str, str]]]
    for i in range(workers):
        start = i * n // workers
        end = (i + 1) * n // workers - 1
        if start >= n:
            ranges.append(None)
            continue
        lo = nos[start - 1] if start > 0 else ""
        hi = nos[end]
        ranges.append((lo, hi))
    return ranges


def _run_loan_pass(
    tgt,
    dry_run: bool,
    strategy: str,
    scan_size: int,
    work_limit: int,
    log_every: int,
    audit: Optional[DeleteAuditLog],
    commit_tracker: Optional[CommitTracker],
    loan_no_min: str = "",
    loan_no_max: Optional[str] = None,
    worker_label: str = "",
    repair_wrong_loan: bool = False,
    wrong_sn_min_len: int = 15,
) -> Tuple[int, int]:
    """纯数字 application_no 的 loan → 查 application → 有则改，无则 skip。"""
    ok = skip = scanned = 0
    after = loan_no_min
    batch_no = 0
    while True:
        batch_no += 1
        if repair_wrong_loan:
            rows = exec_with_retry(
                tgt,
                lambda: scan_repair_wrong_loan_batch(
                    tgt, after, scan_size, wrong_sn_min_len, loan_no_max
                ),
                "loan repair scan after=%s max=%s" % (after, loan_no_max or "*"),
            )
            todo = enrich_repair_loan_candidates(rows)
        else:
            rows = exec_with_retry(
                tgt,
                lambda: scan_loan_numeric_batch(tgt, after, scan_size, loan_no_max),
                "loan scan after=%s max=%s" % (after, loan_no_max or "*"),
            )
            todo = enrich_loan_candidates(tgt, rows)
        if not rows:
            break
        after = str(rows[-1]["loan_no"])
        scanned += len(rows)
        orphan_batch = len(rows) - len(todo)
        status_counts = {}  # type: Dict[str, int]
        for row in todo:
            loan_no = str(row["loan_no"])
            status = exec_with_retry(
                tgt,
                lambda r=row: update_one_loan(
                    tgt, r, dry_run, strategy, audit, commit_tracker
                ),
                "loan %s loan_no=%s" % (strategy, loan_no),
            )
            status_counts[status] = status_counts.get(status, 0) + 1
            if status == "ok":
                ok += 1
            else:
                skip += 1
            if work_limit and ok >= work_limit:
                if commit_tracker:
                    commit_tracker.flush()
                _log(worker_label, "loan stop work_limit=%s" % work_limit)
                return ok, skip
            if (ok + skip) and (ok + skip) % log_every == 0:
                _log(
                    worker_label,
                    "loan progress ok=%s skip=%s scanned=%s last_loan_no=%s"
                    % (ok, skip, scanned, loan_no),
                )
        reasons = " ".join(
            "%s=%s" % (k, v) for k, v in sorted(status_counts.items())
        )
        _log(
            worker_label,
            "loan scan_batch=%s numeric=%s matched=%s orphan=%s ok=%s skip=%s reasons=[%s] after=%s"
            % (
                batch_no,
                len(rows),
                len(todo),
                orphan_batch,
                ok,
                skip,
                reasons or "-",
                after,
            ),
        )
        if len(rows) < scan_size:
            break
    if commit_tracker:
        commit_tracker.flush()
    return ok, skip


def run_loans(
    tgt,
    dry_run: bool,
    strategy: str,
    scan_size: int,
    work_limit: int,
    log_every: int,
    audit: Optional[DeleteAuditLog],
    commit_every: int,
    loan_no_min: str = "",
    loan_no_max: Optional[str] = None,
    worker_label: str = "",
    repair_wrong_loan: bool = False,
    wrong_sn_min_len: int = 15,
) -> Tuple[int, int]:
    if repair_wrong_loan:
        _log(
            worker_label,
            "loan repair: ng-{长market号}-01000 → ng-{application.sn}-01000 (min_len=%s)"
            % wrong_sn_min_len,
        )
    else:
        _log(
            worker_label,
            "loan: REGEXP '^[0-9]+$' 扫待改行，=旧号 / ng____-旧号 查 application",
        )
    if loan_no_max or loan_no_min:
        _log(
            worker_label,
            "loan segment (%s, %s]" % (loan_no_min or "(start)", loan_no_max or "(end)"),
        )
    tracker = CommitTracker(tgt, commit_every, dry_run)
    return _run_loan_pass(
        tgt,
        dry_run,
        strategy,
        scan_size,
        work_limit,
        log_every,
        audit,
        tracker,
        loan_no_min,
        loan_no_max,
        worker_label,
        repair_wrong_loan,
        wrong_sn_min_len,
    )


def count_loan_pass(tgt, scan_size: int) -> Tuple[int, int]:
    """Returns (numeric_total, matched_with_application)."""
    total = matched = 0
    after = ""
    batch_no = 0
    while True:
        batch_no += 1
        rows = exec_with_retry(
            tgt,
            lambda: scan_loan_numeric_batch(tgt, after, scan_size),
            "loan count after=%s" % after,
        )
        if not rows:
            break
        after = str(rows[-1]["loan_no"])
        total += len(rows)
        batch_matched = enrich_loan_candidates(tgt, rows)
        matched += len(batch_matched)
        print(
            "loan count_batch=%s numeric=%s matched=%s orphan=%s total_numeric=%s total_matched=%s after=%s"
            % (
                batch_no,
                len(rows),
                len(batch_matched),
                len(rows) - len(batch_matched),
                total,
                matched,
                after,
            ),
            flush=True,
        )
        if len(rows) < scan_size:
            break
    return total, matched


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
    numeric, matched = count_loan_pass(tgt, scan_size)
    orphan = numeric - matched
    print(
        "loan_numeric=%s matched=%s orphan=%s orphan_loan_hint=%s"
        % (numeric, matched, orphan, orphan_loan_hint or "?"),
        flush=True,
    )
    return matched


def run_applications(
    tgt,
    dry_run: bool,
    strategy: str,
    scan_size: int,
    work_limit: int,
    log_every: int,
    audit: Optional[DeleteAuditLog],
    commit_every: int,
) -> Tuple[int, int]:
    ok = skip = scanned = 0
    after = ""
    batch_no = 0
    tracker = CommitTracker(tgt, commit_every, dry_run)
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
                lambda r=row: update_one_application(
                    tgt, r, dry_run, strategy, audit, tracker
                ),
                "application %s app_no=%s" % (strategy, app_no),
            )
            if status == "ok":
                ok += 1
            else:
                skip += 1
            if work_limit and ok >= work_limit:
                tracker.flush()
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
    tracker.flush()
    return ok, skip


def _worker_delete_log_path(base: str, worker_id: int) -> str:
    if not base:
        return ""
    p = Path(base)
    return str(p.with_name("%s.w%s%s" % (p.stem, worker_id, p.suffix or ".csv")))


def loan_worker_run(spec: dict) -> Tuple[int, int]:
    worker_id = spec["worker_id"]
    workers = spec["workers"]
    label = "w%s/%s" % (worker_id, workers)
    seg = spec.get("segment")
    if seg is None:
        _log(label, "empty segment, skip")
        return 0, 0
    lo, hi = seg

    cfg = load_env(Path(spec["env"]))
    tgt = connect_target(cfg)
    if spec.get("no_delete_log"):
        audit = DeleteAuditLog(None, enabled=False)
        delete_log = ""
    else:
        delete_log = spec.get("delete_log") or ""
        audit = DeleteAuditLog(delete_log or None, enabled=bool(delete_log))

    try:
        _log(label, "start segment (%s, %s]" % (lo or "(start)", hi))
        ok, skip = run_loans(
            tgt,
            spec["dry_run"],
            spec["strategy"],
            spec["scan_size"],
            spec["work_limit"],
            spec["log_every"],
            audit,
            spec["commit_every"],
            lo,
            hi,
            label,
            spec.get("repair_wrong_loan", False),
            spec.get("wrong_sn_min_len", 15),
        )
        _log(label, "done ok=%s skip=%s delete_log=%s" % (ok, skip, delete_log or ""))
        return ok, skip
    finally:
        audit.close()
        tgt.close()


def run_loans_parallel(args, cfg_path: str, delete_log_path: str) -> Tuple[int, int]:
    tgt = connect_target(load_env(Path(cfg_path)))
    try:
        print(
            "computing loan_no ranges workers=%s scan_size=%s ..."
            % (args.workers, args.scan_size),
            flush=True,
        )
        ranges = compute_worker_ranges(tgt, args.workers, args.scan_size)
        for i, seg in enumerate(ranges):
            if seg is None:
                print("  worker %s/%s: (empty)" % (i, args.workers), flush=True)
            else:
                lo, hi = seg
                print(
                    "  worker %s/%s: (%s, %s]"
                    % (i, args.workers, lo or "(start)", hi),
                    flush=True,
                )
    finally:
        tgt.close()

    specs = []
    for i, seg in enumerate(ranges):
        if seg is None:
            continue
        wlog = _worker_delete_log_path(delete_log_path, i) if delete_log_path else ""
        specs.append(
            {
                "worker_id": i,
                "workers": args.workers,
                "env": cfg_path,
                "dry_run": not args.apply,
                "strategy": args.strategy,
                "scan_size": args.scan_size,
                "work_limit": args.work_limit,
                "log_every": args.log_every,
                "commit_every": args.commit_every,
                "segment": seg,
                "delete_log": wlog,
                "no_delete_log": args.no_delete_log,
                "repair_wrong_loan": getattr(args, "repair_wrong_loan", False),
                "wrong_sn_min_len": getattr(args, "wrong_sn_min_len", 15),
            }
        )

    if not specs:
        print("no loan rows to process", flush=True)
        return 0, 0

    ctx = multiprocessing.get_context("spawn")
    with ctx.Pool(processes=len(specs)) as pool:
        results = pool.map(loan_worker_run, specs)

    total_ok = sum(r[0] for r in results)
    total_skip = sum(r[1] for r in results)
    print(
        "loan parallel done workers=%s ok=%s skip=%s" % (len(specs), total_ok, total_skip),
        flush=True,
    )
    return total_ok, total_skip


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
    p.add_argument("--scan-size", type=int, default=150, help="每批扫描 loan 数")
    p.add_argument(
        "--commit-every",
        type=int,
        default=20,
        help="每 N 条成功写入 commit 一次（默认 20）",
    )
    p.add_argument(
        "--workers",
        type=int,
        default=1,
        help="loan 并行 worker 数，按 loan_no 分段（仅 --tables loan）",
    )
    p.add_argument(
        "--worker-id",
        type=int,
        default=-1,
        help="手动指定 worker 编号 0..workers-1（配合 --loan-no-max 或由主进程分段）",
    )
    p.add_argument(
        "--loan-no-min",
        default="",
        help="手动 segment 下界（exclusive，默认空=从头）",
    )
    p.add_argument(
        "--loan-no-max",
        default="",
        help="手动 segment 上界（inclusive）；并行时由主进程自动计算",
    )
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
        help="optional orphan_loan COUNT from SQL for cross-check",
    )
    p.add_argument(
        "--repair-wrong-loan",
        action="store_true",
        help="修复曾错误 backfill 的 loan_no（ng-{market长号}-01000 → ng-{core_sn}-01000）",
    )
    p.add_argument(
        "--wrong-sn-min-len",
        type=int,
        default=15,
        help="--repair-wrong-loan 时，仅处理 loan_no 中间段长度 >= 该值的行（默认 15）",
    )
    p.add_argument(
        "--from-audit-log",
        action="append",
        default=[],
        metavar="PATH",
        help="从 backfill 审计 CSV / backfill_loan.log 修复；可多次指定或逗号分隔",
    )
    p.add_argument(
        "--no-merge-worker-logs",
        action="store_true",
        help="--from-audit-log 时不自动合并同目录 .w0.csv .w1.csv ...",
    )
    args = p.parse_args(argv)
    audit_log_inputs = []
    for item in args.from_audit_log:
        for part in str(item).split(","):
            part = part.strip()
            if part:
                audit_log_inputs.append(part)
    if audit_log_inputs and args.repair_wrong_loan:
        print("warning: --from-audit-log 优先，忽略 --repair-wrong-loan", flush=True)
        args.repair_wrong_loan = False
    if args.repair_wrong_loan and args.workers > 1:
        print("warning: --repair-wrong-loan 不支持多 worker，已降为 workers=1", flush=True)
        args.workers = 1
    if args.apply and args.dry_run:
        p.error("use either --apply or --dry-run, not both")
    dry_run = not args.apply

    delete_log_path = args.delete_log
    if audit_log_inputs and not delete_log_path and not args.no_delete_log:
        delete_log_path = "/tmp/backfill_repair_audit_loan_%s.csv" % datetime.now().strftime(
            "%Y%m%d_%H%M%S"
        )
    if args.count_only:
        audit = DeleteAuditLog(None, enabled=False)
    else:
        if not args.no_delete_log and not delete_log_path:
            delete_log_path = "/tmp/backfill_delete_audit_%s.csv" % datetime.now().strftime(
                "%Y%m%d_%H%M%S"
            )
        audit = DeleteAuditLog(delete_log_path or None, enabled=not args.no_delete_log)

    cfg = load_env(Path(args.env))
    audit_paths = expand_audit_log_paths(
        audit_log_inputs, merge_worker_logs=not args.no_merge_worker_logs
    )
    repair_rows_from_audit = []  # type: List[dict]
    if audit_paths:
        print("loading audit logs paths=%s" % len(audit_paths), flush=True)
        audit_records = load_audit_records(audit_paths)
        print("audit_records=%s" % len(audit_records), flush=True)
        repair_rows_from_audit = build_repair_rows_from_audit(audit_records)
        summarize_audit_repair_plan(repair_rows_from_audit, audit_paths)
        if args.count_only:
            print("count_only finished (audit repair plan only)", flush=True)
            return 0
        if not repair_rows_from_audit:
            print("no repair rows from audit logs", flush=True)
            return 1

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
            "start dry_run=%s strategy=%s tables=%s scan_size=%s commit_every=%s "
            "workers=%s work_limit=%s repair_wrong_loan=%s from_audit=%s delete_log=%s"
            % (
                dry_run,
                args.strategy,
                args.tables,
                args.scan_size,
                args.commit_every,
                args.workers,
                args.work_limit,
                args.repair_wrong_loan,
                len(repair_rows_from_audit),
                delete_log_path if not args.no_delete_log else "(disabled)",
            ),
            flush=True,
        )
        if args.tables in ("loan", "all"):
            if repair_rows_from_audit:
                if args.workers > 1:
                    print("warning: --workers ignored for --from-audit-log", flush=True)
                ok, skip = run_loans_from_audit(
                    tgt,
                    repair_rows_from_audit,
                    dry_run,
                    args.strategy,
                    args.work_limit,
                    args.log_every,
                    audit,
                    args.commit_every,
                )
            elif args.workers > 1 and args.worker_id < 0:
                ok, skip = run_loans_parallel(args, args.env, delete_log_path)
            else:
                seg_max = args.loan_no_max or None
                label = ""
                if args.worker_id >= 0:
                    label = "w%s/%s" % (args.worker_id, max(args.workers, 1))
                ok, skip = run_loans(
                    tgt,
                    dry_run,
                    args.strategy,
                    args.scan_size,
                    args.work_limit,
                    args.log_every,
                    audit,
                    args.commit_every,
                    args.loan_no_min,
                    seg_max,
                    label,
                    args.repair_wrong_loan,
                    args.wrong_sn_min_len,
                )
            print("loan done ok=%s skip=%s" % (ok, skip), flush=True)
        if args.tables in ("application", "all"):
            if args.workers > 1:
                print("warning: --workers ignored for application table", flush=True)
            ok, skip = run_applications(
                tgt,
                dry_run,
                args.strategy,
                args.scan_size,
                args.work_limit,
                args.log_every,
                audit,
                args.commit_every,
            )
            print("application done ok=%s skip=%s" % (ok, skip), flush=True)
        print("finished delete_log=%s" % (delete_log_path or ""), flush=True)
        return 0
    finally:
        audit.close()
        tgt.close()


if __name__ == "__main__":
    raise SystemExit(main())
