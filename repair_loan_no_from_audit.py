#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""按 backfill 审计 CSV 修复 loan_no。

流程:
  1. 读本地审计 CSV（及可选 backfill_loan.log）
  2. 用 old_loan_no 在本地拼目标号，例如:
       NG-217798621621  ->  ng-217798621621-01000
  3. 用 CSV 的 new_loan_no（库里当前错误号）定位 loan 行并改成目标号
  4. 每条打印 REPAIR_AUDIT；删除/修改的完整行写入 .deleted.csv / .modified.csv

审计 CSV 列（与 backfill_delete_audit_*.csv 一致）:
  ts,action,table,old_application_no,new_application_no,old_loan_no,new_loan_no,app_id

Usage:
  # 仅生成修复计划（不写库）
  python3 repair_loan_no_from_audit.py \\
    --audit-csv /tmp/backfill_delete_audit_loan.csv \\
    --plan-out /tmp/repair_loan_plan.csv

  # 试跑
  python3 repair_loan_no_from_audit.py \\
    --env ./ng_migration.env --dry-run \\
    --audit-csv /tmp/backfill_delete_audit_loan.csv,/tmp/backfill_loan.log \\
    --repair-log /tmp/repair_loan_audit.csv

  # 正式修复
  python3 repair_loan_no_from_audit.py \\
    --env ./ng_migration.env --apply \\
    --audit-csv /tmp/backfill_delete_audit_loan.csv \\
    --repair-log /tmp/repair_loan_audit.csv \\
    --commit-every 20 --workers 4
"""
import argparse
import json
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
LEGACY_LOAN_NO_RE = re.compile(r"^[Nn][Gg]-(\d+)$")
DELETE_AUDIT_LINE_RE = re.compile(r"^DELETE_AUDIT\s+(.+)$")
REPAIR_AUDIT_LINE_RE = re.compile(r"^REPAIR_AUDIT\s+(.+)$")
AUDIT_HEADER = (
    "ts,action,table,old_application_no,new_application_no,"
    "old_loan_no,new_loan_no,app_id"
)
PLAN_HEADER = (
    "wrong_loan_no,correct_loan_no,legacy_loan_no,application_no,app_id,source"
)
DELETED_ROW_HEADER = "ts,action,loan_no,application_no,row_json"
MODIFIED_ROW_HEADER = "ts,action,old_loan_no,new_loan_no,before_json,after_json"


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


def core_sn_from_legacy_loan_no(loan_no: str) -> str:
    m = LEGACY_LOAN_NO_RE.match(str(loan_no or "").strip())
    return m.group(1) if m else ""


def correct_loan_no_from_legacy(legacy_loan_no: str) -> str:
    core_sn = core_sn_from_legacy_loan_no(legacy_loan_no)
    if not core_sn:
        return ""
    return mig.format_loan_no(core_sn, 1, 0)


def parse_audit_line(line: str) -> Optional[dict]:
    raw = str(line or "").strip()
    if not raw or raw == AUDIT_HEADER:
        return None
    for pat in (DELETE_AUDIT_LINE_RE, REPAIR_AUDIT_LINE_RE):
        m = pat.match(raw)
        if m:
            raw = m.group(1).strip()
            break
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


def expand_audit_paths(paths: List[str], merge_worker_logs: bool) -> List[str]:
    out = []  # type: List[str]
    seen = set()
    for raw in paths:
        base = Path(raw).expanduser()
        candidates = [base]
        if merge_worker_logs and base.suffix.lower() == ".csv" and not _is_worker_audit_path(base):
            for i in range(32):
                wp = base.parent / ("%s.w%s%s" % (base.stem, i, base.suffix))
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
        for line in p.read_text(encoding="utf-8", errors="replace").splitlines():
            rec = parse_audit_line(line)
            if rec:
                rec["_source"] = str(p)
                records.append(rec)
    return records


def build_repair_plan(records: List[dict]) -> Tuple[List[dict], Dict[str, int]]:
    """wrong_loan_no=CSV new_loan_no; correct_loan_no=由 old_loan_no 本地拼接。"""
    plan = []  # type: List[dict]
    seen_wrong = set()
    skipped = {}  # type: Dict[str, int]

    for rec in records:
        if rec.get("table") != "loan":
            skipped["not_loan"] = skipped.get("not_loan", 0) + 1
            continue
        if rec.get("action") not in ("delete", "would_delete"):
            skipped["not_delete"] = skipped.get("not_delete", 0) + 1
            continue

        legacy = str(rec.get("old_loan_no") or "").strip()
        wrong = str(rec.get("new_loan_no") or "").strip()
        if not legacy or not wrong:
            skipped["missing_loan_no"] = skipped.get("missing_loan_no", 0) + 1
            continue

        correct = correct_loan_no_from_legacy(legacy)
        if not correct:
            skipped["bad_legacy"] = skipped.get("bad_legacy", 0) + 1
            continue
        if correct == wrong:
            skipped["already_correct"] = skipped.get("already_correct", 0) + 1
            continue
        if wrong in seen_wrong:
            skipped["dup_wrong"] = skipped.get("dup_wrong", 0) + 1
            continue

        seen_wrong.add(wrong)
        plan.append(
            {
                "wrong_loan_no": wrong,
                "correct_loan_no": correct,
                "legacy_loan_no": legacy,
                "application_no": rec.get("new_application_no")
                or rec.get("old_application_no")
                or "",
                "app_id": rec.get("app_id") or "",
                "source": rec.get("_source", ""),
            }
        )

    plan.sort(key=lambda r: r["wrong_loan_no"])
    return plan, skipped


def write_plan_csv(path: str, plan: List[dict]) -> None:
    with open(path, "w", encoding="utf-8") as fp:
        fp.write(PLAN_HEADER + "\n")
        for row in plan:
            fp.write(
                "%s,%s,%s,%s,%s,%s\n"
                % (
                    row["wrong_loan_no"],
                    row["correct_loan_no"],
                    row["legacy_loan_no"],
                    row["application_no"],
                    row["app_id"],
                    row.get("source", ""),
                )
            )


class RepairAuditLog(object):
    HEADER = (
        "ts,action,wrong_loan_no,correct_loan_no,legacy_loan_no,"
        "application_no,app_id,result"
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

    def record(self, action: str, row: dict, result: str):
        if not self.enabled:
            return
        line = "%s,%s,%s,%s,%s,%s,%s,%s" % (
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            action,
            row.get("wrong_loan_no", ""),
            row.get("correct_loan_no", ""),
            row.get("legacy_loan_no", ""),
            row.get("application_no", ""),
            row.get("app_id", ""),
            result,
        )
        print("REPAIR_AUDIT %s" % line, flush=True)
        if self._fp:
            self._fp.write(line + "\n")
            self._fp.flush()


def _loan_row_json(row: Optional[dict]) -> str:
    if not row:
        return ""
    payload = {c: row.get(c) for c in LOAN_COLS}
    return json.dumps(payload, ensure_ascii=False, default=str)


def _audit_sidecar_path(base: str, suffix: str) -> str:
    if not base:
        return ""
    p = Path(base)
    return str(p.with_name("%s.%s%s" % (p.stem, suffix, p.suffix or ".csv")))


class RowChangeAuditLog(object):
    """Record full loan rows for every delete / update."""

    def __init__(self, base_path: str, enabled: bool = True):
        self.enabled = enabled
        self.deleted_path = _audit_sidecar_path(base_path, "deleted")
        self.modified_path = _audit_sidecar_path(base_path, "modified")
        self._deleted_fp = None  # type: Optional[TextIO]
        self._modified_fp = None  # type: Optional[TextIO]
        if enabled and base_path:
            self._deleted_fp = open(self.deleted_path, "a", encoding="utf-8")
            if self._deleted_fp.tell() == 0:
                self._deleted_fp.write(DELETED_ROW_HEADER + "\n")
            self._modified_fp = open(self.modified_path, "a", encoding="utf-8")
            if self._modified_fp.tell() == 0:
                self._modified_fp.write(MODIFIED_ROW_HEADER + "\n")

    def close(self):
        for fp in (self._deleted_fp, self._modified_fp):
            if fp:
                fp.close()
        self._deleted_fp = None
        self._modified_fp = None

    def _emit(self, kind: str, line: str):
        tag = "ROW_DELETED" if kind == "deleted" else "ROW_MODIFIED"
        print("%s %s" % (tag, line), flush=True)
        fp = self._deleted_fp if kind == "deleted" else self._modified_fp
        if fp:
            fp.write(line + "\n")
            fp.flush()

    def record_deleted(self, action: str, row: dict):
        if not self.enabled:
            return
        loan_no = str(row.get("loan_no") or "")
        app_no = str(row.get("application_no") or "")
        line = "%s,%s,%s,%s,%s" % (
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            action,
            loan_no,
            app_no,
            _loan_row_json(row),
        )
        self._emit("deleted", line)

    def record_modified(self, action: str, before: dict, after: dict):
        if not self.enabled:
            return
        line = "%s,%s,%s,%s,%s,%s" % (
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            action,
            str(before.get("loan_no") or ""),
            str(after.get("loan_no") or ""),
            _loan_row_json(before),
            _loan_row_json(after),
        )
        self._emit("modified", line)


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


def cols_sql(cols: List[str]) -> str:
    return ", ".join("`%s`" % c for c in cols)


def loan_exists(tgt, loan_no: str) -> bool:
    with tgt.cursor() as cur:
        cur.execute("SELECT 1 FROM loan WHERE loan_no=%s LIMIT 1", (loan_no,))
        return cur.fetchone() is not None


def fetch_loan_row(tgt, loan_no: str) -> Optional[dict]:
    sql = "SELECT %s FROM loan WHERE loan_no=%%s" % cols_sql(LOAN_COLS)
    with tgt.cursor() as cur:
        cur.execute(sql, (loan_no,))
        return cur.fetchone()


def fetch_loan_application_no(tgt, loan_no: str) -> Optional[str]:
    with tgt.cursor() as cur:
        cur.execute(
            "SELECT application_no FROM loan WHERE loan_no=%s LIMIT 1", (loan_no,)
        )
        row = cur.fetchone()
        return str(row["application_no"]) if row else None


def insert_row(tgt, table: str, cols: List[str], row: dict) -> None:
    placeholders = ", ".join(["%s"] * len(cols))
    sql = "INSERT INTO %s (%s) VALUES (%s)" % (table, cols_sql(cols), placeholders)
    with tgt.cursor() as cur:
        cur.execute(sql, [row[c] for c in cols])


def repair_one_loan(
    tgt,
    row: dict,
    dry_run: bool,
    strategy: str,
    audit: RepairAuditLog,
    row_audit: Optional[RowChangeAuditLog],
    tracker: Optional[CommitTracker],
) -> str:
    wrong = row["wrong_loan_no"]
    correct = row["correct_loan_no"]
    app_no = row.get("application_no") or ""

    if not loan_exists(tgt, wrong):
        if loan_exists(tgt, correct):
            audit.record("skip_done", row, "wrong_missing_correct_exists")
            return "skip_done"
        audit.record("skip", row, "wrong_missing")
        return "skip_missing"

    if loan_exists(tgt, correct):
        existing_app = fetch_loan_application_no(tgt, correct)
        if app_no and existing_app and existing_app != app_no:
            audit.record("skip", row, "conflict_correct_app")
            return "conflict"
        before = fetch_loan_row(tgt, wrong)
        if dry_run:
            if before and row_audit:
                row_audit.record_deleted("would_delete", before)
            audit.record("would_delete", row, "delete_wrong_keep_correct")
            return "ok"
        if before and row_audit:
            row_audit.record_deleted("delete", before)
        with tgt.cursor() as cur:
            cur.execute("DELETE FROM loan WHERE loan_no=%s", (wrong,))
            if not cur.rowcount:
                audit.record("skip", row, "delete_wrong_failed")
                return "missing"
        audit.record("delete", row, "delete_wrong_keep_correct")
        if tracker:
            tracker.note_write()
        else:
            tgt.commit()
        return "ok"

    before = fetch_loan_row(tgt, wrong)
    if dry_run:
        if before and row_audit:
            after = dict(before)
            after["loan_no"] = correct
            row_audit.record_modified("would_update", before, after)
        audit.record("would_update", row, "update_loan_no")
        return "ok"

    if strategy == "insert-delete":
        if not before:
            audit.record("skip", row, "fetch_wrong_missing")
            return "missing"
        after = dict(before)
        after["loan_no"] = correct
        insert_row(tgt, "loan", LOAN_COLS, after)
        with tgt.cursor() as cur:
            cur.execute("DELETE FROM loan WHERE loan_no=%s", (wrong,))
            if not cur.rowcount:
                raise RuntimeError("delete failed wrong_loan_no=%s" % wrong)
        if row_audit:
            row_audit.record_deleted("delete", before)
            row_audit.record_modified("insert_delete", before, after)
        audit.record("update", row, "insert_delete")
    else:
        if not before:
            audit.record("skip", row, "fetch_wrong_missing")
            return "missing"
        with tgt.cursor() as cur:
            cur.execute(
                "UPDATE loan SET loan_no=%s WHERE loan_no=%s",
                (correct, wrong),
            )
            if not cur.rowcount:
                audit.record("skip", row, "update_no_row")
                return "missing"
        after = fetch_loan_row(tgt, correct) or dict(before)
        after["loan_no"] = correct
        if row_audit:
            row_audit.record_modified("update", before, after)
        audit.record("update", row, "update_loan_no")

    if tracker:
        tracker.note_write()
    else:
        tgt.commit()
    return "ok"


def run_repair(
    tgt,
    plan: List[dict],
    dry_run: bool,
    strategy: str,
    work_limit: int,
    log_every: int,
    audit: RepairAuditLog,
    row_audit: Optional[RowChangeAuditLog],
    commit_every: int,
    worker_label: str = "",
) -> Tuple[int, int]:
    ok = skip = 0
    tracker = CommitTracker(tgt, commit_every, dry_run)
    counts = {}  # type: Dict[str, int]
    prefix = "[%s] " % worker_label if worker_label else ""
    for row in plan:
        status = exec_with_retry(
            tgt,
            lambda r=row: repair_one_loan(
                tgt, r, dry_run, strategy, audit, row_audit, tracker
            ),
            "repair wrong=%s" % row["wrong_loan_no"],
        )
        counts[status] = counts.get(status, 0) + 1
        if status == "ok":
            ok += 1
        else:
            skip += 1
        if work_limit and ok >= work_limit:
            tracker.flush()
            print("%sstop work_limit=%s" % (prefix, work_limit), flush=True)
            break
        if (ok + skip) % max(1, log_every) == 0:
            print(
                "%sprogress ok=%s skip=%s last_wrong=%s"
                % (prefix, ok, skip, row["wrong_loan_no"]),
                flush=True,
            )
    tracker.flush()
    reasons = " ".join("%s=%s" % (k, v) for k, v in sorted(counts.items()))
    print(
        "%srepair done ok=%s skip=%s reasons=[%s]" % (prefix, ok, skip, reasons or "-"),
        flush=True,
    )
    return ok, skip


def split_plan_chunks(plan: List[dict], workers: int) -> List[List[dict]]:
    n = max(1, int(workers))
    if not plan:
        return []
    size = len(plan)
    chunks = []  # type: List[List[dict]]
    for i in range(n):
        start = i * size // n
        end = (i + 1) * size // n
        if start >= end:
            chunks.append([])
        else:
            chunks.append(plan[start:end])
    return chunks


def _worker_repair_log_path(base: str, worker_id: int) -> str:
    if not base:
        return ""
    p = Path(base)
    return str(p.with_name("%s.w%s%s" % (p.stem, worker_id, p.suffix or ".csv")))


def repair_worker_run(spec: dict) -> Tuple[int, int]:
    worker_id = spec["worker_id"]
    workers = spec["workers"]
    label = "w%s/%s" % (worker_id, workers)
    chunk = spec.get("plan_chunk") or []
    if not chunk:
        print("[%s] empty chunk, skip" % label, flush=True)
        return 0, 0

    cfg = load_env(Path(spec["env"]))
    tgt = connect_target(cfg)
    if spec.get("no_repair_log"):
        audit = RepairAuditLog(None, enabled=False)
        row_audit = RowChangeAuditLog("", enabled=False)
        repair_log = ""
    else:
        repair_log = spec.get("repair_log") or ""
        audit = RepairAuditLog(repair_log or None, enabled=bool(repair_log))
        row_audit = RowChangeAuditLog(repair_log or "", enabled=bool(repair_log))

    try:
        print(
            "[%s] start rows=%s first=%s last=%s repair_log=%s deleted=%s modified=%s"
            % (
                label,
                len(chunk),
                chunk[0]["wrong_loan_no"],
                chunk[-1]["wrong_loan_no"],
                repair_log or "",
                row_audit.deleted_path if row_audit.enabled else "",
                row_audit.modified_path if row_audit.enabled else "",
            ),
            flush=True,
        )
        ok, skip = run_repair(
            tgt,
            chunk,
            spec["dry_run"],
            spec["strategy"],
            spec["work_limit"],
            spec["log_every"],
            audit,
            row_audit,
            spec["commit_every"],
            label,
        )
        print("[%s] done ok=%s skip=%s" % (label, ok, skip), flush=True)
        return ok, skip
    finally:
        audit.close()
        if row_audit:
            row_audit.close()
        tgt.close()


def run_repair_parallel(
    plan: List[dict],
    workers: int,
    env_path: str,
    dry_run: bool,
    strategy: str,
    work_limit: int,
    log_every: int,
    commit_every: int,
    repair_log_path: str,
    no_repair_log: bool,
) -> Tuple[int, int]:
    chunks = split_plan_chunks(plan, workers)
    specs = []
    for i, chunk in enumerate(chunks):
        if not chunk:
            continue
        wlog = _worker_repair_log_path(repair_log_path, i) if repair_log_path else ""
        specs.append(
            {
                "worker_id": i,
                "workers": workers,
                "env": env_path,
                "dry_run": dry_run,
                "strategy": strategy,
                "work_limit": work_limit,
                "log_every": log_every,
                "commit_every": commit_every,
                "plan_chunk": chunk,
                "repair_log": wlog,
                "no_repair_log": no_repair_log,
            }
        )
    if not specs:
        print("no plan rows to process", flush=True)
        return 0, 0

    for spec in specs:
        chunk = spec["plan_chunk"]
        print(
            "  worker %s/%s: rows=%s [%s .. %s] log=%s"
            % (
                spec["worker_id"],
                workers,
                len(chunk),
                chunk[0]["wrong_loan_no"],
                chunk[-1]["wrong_loan_no"],
                spec.get("repair_log") or "",
            ),
            flush=True,
        )

    ctx = multiprocessing.get_context("spawn")
    with ctx.Pool(processes=len(specs)) as pool:
        results = pool.map(repair_worker_run, specs)

    total_ok = sum(r[0] for r in results)
    total_skip = sum(r[1] for r in results)
    print(
        "repair parallel done workers=%s ok=%s skip=%s"
        % (len(specs), total_ok, total_skip),
        flush=True,
    )
    return total_ok, total_skip


def split_csv_args(values: List[str]) -> List[str]:
    out = []
    for v in values:
        for part in str(v).split(","):
            part = part.strip()
            if part:
                out.append(part)
    return out


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(
        description="Read backfill audit CSV, fix loan_no: wrong(new_loan_no) -> correct(from old_loan_no)"
    )
    p.add_argument("--env", default=str(HERE / "ng_migration.env"))
    p.add_argument("--apply", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument(
        "--audit-csv",
        action="append",
        default=[],
        help="审计 CSV 或 backfill_loan.log；可多次或逗号分隔",
    )
    p.add_argument(
        "--no-merge-worker-logs",
        action="store_true",
        help="不自动合并 .w0.csv .w1.csv ...",
    )
    p.add_argument(
        "--plan-out",
        default="",
        help="把本地拼好的修复计划写出 CSV（wrong -> correct）",
    )
    p.add_argument(
        "--plan-only",
        action="store_true",
        help="只读审计、拼计划、写 --plan-out，不连库",
    )
    p.add_argument(
        "--strategy",
        choices=["update", "insert-delete"],
        default="update",
        help="update: 原地改 loan_no；insert-delete: INSERT 新行再 DELETE 旧行",
    )
    p.add_argument("--commit-every", type=int, default=20)
    p.add_argument("--work-limit", type=int, default=0)
    p.add_argument("--log-every", type=int, default=100)
    p.add_argument(
        "--workers",
        type=int,
        default=1,
        help="并行 worker 数，按修复计划均分（每 worker 独立连接 + repair_log.wN.csv）",
    )
    p.add_argument(
        "--repair-log",
        default="",
        help="REPAIR_AUDIT 日志路径（默认 /tmp/repair_loan_audit_YYYYMMDD_HHMMSS.csv）",
    )
    p.add_argument("--no-repair-log", action="store_true")
    p.add_argument(
        "--no-row-log",
        action="store_true",
        help="不记录删除/修改的完整行（默认随 --repair-log 自动写 .deleted.csv / .modified.csv）",
    )
    args = p.parse_args(argv)

    if args.apply and args.dry_run:
        p.error("use either --apply or --dry-run, not both")
    dry_run = not args.apply

    inputs = split_csv_args(args.audit_csv)
    if not inputs:
        p.error("--audit-csv is required")

    paths = expand_audit_paths(inputs, merge_worker_logs=not args.no_merge_worker_logs)
    if not paths:
        print("no audit files found", flush=True)
        return 1

    print("audit_files=%s" % len(paths), flush=True)
    for path in paths:
        print("  %s" % path, flush=True)

    records = load_audit_records(paths)
    print("audit_records=%s" % len(records), flush=True)
    plan, skipped = build_repair_plan(records)
    if skipped:
        print(
            "plan_skipped %s"
            % " ".join("%s=%s" % (k, v) for k, v in sorted(skipped.items())),
            flush=True,
        )
    print("repair_plan=%s" % len(plan), flush=True)
    for row in plan[:10]:
        print(
            "  %s -> %s (legacy=%s)"
            % (row["wrong_loan_no"], row["correct_loan_no"], row["legacy_loan_no"]),
            flush=True,
        )
    if len(plan) > 10:
        print("  ... and %s more" % (len(plan) - 10), flush=True)

    plan_out = args.plan_out
    if not plan_out and (args.plan_only or dry_run):
        plan_out = "/tmp/repair_loan_plan.csv"
    if plan_out:
        write_plan_csv(plan_out, plan)
        print("plan_out=%s rows=%s" % (plan_out, len(plan)), flush=True)

    if args.plan_only:
        return 0 if plan else 1

    if args.workers < 1:
        p.error("--workers must be >= 1")

    repair_log = args.repair_log
    if not repair_log and not args.no_repair_log:
        repair_log = "/tmp/repair_loan_audit_%s.csv" % datetime.now().strftime(
            "%Y%m%d_%H%M%S"
        )

    if not plan:
        return 1

    print(
        "start dry_run=%s strategy=%s plan=%s workers=%s commit_every=%s repair_log=%s row_log=%s"
        % (
            dry_run,
            args.strategy,
            len(plan),
            args.workers,
            args.commit_every,
            repair_log if not args.no_repair_log else "(disabled)",
            "off" if args.no_row_log or args.no_repair_log else "deleted+modified",
        ),
        flush=True,
    )

    if args.workers > 1:
        ok, skip = run_repair_parallel(
            plan,
            args.workers,
            args.env,
            dry_run,
            args.strategy,
            args.work_limit,
            args.log_every,
            args.commit_every,
            repair_log,
            args.no_repair_log,
        )
        print("finished ok=%s skip=%s repair_log=%s" % (ok, skip, repair_log or ""), flush=True)
        return 0 if ok or skip else 1

    audit = RepairAuditLog(repair_log or None, enabled=not args.no_repair_log)
    row_audit = RowChangeAuditLog(
        repair_log or "",
        enabled=not args.no_repair_log and not args.no_row_log,
    )
    cfg = load_env(Path(args.env))
    tgt = connect_target(cfg)
    try:
        if row_audit.enabled:
            print("row_deleted_log=%s" % row_audit.deleted_path, flush=True)
            print("row_modified_log=%s" % row_audit.modified_path, flush=True)
        ok, skip = run_repair(
            tgt,
            plan,
            dry_run,
            args.strategy,
            args.work_limit,
            args.log_every,
            audit,
            row_audit,
            args.commit_every,
        )
        print("finished ok=%s skip=%s repair_log=%s" % (ok, skip, repair_log or ""), flush=True)
        return 0 if ok or skip else 1
    finally:
        audit.close()
        row_audit.close()
        tgt.close()


if __name__ == "__main__":
    raise SystemExit(main())
