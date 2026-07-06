#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""按 loan_no 中间数字段长度统计/抽样（区分 market 长单号 vs core 短 sn）。

loan_no 格式: ng-{sn}-01000
  - 长 sn（常见 15~18 位）: 多为 backfill / 旧 market applicationNo
  - 短 sn（常见 12 位）: 多为 window upsert 用的 core.application.sn

Usage:
  python3 audit_loan_sn_length.py --env ./ng_migration.env
  python3 audit_loan_sn_length.py --env ./ng_migration.env --len 18 --limit 20
  python3 audit_loan_sn_length.py --env ./ng_migration.env --dup-by-app
"""
import argparse
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pymysql
from pymysql.cursors import DictCursor

HERE = Path(__file__).resolve().parent
LOAN_NO_RE = re.compile(r"^ng-(\d+)-01000$", re.I)


def load_env(path: Path) -> Dict[str, str]:
    cfg: Dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        cfg[k.strip()] = v.strip().strip("'\"")
    return cfg


def connect(cfg: Dict[str, str]):
    return pymysql.connect(
        host=cfg["TARGET_HOST"],
        port=int(cfg.get("TARGET_PORT", "3306")),
        user=cfg["TARGET_USER"],
        password=cfg["TARGET_PASSWORD"],
        database=cfg.get("TARGET_DB", "ng"),
        charset="utf8mb4",
        cursorclass=DictCursor,
        read_timeout=3600,
    )


def parse_loan_sn(loan_no: str) -> Optional[Tuple[str, int]]:
    m = LOAN_NO_RE.match(str(loan_no or "").strip())
    if not m:
        return None
    sn = m.group(1)
    return sn, len(sn)


def scan_loans(tgt, after: str, batch: int) -> List[dict]:
    sql = """
        SELECT loan_no, application_no, status
        FROM loan
        WHERE loan_no > %s
        ORDER BY loan_no ASC
        LIMIT %s
    """
    with tgt.cursor() as cur:
        cur.execute(sql, (after, batch))
        return list(cur.fetchall())


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(description="Audit loan_no sn length on target")
    p.add_argument("--env", default=str(HERE / "ng_migration.env"))
    p.add_argument("--scan-size", type=int, default=5000)
    p.add_argument("--len", type=int, default=0, help="only show this sn length (e.g. 18)")
    p.add_argument("--min-len", type=int, default=0)
    p.add_argument("--max-len", type=int, default=0)
    p.add_argument("--limit", type=int, default=10, help="sample rows per length bucket")
    p.add_argument(
        "--dup-by-app",
        action="store_true",
        help="find application_no with multiple loan_no (different sn length)",
    )
    args = p.parse_args(argv)

    tgt = connect(load_env(Path(args.env)))
    try:
        by_len: Dict[int, int] = {}
        non_std = 0
        samples: Dict[int, List[dict]] = {}
        app_to_loans: Dict[str, List[dict]] = {}
        after = ""
        total = 0

        while True:
            rows = scan_loans(tgt, after, args.scan_size)
            if not rows:
                break
            after = str(rows[-1]["loan_no"])
            for row in rows:
                parsed = parse_loan_sn(row["loan_no"])
                if not parsed:
                    non_std += 1
                    continue
                sn, sn_len = parsed
                if args.len and sn_len != args.len:
                    continue
                if args.min_len and sn_len < args.min_len:
                    continue
                if args.max_len and sn_len > args.max_len:
                    continue

                total += 1
                by_len[sn_len] = by_len.get(sn_len, 0) + 1
                if sn_len not in samples:
                    samples[sn_len] = []
                if len(samples[sn_len]) < args.limit:
                    samples[sn_len].append(
                        {
                            "loan_no": row["loan_no"],
                            "sn": sn,
                            "application_no": row["application_no"],
                            "status": row["status"],
                        }
                    )
                if args.dup_by_app:
                    app_no = str(row["application_no"] or "")
                    app_to_loans.setdefault(app_no, []).append(
                        {
                            "loan_no": row["loan_no"],
                            "sn": sn,
                            "sn_len": sn_len,
                            "status": row["status"],
                        }
                    )
            if len(rows) < args.scan_size:
                break

        print("=== loan_no sn length summary (ng-{sn}-01000) ===", flush=True)
        print("scanned_matching_filter=%s non_std_loan_no=%s" % (total, non_std), flush=True)
        for sn_len in sorted(by_len):
            print("len=%s count=%s" % (sn_len, by_len[sn_len]), flush=True)
            for s in samples.get(sn_len, []):
                print(
                    "  sample loan_no=%s application_no=%s status=%s"
                    % (s["loan_no"], s["application_no"], s["status"]),
                    flush=True,
                )

        if args.dup_by_app:
            dup_cnt = 0
            print("=== application_no with multiple loan_no ===", flush=True)
            for app_no, loans in sorted(app_to_loans.items()):
                if len(loans) < 2:
                    continue
                lens = sorted({x["sn_len"] for x in loans})
                if len(lens) < 2 and len(loans) < 2:
                    continue
                dup_cnt += 1
                print(
                    "app=%s loans=%s lens=%s"
                    % (app_no, len(loans), lens),
                    flush=True,
                )
                for x in loans:
                    print(
                        "  loan_no=%s sn_len=%s status=%s"
                        % (x["loan_no"], x["sn_len"], x["status"]),
                        flush=True,
                    )
                if dup_cnt >= args.limit:
                    print("...(truncated at limit=%s)" % args.limit, flush=True)
                    break
            print("dup_application_count=%s" % dup_cnt, flush=True)

        print(
            "hint: len>=15 多为 market 旧号 backfill; len~12 多为 core sn 同步",
            flush=True,
        )
        return 0
    finally:
        tgt.close()


if __name__ == "__main__":
    raise SystemExit(main())
