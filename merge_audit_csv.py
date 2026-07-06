#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""合并 /tmp 下分散的 repair/backfill 审计 CSV（含 worker 分片）。

按「族」+「类型」合并:
  - main:     *.csv（不含 .deleted / .modified）
  - deleted:  *.deleted.csv
  - modified: *.modified.csv

输出:
  {output_dir}/{family}_{kind}_merged.csv

Usage:
  python3 merge_audit_csv.py --dir /tmp --output-dir /tmp/merged --dry-run
  python3 merge_audit_csv.py --dir /tmp --output-dir /tmp/merged

  python3 merge_audit_csv.py --dir /tmp --families repair_loan_status20 --dry-run
"""
import argparse
import re
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

HERE = Path(__file__).resolve().parent

# 识别文件名所属的审计「族」
FAMILY_RULES = (
    ("repair_loan_status20", re.compile(r"^repair_loan_status20")),
    ("backfill_delete_audit", re.compile(r"^backfill_delete_audit")),
    ("repair_drop_wrong_app_no", re.compile(r"^repair_drop_wrong_app_no")),
    ("repair_loan_app_no_market", re.compile(r"^repair_loan_app_no_market")),
    ("repair_loan_audit", re.compile(r"^repair_loan_audit")),
)

KINDS = ("main", "deleted", "modified")


def infer_family(name: str) -> Optional[str]:
    for fam, pat in FAMILY_RULES:
        if pat.match(name):
            return fam
    return None


def infer_kind(name: str) -> Optional[str]:
    if name.endswith(".deleted.csv"):
        return "deleted"
    if name.endswith(".modified.csv"):
        return "modified"
    if name.endswith(".csv"):
        return "main"
    return None


def scan_files(
    root: Path, families: Optional[Set[str]]
) -> Dict[Tuple[str, str], List[Path]]:
    groups: Dict[Tuple[str, str], List[Path]] = defaultdict(list)
    for p in sorted(root.iterdir()):
        if not p.is_file():
            continue
        fam = infer_family(p.name)
        kind = infer_kind(p.name)
        if not fam or not kind:
            continue
        if families and fam not in families:
            continue
        groups[(fam, kind)].append(p)
    for key in groups:
        groups[key] = sorted(groups[key], key=lambda x: x.name)
    return groups


def read_lines(path: Path) -> List[str]:
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError as exc:
        print("skip read error %s: %s" % (path, exc), flush=True)
        return []
    return [ln for ln in text.splitlines() if ln.strip()]


def is_header_line(line: str, header: Optional[str]) -> bool:
    if not header:
        return False
    s = line.strip()
    if s == header:
        return True
    if s.startswith("REPAIR_AUDIT "):
        s = s[len("REPAIR_AUDIT ") :]
    if s.startswith("ROW_DELETED ") or s.startswith("ROW_MODIFIED "):
        return False
    return s.split(",")[0] in ("ts", "timestamp")


def merge_group(files: List[Path], dry_run: bool) -> Tuple[int, Optional[str]]:
    if not files:
        return 0, None
    header: Optional[str] = None
    out_lines: List[str] = []
    seen_data: Set[str] = set()
    dup = 0
    for fp in files:
        lines = read_lines(fp)
        if not lines:
            continue
        start = 0
        if is_header_line(lines[0], header) or (
            header is None and is_header_line(lines[0], lines[0])
        ):
            if header is None:
                header = lines[0].strip()
                if header.startswith("REPAIR_AUDIT "):
                    header = header[len("REPAIR_AUDIT ") :]
            start = 1
        elif header is None and not lines[0].startswith("ts,"):
            # 无表头文件，保留原样
            pass
        elif header is None:
            header = lines[0].strip()
            start = 1
        for ln in lines[start:]:
            key = ln.strip()
            if key in seen_data:
                dup += 1
                continue
            seen_data.add(key)
            out_lines.append(ln)
    if dry_run:
        print(
            "  files=%s data_lines=%s dup_skipped=%s header=%s"
            % (len(files), len(out_lines), dup, (header or "")[:60]),
            flush=True,
        )
        return len(out_lines), header
    return len(out_lines), header if out_lines else header


def write_merged(
    out_path: Path, header: Optional[str], files: List[Path]
) -> Tuple[int, int]:
    header_line = header
    total = 0
    dup = 0
    seen: Set[str] = set()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as out:
        first = True
        for fp in files:
            lines = read_lines(fp)
            if not lines:
                continue
            start = 0
            if is_header_line(lines[0], header_line) or (
                header_line is None and lines[0].startswith("ts,")
            ):
                if header_line is None:
                    header_line = lines[0].strip()
                start = 1
            if first and header_line:
                out.write(header_line + "\n")
                first = False
            for ln in lines[start:]:
                key = ln.strip()
                if key in seen:
                    dup += 1
                    continue
                seen.add(key)
                out.write(ln + "\n")
                total += 1
    return total, dup


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(description="Merge scattered audit CSV files in /tmp")
    p.add_argument("--dir", default="/tmp", help="扫描目录")
    p.add_argument("--output-dir", default="/tmp/merged", help="合并结果输出目录")
    p.add_argument(
        "--families",
        default="",
        help="逗号分隔族名，空=全部已知族",
    )
    p.add_argument("--dry-run", action="store_true", help="只统计不写文件")
    p.add_argument("--no-dedupe", action="store_true", help="不去重（默认按整行去重）")
    args = p.parse_args(argv)

    root = Path(args.dir)
    if not root.is_dir():
        print("not a directory: %s" % root, flush=True)
        return 1

    fam_filter: Optional[Set[str]] = None
    if args.families.strip():
        fam_filter = {x.strip() for x in args.families.split(",") if x.strip()}

    groups = scan_files(root, fam_filter)
    if not groups:
        print("no audit csv files found under %s" % root, flush=True)
        return 1

    print("scan dir=%s families=%s groups=%s" % (root, fam_filter or "ALL", len(groups)), flush=True)
    out_dir = Path(args.output_dir)
    grand_lines = 0

    for (fam, kind) in sorted(groups.keys()):
        files = groups[(fam, kind)]
        out_name = "%s_%s_merged.csv" % (fam, kind)
        out_path = out_dir / out_name
        print(
            "[%s/%s] files=%s -> %s"
            % (fam, kind, len(files), out_path),
            flush=True,
        )
        for fp in files[:5]:
            print("    %s" % fp.name, flush=True)
        if len(files) > 5:
            print("    ... and %s more" % (len(files) - 5), flush=True)

        if args.dry_run:
            n, _ = merge_group(files, dry_run=True)
            grand_lines += n
            continue

        if args.no_dedupe:
            # 简单拼接
            out_path.parent.mkdir(parents=True, exist_ok=True)
            header_written = False
            n = 0
            with out_path.open("w", encoding="utf-8") as out:
                for fp in files:
                    lines = read_lines(fp)
                    if not lines:
                        continue
                    start = 0
                    if lines[0].startswith("ts,"):
                        if not header_written:
                            out.write(lines[0] + "\n")
                            header_written = True
                        start = 1
                    for ln in lines[start:]:
                        out.write(ln + "\n")
                        n += 1
            print("  wrote lines=%s dup=0 (no-dedupe)" % n, flush=True)
            grand_lines += n
        else:
            n, dup = write_merged(out_path, None, files)
            print("  wrote lines=%s dup_skipped=%s" % (n, dup), flush=True)
            grand_lines += n

    print("done groups=%s total_data_lines=%s out=%s" % (len(groups), grand_lines, out_dir), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
