#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""清洗已有 plan：去掉仅因 date vs str 产生的假 update。

Usage:
  python3 filter_reconcile_plan_false_dates.py \\
    --in /tmp/reconcile_loan_plan.jsonl \\
    --out /tmp/reconcile_loan_plan.filtered.jsonl

  # 原地替换（先写 .bak）
  python3 filter_reconcile_plan_false_dates.py \\
    --in /tmp/reconcile_loan_plan.jsonl --inplace
"""
from __future__ import print_function

import argparse
import json
import shutil
import sys
from pathlib import Path
from typing import Any, Dict, Optional

# 复用 reconcile 归一化
import reconcile_tables as rec

DATE_COLS = (
    "start_date", "due_date", "due_date_final", "paid_off_date", "birthday",
)


def _diff_still_real(diff: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    out = {}  # type: Dict[str, Dict[str, Any]]
    for col, pair in (diff or {}).items():
        if not isinstance(pair, dict):
            out[col] = pair
            continue
        ev = rec.normalize_cell(col, pair.get("expected"))
        av = rec.normalize_cell(col, pair.get("target"))
        if ev != av:
            out[col] = {"target": av, "expected": ev}
    return out


def filter_file(in_path: Path, out_path: Path) -> Dict[str, int]:
    stats = {
        "in": 0,
        "out": 0,
        "drop_empty_diff": 0,
        "kept_insert": 0,
        "kept_update": 0,
        "rewrote_diff": 0,
    }
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with in_path.open(encoding="utf-8") as fin, out_path.open(
        "w", encoding="utf-8"
    ) as fout:
        for line in fin:
            line = line.strip()
            if not line:
                continue
            stats["in"] += 1
            row = json.loads(line)
            action = row.get("action")
            if action == "insert":
                fout.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")
                stats["out"] += 1
                stats["kept_insert"] += 1
                continue
            if action != "update":
                fout.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")
                stats["out"] += 1
                continue
            new_diff = _diff_still_real(row.get("diff") or {})
            if not new_diff:
                stats["drop_empty_diff"] += 1
                continue
            if new_diff != (row.get("diff") or {}):
                stats["rewrote_diff"] += 1
                row["diff"] = new_diff
            fout.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")
            stats["out"] += 1
            stats["kept_update"] += 1
    return stats


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Filter false date diffs from reconcile plan")
    p.add_argument("--in", dest="in_path", required=True)
    p.add_argument("--out", dest="out_path", default="")
    p.add_argument("--inplace", action="store_true")
    args = p.parse_args(argv)

    in_path = Path(args.in_path)
    if not in_path.is_file():
        print("missing: {0}".format(in_path), file=sys.stderr)
        return 1

    if args.inplace:
        bak = in_path.with_suffix(in_path.suffix + ".bak")
        shutil.copy2(str(in_path), str(bak))
        out_path = Path(str(in_path) + ".filtered.tmp")
        stats = filter_file(in_path, out_path)
        out_path.replace(in_path)
        print("bak={0}".format(bak), flush=True)
    else:
        out_path = Path(args.out_path) if args.out_path else Path(str(in_path) + ".filtered")
        stats = filter_file(in_path, out_path)
        print("out={0}".format(out_path), flush=True)

    print("stats={0}".format(stats), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
