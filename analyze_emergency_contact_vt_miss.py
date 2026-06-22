#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
只读分析：统计紧急联系人 VT 未命中情况，并导出明文清单。

与 ng_migration_run.py 中 _mobile_vt_lookup_candidates / _resolve_contact_mobile 逻辑一致。
不连接目标库、不写入任何业务表，不影响正在运行的迁移任务。

用法：
  cd docs/migration/ng-migration-runner
  python3 analyze_emergency_contact_vt_miss.py
  python3 analyze_emergency_contact_vt_miss.py --max-miss-rows 5000   # 仅导出前 N 条未命中
  python3 analyze_emergency_contact_vt_miss.py --max-users 100000      # 仅扫描前 N 个用户

输出（reports/ 目录）：
  emergency_contact_vt_miss_report.md   — 汇总统计
  emergency_contact_vt_miss_plaintext.csv — 未命中明细（含 user_id / 姓名 / 明文手机号）
  emergency_contact_vt_miss_unique_phones.txt — 去重明文手机号列表
"""
import csv
import json
import os
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

try:
    import pymysql
    from pymysql.cursors import DictCursor
except ImportError:
    print("请先安装: pip install pymysql", file=sys.stderr)
    sys.exit(1)

HERE = Path(__file__).resolve().parent
ENV_FILE = HERE / "ng_migration.env"
REPORT_DIR = HERE / "reports"

VT_MOBILE = "mobile"
VT_EMERGENCY_CONTACT = "emergency_contact"
VT_TYPES = (VT_MOBILE, VT_EMERGENCY_CONTACT)

USER_DATA_SQL = """
SELECT u.id AS user_id,
       u.`appId` AS app_id,
       u.mobile AS user_mobile,
       ud.`emergencyContact` AS emergency_contact
FROM ng_loan_market.`user` u
INNER JOIN ng_loan_market.user_data ud ON ud.`userId` = u.id
INNER JOIN (
    SELECT `userId`, MAX(id) AS max_id
    FROM ng_loan_market.user_data
    GROUP BY `userId`
) latest ON latest.`userId` = ud.`userId` AND latest.max_id = ud.id
WHERE ud.`emergencyContact` IS NOT NULL
  AND TRIM(ud.`emergencyContact`) <> ''
  AND TRIM(ud.`emergencyContact`) NOT IN ('[]', 'null', 'NULL')
ORDER BY u.id ASC
"""


def load_env(path: Path) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if not path.is_file():
        raise FileNotFoundError(f"缺少配置文件: {path}")
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        out[k.strip()] = v.strip().strip("'").strip('"')
    return out


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
        connect_timeout=60,
    )


def mobile_vt_lookup_candidates(raw: str) -> List[str]:
    s = str(raw).strip()
    if not s:
        return []
    cands = [s]
    if s.startswith("+234"):
        cands.append(s[4:])
    elif s.startswith("234"):
        cands.append("+" + s)
        cands.append(s[3:])
    elif s.startswith("0"):
        cands.append("+234" + s[1:])
        cands.append(s[1:])
    else:
        cands.append("+234" + s)
    seen: Set[str] = set()
    out: List[str] = []
    for c in cands:
        if c and c not in seen:
            seen.add(c)
            out.append(c)
    return out


def parse_emergency_contact(val: Any) -> Any:
    if val is None:
        return None
    if isinstance(val, (list, dict)):
        return val
    s = str(val).strip()
    if not s:
        return None
    if s[0] in "[{":
        try:
            return json.loads(s)
        except (json.JSONDecodeError, TypeError, ValueError):
            return s
    return s


def resolve_contact_token(store: Dict[Tuple[str, str], str], mobile_raw: str) -> Optional[str]:
    if not mobile_raw:
        return None
    for cand in mobile_vt_lookup_candidates(mobile_raw):
        for vt_type in VT_TYPES:
            token = store.get((vt_type, cand))
            if token:
                return token
    return None


def preload_vt_store(conn) -> Dict[Tuple[str, str], str]:
    sql = """
        SELECT vt_type, raw_value, token
        FROM ng_loan_market.vt_token_cache
        WHERE status = 1
          AND vt_type IN ('mobile', 'emergency_contact')
          AND token IS NOT NULL AND token <> ''
          AND raw_value IS NOT NULL AND raw_value <> ''
    """
    store: Dict[Tuple[str, str], str] = {}
    t0 = time.perf_counter()
    with conn.cursor() as cur:
        cur.execute(sql)
        while True:
            chunk = cur.fetchmany(50000)
            if not chunk:
                break
            for row in chunk:
                store[(row["vt_type"], row["raw_value"])] = row["token"]
    elapsed = time.perf_counter() - t0
    print(f"VT preload: {len(store)} entries, {elapsed:.1f}s", flush=True)
    return store


def iter_contacts(parsed: Any) -> Iterable[Tuple[int, Any, Any, str]]:
    if not isinstance(parsed, list):
        return
    for idx, item in enumerate(parsed):
        if isinstance(item, (list, tuple)) and len(item) >= 3:
            yield idx, item[0], item[1], str(item[2]).strip() if item[2] is not None else ""
        elif isinstance(item, dict):
            mobile = (
                item.get("mobile")
                or item.get("contactNumber")
                or item.get("contact_number")
            )
            name = item.get("name") or item.get("contactName") or item.get("contact_name")
            relation = (
                item.get("relation")
                if item.get("relation") is not None
                else item.get("contactRelationship")
                if item.get("contactRelationship") is not None
                else item.get("contact_relationship")
            )
            yield idx, name, relation, str(mobile).strip() if mobile is not None else ""


def main() -> int:
    import argparse
    parser = argparse.ArgumentParser(description="只读分析紧急联系人 VT 未命中")
    parser.add_argument("--max-miss-rows", type=int, default=0, help="最多写入 CSV 的未命中条数，0=不限制")
    parser.add_argument("--max-users", type=int, default=0, help="最多扫描用户数，0=不限制")
    args = parser.parse_args()
    max_miss_rows = max(0, args.max_miss_rows)
    max_users = max(0, args.max_users)

    cfg = load_env(ENV_FILE)
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    csv_path = REPORT_DIR / f"emergency_contact_vt_miss_plaintext_{ts}.csv"
    md_path = REPORT_DIR / f"emergency_contact_vt_miss_report_{ts}.md"
    phones_path = REPORT_DIR / f"emergency_contact_vt_miss_unique_phones_{ts}.txt"

    conn = connect_source(cfg)
    try:
        store = preload_vt_store(conn)

        stats = Counter()
        users_with_contacts: Set[int] = set()
        users_with_miss: Set[int] = set()
        unique_miss_phones: Set[str] = set()
        miss_by_app: Counter = Counter()

        t0 = time.perf_counter()
        with conn.cursor() as cur:
            cur.execute(USER_DATA_SQL)
            with csv_path.open("w", encoding="utf-8", newline="") as csv_file:
                writer = csv.writer(csv_file)
                writer.writerow([
                    "user_id", "app_id", "user_mobile",
                    "contact_index", "contact_name", "relation",
                    "mobile_plaintext", "normalized_candidates", "vt_token",
                ])
                row_num = 0
                while True:
                    batch = cur.fetchmany(2000)
                    if not batch:
                        break
                    for row in batch:
                        user_id = int(row["user_id"])
                        app_id = row.get("app_id")
                        user_mobile = row.get("user_mobile") or ""
                        parsed = parse_emergency_contact(row.get("emergency_contact"))
                        contacts = list(iter_contacts(parsed))
                        if not contacts:
                            stats["users_no_parsed_contacts"] += 1
                            continue
                        users_with_contacts.add(user_id)
                        user_has_miss = False
                        for idx, name, relation, mobile_plain in contacts:
                            stats["contact_entries_total"] += 1
                            cands = mobile_vt_lookup_candidates(mobile_plain) if mobile_plain else []
                            if not mobile_plain:
                                stats["contact_empty_source_mobile"] += 1
                                continue
                            token = resolve_contact_token(store, mobile_plain)
                            if token:
                                stats["contact_vt_hit"] += 1
                            else:
                                stats["contact_vt_miss"] += 1
                                user_has_miss = True
                                unique_miss_phones.add(mobile_plain)
                                miss_by_app[str(app_id)] += 1
                                writer.writerow([
                                    user_id,
                                    app_id,
                                    user_mobile,
                                    idx,
                                    name,
                                    relation,
                                    mobile_plain,
                                    "|".join(cands),
                                    "",
                                ])
                                if max_miss_rows and stats["contact_vt_miss"] >= max_miss_rows:
                                    break
                        if user_has_miss:
                            users_with_miss.add(user_id)
                        row_num += 1
                        if max_users and row_num >= max_users:
                            break
                        if max_miss_rows and stats["contact_vt_miss"] >= max_miss_rows:
                            break
                        if row_num % 50000 == 0:
                            elapsed = time.perf_counter() - t0
                            print(
                                f"scanned users={row_num} misses={stats['contact_vt_miss']} "
                                f"elapsed={elapsed:.0f}s",
                                flush=True,
                            )
                    if max_users and row_num >= max_users:
                        break
                    if max_miss_rows and stats["contact_vt_miss"] >= max_miss_rows:
                        break
    finally:
        conn.close()

    phones_path.write_text(
        "\n".join(sorted(unique_miss_phones)) + ("\n" if unique_miss_phones else ""),
        encoding="utf-8",
    )

    total_el = time.perf_counter() - t0
    hit_rate = (
        stats["contact_vt_hit"] / stats["contact_entries_total"] * 100
        if stats["contact_entries_total"]
        else 0.0
    )
    miss_rate = (
        stats["contact_vt_miss"] / stats["contact_entries_total"] * 100
        if stats["contact_entries_total"]
        else 0.0
    )

    md_lines = [
        "# 紧急联系人 VT 未命中统计报告",
        "",
        f"- 生成时间（UTC）: {datetime.now(timezone.utc).isoformat()}",
        f"- 数据源: `{cfg['SOURCE_HOST']}` / `ng_loan_market.user_data`（最新一条）",
        f"- VT 字典: `vt_token_cache`（`status=1`，`mobile` + `emergency_contact`）",
        f"- 分析逻辑: 与 `ng_migration_run.py` 迁移脚本一致（未命中 → 目标库 `mobile=null`）",
        f"- **只读分析，未修改源库/目标库**",
        "",
        "## 汇总",
        "",
        "| 指标 | 数量 |",
        "|------|------|",
        f"| 扫描用户数（有 emergencyContact） | {len(users_with_contacts):,} |",
        f"| 紧急联系人条目总数 | {stats['contact_entries_total']:,} |",
        f"| 源库手机号为空 | {stats['contact_empty_source_mobile']:,} |",
        f"| VT 命中（会写入 token） | {stats['contact_vt_hit']:,} ({hit_rate:.2f}%) |",
        f"| **VT 未命中（迁移写 null）** | **{stats['contact_vt_miss']:,} ({miss_rate:.2f}%)** |",
        f"| 未命中去重明文手机号 | {len(unique_miss_phones):,} |",
        f"| 至少 1 条未命中的用户数 | {len(users_with_miss):,} |",
        f"| 无法解析联系人数组的用户 | {stats['users_no_parsed_contacts']:,} |",
        f"| 扫描耗时 | {total_el:.1f}s |",
        "",
        "## 按 app_id 未命中条目数（Top 20）",
        "",
        "| app_id | 未命中条数 |",
        "|--------|------------|",
    ]
    for app_id, cnt in miss_by_app.most_common(20):
        md_lines.append(f"| {app_id} | {cnt:,} |")

    md_lines.extend([
        "",
        "## 输出文件",
        "",
        f"- 明细 CSV: `{csv_path.name}`",
        f"- 去重明文: `{phones_path.name}`",
        "",
        "## 说明",
        "",
        "未命中原因：`vt_token_cache` 中不存在该手机号（含 +234 变体）的 `mobile` 或 `emergency_contact` token。",
        "补全方式：对 CSV/txt 中明文跑 VT `/v2t` 写入字典后，再补灌对应 `user_info`（勿在迁移进行中改脚本逻辑）。",
        "",
        "## 样例（未命中前 5 条，见 CSV 全量）",
        "",
    ])

    sample_rows: List[List[str]] = []
    if csv_path.is_file():
        with csv_path.open(encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for i, r in enumerate(reader):
                if i >= 5:
                    break
                sample_rows.append(r)
    if sample_rows:
        md_lines.append("| user_id | name | relation | mobile_plaintext |")
        md_lines.append("|---------|------|----------|------------------|")
        for r in sample_rows:
            md_lines.append(
                f"| {r['user_id']} | {r['contact_name']} | {r['relation']} | {r['mobile_plaintext']} |"
            )
    else:
        md_lines.append("_无未命中记录_")

    md_path.write_text("\n".join(md_lines) + "\n", encoding="utf-8")

    print(f"Done. report={md_path}", flush=True)
    print(f"csv={csv_path} rows={stats['contact_vt_miss']}", flush=True)
    print(f"unique_phones={phones_path} count={len(unique_miss_phones)}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
