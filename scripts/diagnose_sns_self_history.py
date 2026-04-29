import csv
import json
import sqlite3
import sys
import xml.etree.ElementTree as ET
from collections import Counter
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from wechat_cli.core.contacts import get_self_username
from wechat_cli.core.context import AppContext

OUT = ROOT / "analysis" / "sns_self_history_diagnostics"


def unix_time(value):
    try:
        ts = int(value)
    except (TypeError, ValueError):
        return ""
    if ts <= 0:
        return ""
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")


def write_csv(path, rows, fields):
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def scan_sns_cache_dirs(account_dir):
    rows = []
    cache_dir = account_dir / "cache"
    if not cache_dir.exists():
        return rows
    for sns_dir in sorted(cache_dir.glob("*/Sns")):
        files = [p for p in sns_dir.rglob("*") if p.is_file()]
        total_size = sum(p.stat().st_size for p in files)
        non_empty = [p for p in files if p.stat().st_size > 0]
        rows.append({
            "month_dir": sns_dir.parent.name,
            "path": str(sns_dir),
            "file_count": len(files),
            "non_empty_file_count": len(non_empty),
            "total_size_bytes": total_size,
        })
    return rows


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    app = AppContext()
    self_username = get_self_username(app.db_dir, app.cache, app.decrypted_dir)
    account_dir = Path(app.db_dir).parents[0]
    sns_path = app.cache.get("sns\\sns.db")
    if not sns_path:
        raise SystemExit("sns\\sns.db was not found.")

    table_rows = []
    self_mentions = []
    own_posts = []
    content_self_mentions = 0
    xml_author_self = 0
    table_user_self = 0
    years = Counter()

    with sqlite3.connect(sns_path) as conn:
        conn.row_factory = sqlite3.Row
        tables = [
            row["name"]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        ]
        for table in tables:
            count = conn.execute(f"SELECT COUNT(*) FROM [{table}]").fetchone()[0]
            cols = [
                row["name"]
                for row in conn.execute(f"PRAGMA table_info([{table}])").fetchall()
            ]
            hit_cols = []
            for col in cols:
                if any(key in col.lower() for key in ["user", "name", "content", "summary", "buf"]):
                    try:
                        hits = conn.execute(
                            f"SELECT COUNT(*) FROM [{table}] WHERE CAST([{col}] AS TEXT) LIKE ?",
                            (f"%{self_username}%",),
                        ).fetchone()[0]
                    except sqlite3.Error:
                        hits = 0
                    if hits:
                        hit_cols.append(f"{col}:{hits}")
            table_rows.append({
                "table": table,
                "rows": count,
                "self_hits_by_column": ";".join(hit_cols),
                "columns": ",".join(cols),
            })

        for row in conn.execute("SELECT tid, user_name, content FROM SnsTimeLine"):
            raw = row["content"] or ""
            try:
                root = ET.fromstring(raw)
            except ET.ParseError:
                continue
            xml_user = root.findtext(".//TimelineObject/username") or ""
            create_time = unix_time(root.findtext(".//TimelineObject/createTime"))
            desc = root.findtext(".//TimelineObject/contentDesc") or ""
            if create_time:
                years[create_time[:4]] += 1
            if self_username in raw:
                content_self_mentions += 1
            if xml_user == self_username:
                xml_author_self += 1
            if row["user_name"] == self_username:
                table_user_self += 1
            if xml_user == self_username or row["user_name"] == self_username:
                own_posts.append({
                    "tid": row["tid"],
                    "table_user_name": row["user_name"],
                    "xml_username": xml_user,
                    "create_time": create_time,
                    "content_len": len(desc),
                    "preview": desc.replace("\n", " ").replace("\r", " ")[:80],
                })
            elif self_username in raw:
                self_mentions.append({
                    "tid": row["tid"],
                    "table_user_name": row["user_name"],
                    "xml_username": xml_user,
                    "create_time": create_time,
                    "content_len": len(desc),
                    "preview": desc.replace("\n", " ").replace("\r", " ")[:80],
                })

    own_posts.sort(key=lambda r: r["create_time"])
    self_mentions.sort(key=lambda r: r["create_time"])
    cache_rows = scan_sns_cache_dirs(account_dir)

    write_csv(
        OUT / "sns_tables_self_hits.csv",
        table_rows,
        ["table", "rows", "self_hits_by_column", "columns"],
    )
    write_csv(
        OUT / "own_posts_found.csv",
        own_posts,
        ["tid", "table_user_name", "xml_username", "create_time", "content_len", "preview"],
    )
    write_csv(
        OUT / "timeline_rows_mentioning_self_but_not_authored_by_self.csv",
        self_mentions,
        ["tid", "table_user_name", "xml_username", "create_time", "content_len", "preview"],
    )
    write_csv(
        OUT / "sns_cache_dirs.csv",
        cache_rows,
        ["month_dir", "path", "file_count", "non_empty_file_count", "total_size_bytes"],
    )
    write_csv(
        OUT / "timeline_year_counts.csv",
        [{"year": year, "timeline_rows": count} for year, count in sorted(years.items())],
        ["year", "timeline_rows"],
    )

    summary = {
        "self_username": self_username,
        "sns_db": sns_path,
        "account_dir": str(account_dir),
        "timeline_rows": sum(years.values()),
        "timeline_year_counts": dict(sorted(years.items())),
        "own_posts_by_table_user_name": table_user_self,
        "own_posts_by_xml_author": xml_author_self,
        "timeline_rows_containing_self_username": content_self_mentions,
        "non_own_rows_containing_self_username": len(self_mentions),
        "own_first_time": own_posts[0]["create_time"] if own_posts else None,
        "own_last_time": own_posts[-1]["create_time"] if own_posts else None,
        "sns_cache_month_dirs": len(cache_rows),
        "sns_cache_non_empty_month_dirs": sum(1 for row in cache_rows if row["non_empty_file_count"] > 0),
        "output_dir": str(OUT),
        "diagnosis": (
            "Structured own Moments found in sns.db are limited to the rows listed in own_posts_found.csv. "
            "Earlier Moments are likely not cached locally in this Windows WeChat data directory unless another device/account backup contains older sns.db data."
        ),
    }
    (OUT / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    report = [
        "# 朋友圈本人历史数据遗漏诊断",
        "",
        f"- 本人 wxid: `{self_username}`",
        f"- 结构化朋友圈库: `{sns_path}`",
        f"- `SnsTimeLine` 总记录: {summary['timeline_rows']:,}",
        f"- XML 作者为本人的朋友圈: {xml_author_self:,}",
        f"- 表字段 `user_name` 为本人的朋友圈: {table_user_self:,}",
        f"- 本人朋友圈时间范围: {summary['own_first_time']} 至 {summary['own_last_time']}",
        f"- `content` 中包含本人 wxid 但作者不是本人的记录: {len(self_mentions):,}",
        "",
        "## 判断",
        "",
        "当前本机 `sns.db` 里可以结构化解析出的本人朋友圈只有这些记录。更早朋友圈没有在 `SnsTimeLine`、`SnsTopItem_1`、`SnsMessage_tmp3` 或 BreakFlag 表里以本人动态主体形式出现。",
        "",
        "按月份的 `cache/*/Sns` 目录存在，但它们更像媒体缓存；没有发现额外的结构化朋友圈历史数据库。",
        "",
        "## 输出文件",
        "",
        "- `own_posts_found.csv`",
        "- `timeline_rows_mentioning_self_but_not_authored_by_self.csv`",
        "- `sns_tables_self_hits.csv`",
        "- `sns_cache_dirs.csv`",
        "- `timeline_year_counts.csv`",
    ]
    (OUT / "report.md").write_text("\n".join(report), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
