import csv
import json
import sqlite3
import sys
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from wechat_cli.core.context import AppContext

OUT = ROOT / "analysis" / "sns_inventory"


def text_of(root, path, default=""):
    node = root.find(path)
    if node is None or node.text is None:
        return default
    return node.text


def unix_time(value):
    try:
        ts = int(value)
    except (TypeError, ValueError):
        return ""
    if ts <= 0:
        return ""
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")


def parse_timeline_content(raw):
    item = {
        "timeline_id": "",
        "create_time": "",
        "content_desc_len": 0,
        "private": "",
        "media_count": 0,
        "media_types": "",
        "like_count": 0,
        "comment_count": 0,
        "xml_ok": False,
    }
    if not raw:
        return item
    try:
        root = ET.fromstring(raw)
    except ET.ParseError:
        return item

    item["xml_ok"] = True
    item["timeline_id"] = text_of(root, ".//TimelineObject/id")
    item["create_time"] = unix_time(text_of(root, ".//TimelineObject/createTime"))
    item["content_desc_len"] = len(text_of(root, ".//TimelineObject/contentDesc"))
    item["private"] = text_of(root, ".//TimelineObject/private")

    media_types = []
    for media in root.findall(".//mediaList/media"):
        media_types.append(text_of(media, "type"))
    item["media_count"] = len(media_types)
    item["media_types"] = ",".join(sorted(set(t for t in media_types if t)))

    item["like_count"] = len(root.findall(".//like_user_list/user_comment"))
    item["comment_count"] = len(root.findall(".//comment_user_list/user_comment"))
    return item


def load_contact_names(app):
    path = app.cache.get("contact/contact.db")
    names = {}
    if not path:
        return names
    with sqlite3.connect(path) as conn:
        for username, nick, remark in conn.execute(
            "SELECT username, nick_name, remark FROM contact"
        ):
            names[username] = remark or nick or username
    return names


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    app = AppContext()
    sns_path = app.cache.get("sns\\sns.db")
    if not sns_path:
        raise SystemExit("sns\\sns.db was not found in configured WeChat keys.")

    names = load_contact_names(app)
    with sqlite3.connect(sns_path) as conn:
        conn.row_factory = sqlite3.Row
        tables = [
            row["name"]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
            )
        ]

        inventory_rows = []
        for table in tables:
            count = conn.execute(f"SELECT COUNT(*) FROM [{table}]").fetchone()[0]
            cols = [
                row["name"]
                for row in conn.execute(f"PRAGMA table_info([{table}])").fetchall()
            ]
            inventory_rows.append({
                "table": table,
                "rows": count,
                "columns": ",".join(cols),
            })

        with (OUT / "table_inventory.csv").open("w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["table", "rows", "columns"])
            writer.writeheader()
            writer.writerows(inventory_rows)

        timeline_rows = []
        for row in conn.execute("SELECT tid, user_name, content FROM SnsTimeLine"):
            parsed = parse_timeline_content(row["content"])
            user_name = row["user_name"]
            timeline_rows.append({
                "tid": row["tid"],
                "user_name": user_name,
                "display_name": names.get(user_name, user_name),
                **parsed,
            })

        timeline_rows.sort(key=lambda r: r["create_time"])
        with (OUT / "timeline_summary.csv").open("w", encoding="utf-8-sig", newline="") as f:
            fields = [
                "tid", "timeline_id", "user_name", "display_name", "create_time",
                "content_desc_len", "private", "media_count", "media_types",
                "like_count", "comment_count", "xml_ok",
            ]
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            writer.writerows(timeline_rows)

        msg_rows = []
        for row in conn.execute(
            """
            SELECT local_id, create_time, type, feed_id, from_username,
                   from_nickname, to_username, to_nickname, length(content) AS content_len,
                   is_relative_me
            FROM SnsMessage_tmp3
            ORDER BY create_time
            """
        ):
            msg_rows.append({
                "local_id": row["local_id"],
                "create_time": unix_time(row["create_time"]),
                "type": row["type"],
                "feed_id": row["feed_id"],
                "from_username": row["from_username"],
                "from_nickname": row["from_nickname"],
                "to_username": row["to_username"],
                "to_nickname": row["to_nickname"],
                "content_len": row["content_len"],
                "is_relative_me": row["is_relative_me"],
            })

        with (OUT / "interaction_summary.csv").open("w", encoding="utf-8-sig", newline="") as f:
            fields = [
                "local_id", "create_time", "type", "feed_id", "from_username",
                "from_nickname", "to_username", "to_nickname", "content_len",
                "is_relative_me",
            ]
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            writer.writerows(msg_rows)

    times = [r["create_time"] for r in timeline_rows if r["create_time"]]
    summary = {
        "sns_db": sns_path,
        "output_dir": str(OUT),
        "table_count": len(inventory_rows),
        "timeline_rows": len(timeline_rows),
        "timeline_first_time": min(times) if times else None,
        "timeline_last_time": max(times) if times else None,
        "interaction_rows": len(msg_rows),
        "files": [
            str(OUT / "table_inventory.csv"),
            str(OUT / "timeline_summary.csv"),
            str(OUT / "interaction_summary.csv"),
        ],
        "privacy_note": "timeline_summary.csv only stores metadata and content length, not full Moments text.",
    }
    (OUT / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
