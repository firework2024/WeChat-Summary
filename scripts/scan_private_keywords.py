import csv
import hashlib
import json
import re
import sqlite3
import sys
from contextlib import closing
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from wechat_cli.core.contacts import get_contact_names, get_self_username
from wechat_cli.core.context import AppContext
from wechat_cli.core.messages import _load_name2id_maps, decompress_content

BASE = ROOT / "analysis" / "private_chats"
OUT = BASE / "keyword_scan"

KEYWORDS = ["笑死我了", "笑死了", "xswl", "笑死", "xs", "哎"]

PRIVATE_EXCLUDE_PREFIXES = ("gh_",)
PRIVATE_EXCLUDE_NAMES = {
    "notifymessage",
    "floatbottle",
    "medianote",
    "newsapp",
    "weixin",
    "filehelper",
}


def table_hash(username):
    return f"Msg_{hashlib.md5(username.encode()).hexdigest()}"


def is_private_username(username, self_username):
    if not username or username == self_username:
        return False
    if username in PRIVATE_EXCLUDE_NAMES:
        return False
    if username.endswith("@chatroom") or username.startswith(PRIVATE_EXCLUDE_PREFIXES):
        return False
    return True


def discover_message_tables(app, self_username):
    contexts = []
    seen = set()
    for rel_key in app.msg_db_keys:
        db_path = app.cache.get(rel_key)
        if not db_path:
            continue
        with closing(sqlite3.connect(db_path)) as conn:
            try:
                usernames = [
                    row[0]
                    for row in conn.execute("SELECT user_name FROM Name2Id").fetchall()
                    if row[0]
                ]
                existing_tables = {
                    row[0]
                    for row in conn.execute(
                        "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'Msg_%'"
                    ).fetchall()
                }
            except sqlite3.Error:
                continue
            for username in usernames:
                if not is_private_username(username, self_username):
                    continue
                table = table_hash(username)
                if table not in existing_tables:
                    continue
                key = (db_path, table, username)
                if key in seen:
                    continue
                seen.add(key)
                contexts.append({"db_path": db_path, "table_name": table, "username": username})
    return contexts


def count_keywords(text):
    text = text or ""
    counts = {}
    # Classify nested laugh phrases by the most specific phrase first, so
    # "笑死我了" is not also counted as "笑死".
    occupied = [False] * len(text)
    for keyword in ["笑死我了", "笑死了", "笑死"]:
        total = 0
        start = 0
        while True:
            idx = text.find(keyword, start)
            if idx < 0:
                break
            end = idx + len(keyword)
            if not any(occupied[idx:end]):
                total += 1
                for i in range(idx, end):
                    occupied[i] = True
            start = idx + 1
        if total:
            counts[keyword] = total

    for keyword in ["xswl", "xs"]:
        pattern = re.compile(rf"(?<![A-Za-z0-9_]){re.escape(keyword)}(?![A-Za-z0-9_])", re.I)
        hits = len(pattern.findall(text))
        if hits:
            counts[keyword] = hits

    ai_hits = text.count("哎")
    if ai_hits:
        counts["哎"] = ai_hits
    return {k: v for k, v in counts.items() if v > 0}


def text_preview(text, max_len=80):
    text = re.sub(r"\s+", " ", text or "").strip()
    if len(text) <= max_len:
        return text
    return text[:max_len] + "..."


def iter_hits(app, names, self_username):
    for ctx in discover_message_tables(app, self_username):
        chat_username = ctx["username"]
        chat_name = names.get(chat_username, chat_username)
        with closing(sqlite3.connect(ctx["db_path"])) as conn:
            id_to_username = _load_name2id_maps(conn)
            rows = conn.execute(
                f"""
                SELECT local_id, local_type, create_time, real_sender_id,
                       message_content, WCDB_CT_message_content
                FROM [{ctx['table_name']}]
                WHERE (local_type & 0xFFFFFFFF) = 1
                ORDER BY create_time ASC
                """
            )
            for local_id, local_type, create_time, real_sender_id, content, ct in rows:
                text = decompress_content(content, ct)
                if not isinstance(text, str):
                    continue
                counts = count_keywords(text)
                if not counts:
                    continue
                sender_username = (id_to_username.get(real_sender_id, "") or "").strip()
                if sender_username == self_username:
                    sender_role = "me"
                elif sender_username == chat_username:
                    sender_role = "contact"
                else:
                    sender_role = "unknown"
                dt = datetime.fromtimestamp(create_time)
                for keyword, count in counts.items():
                    yield {
                        "chat_username": chat_username,
                        "chat_name": chat_name,
                        "local_id": local_id,
                        "datetime": dt.strftime("%Y-%m-%d %H:%M:%S"),
                        "date": dt.strftime("%Y-%m-%d"),
                        "month": dt.strftime("%Y-%m"),
                        "weekday": dt.weekday(),
                        "hour": dt.hour,
                        "sender_username": sender_username,
                        "sender_role": sender_role,
                        "keyword": keyword,
                        "count": count,
                        "text_len": len(text),
                        "preview": text_preview(text),
                    }


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    app = AppContext()
    names = get_contact_names(app.cache, app.decrypted_dir)
    self_username = get_self_username(app.db_dir, app.cache, app.decrypted_dir)
    rows = sorted(
        list(iter_hits(app, names, self_username)),
        key=lambda row: (row["datetime"], row["chat_username"], row["local_id"], row["keyword"]),
    )

    hits_path = OUT / "keyword_hits.csv"
    fields = [
        "chat_username", "chat_name", "local_id", "datetime", "date", "month",
        "weekday", "hour", "sender_username", "sender_role", "keyword", "count",
        "text_len", "preview",
    ]
    with hits_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)

    summary = {
        "keywords": KEYWORDS,
        "hit_rows": len(rows),
        "total_occurrences": sum(row["count"] for row in rows),
        "private_contacts_with_hits": len({row["chat_username"] for row in rows}),
        "first_time": rows[0]["datetime"] if rows else None,
        "last_time": rows[-1]["datetime"] if rows else None,
        "hits_csv": str(hits_path),
    }
    (OUT / "metadata.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
