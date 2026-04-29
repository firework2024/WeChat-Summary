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
OUT = BASE / "laughter_scan"

PRIVATE_EXCLUDE_PREFIXES = ("gh_",)
PRIVATE_EXCLUDE_NAMES = {
    "notifymessage",
    "floatbottle",
    "medianote",
    "newsapp",
    "weixin",
    "filehelper",
}

H_PATTERN = re.compile(r"(?<![A-Za-z])h{2,}(?![A-Za-z])", re.I)
HA_PATTERN = re.compile(r"哈{2,}")


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


def length_bucket(kind, length):
    if kind == "h":
        if length >= 8:
            return "h8+"
        return f"h{length}"
    if length >= 8:
        return "哈8+"
    return "哈" * length


def find_laughter(text):
    hits = []
    for match in H_PATTERN.finditer(text):
        length = len(match.group(0))
        hits.append({
            "kind": "h",
            "length": length,
            "bucket": length_bucket("h", length),
            "raw": match.group(0),
        })
    for match in HA_PATTERN.finditer(text):
        length = len(match.group(0))
        hits.append({
            "kind": "哈",
            "length": length,
            "bucket": length_bucket("哈", length),
            "raw": match.group(0),
        })
    return hits


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
                hits = find_laughter(text)
                if not hits:
                    continue
                sender_username = (id_to_username.get(real_sender_id, "") or "").strip()
                if sender_username == self_username:
                    sender_role = "me"
                elif sender_username == chat_username:
                    sender_role = "contact"
                else:
                    sender_role = "unknown"
                dt = datetime.fromtimestamp(create_time)
                for hit in hits:
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
                        "kind": hit["kind"],
                        "length": hit["length"],
                        "bucket": hit["bucket"],
                        "raw": hit["raw"],
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
        key=lambda row: (row["datetime"], row["chat_username"], row["local_id"], row["kind"]),
    )

    hits_path = OUT / "laughter_hits.csv"
    fields = [
        "chat_username", "chat_name", "local_id", "datetime", "date", "month",
        "weekday", "hour", "sender_username", "sender_role", "kind", "length",
        "bucket", "raw", "text_len", "preview",
    ]
    with hits_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)

    summary = {
        "patterns": ["h{2,}", "哈{2,}"],
        "hit_rows": len(rows),
        "private_contacts_with_hits": len({row["chat_username"] for row in rows}),
        "first_time": rows[0]["datetime"] if rows else None,
        "last_time": rows[-1]["datetime"] if rows else None,
        "hits_csv": str(hits_path),
    }
    (OUT / "metadata.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
