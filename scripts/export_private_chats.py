import csv
import hashlib
import json
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
from wechat_cli.core.messages import _load_name2id_maps, _split_msg_type, format_msg_type

OUT = ROOT / "analysis" / "private_chats"

TYPE_LABELS = {
    1: "文本",
    3: "图片",
    34: "语音",
    42: "名片",
    43: "视频",
    47: "表情",
    48: "位置",
    49: "链接/文件",
    50: "通话",
    10000: "系统",
    10002: "撤回",
    11000: "拍一拍/互动",
}

PRIVATE_EXCLUDE_PREFIXES = ("gh_",)
PRIVATE_EXCLUDE_NAMES = {
    "notifymessage",
    "floatbottle",
    "medianote",
    "newsapp",
    "weixin",
    "filehelper",
}


def extra_buffer_gender(extra_buffer):
    """Parse WeChat contact extra_buffer field 2.

    Empirically, protobuf field 2 stores sex: 1=male, 2=female, 0=unknown.
    """
    if not extra_buffer or len(extra_buffer) < 2 or extra_buffer[0] != 0x10:
        return "unknown", "missing"
    value = 0
    shift = 0
    for b in extra_buffer[1:]:
        value |= (b & 0x7F) << shift
        if not (b & 0x80):
            if value == 1:
                return "男", "extra_buffer"
            if value == 2:
                return "女", "extra_buffer"
            return "unknown", "extra_buffer"
        shift += 7
    return "unknown", "missing"


def load_contact_profiles(app, names):
    path = app.cache.get("contact/contact.db")
    profiles = {}
    if not path:
        return profiles
    with closing(sqlite3.connect(path)) as conn:
        for username, nick, remark, extra_buffer in conn.execute(
            "SELECT username, nick_name, remark, extra_buffer FROM contact"
        ).fetchall():
            display = remark or nick or names.get(username, username)
            gender, source = extra_buffer_gender(extra_buffer)
            profiles[username] = {
                "display_name": display,
                "gender": gender,
                "gender_source": source,
            }
    return profiles


def msg_type_label(local_type):
    base_type, _ = _split_msg_type(local_type)
    return TYPE_LABELS.get(base_type, format_msg_type(base_type))


def table_hash(username):
    return f"Msg_{hashlib.md5(username.encode()).hexdigest()}"


def is_private_username(username, self_username):
    if not username:
        return False
    if username == self_username:
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
            for username in usernames:
                if not is_private_username(username, self_username):
                    continue
                tbl = table_hash(username)
                if tbl not in existing_tables:
                    continue
                key = (db_path, tbl, username)
                if key in seen:
                    continue
                seen.add(key)
                contexts.append({"db_path": db_path, "table_name": tbl, "username": username})
    return contexts


def iter_private_messages(app, names, self_username):
    for ctx in discover_message_tables(app, self_username):
        chat_username = ctx["username"]
        chat_name = names.get(chat_username, chat_username)
        with closing(sqlite3.connect(ctx["db_path"])) as conn:
            id_to_username = _load_name2id_maps(conn)
            rows = conn.execute(
                f"""
                SELECT local_id, local_type, create_time, real_sender_id
                FROM [{ctx['table_name']}]
                ORDER BY create_time ASC
                """
            )
            for local_id, local_type, create_time, real_sender_id in rows:
                sender_username = (id_to_username.get(real_sender_id, "") or "").strip()
                if sender_username == self_username:
                    sender_role = "me"
                elif sender_username == chat_username:
                    sender_role = "contact"
                else:
                    sender_role = "unknown"
                base_type, sub_type = _split_msg_type(local_type)
                dt = datetime.fromtimestamp(create_time)
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
                    "base_type": base_type,
                    "sub_type": sub_type,
                    "type": msg_type_label(local_type),
                }


def write_gender_template(rows, path, profiles):
    existing = {}
    if path.exists():
        with path.open("r", encoding="utf-8-sig", newline="") as f:
            for row in csv.DictReader(f):
                existing[row["username"]] = row
    users = {}
    for row in rows:
        users[row["chat_username"]] = row["chat_name"]
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["username", "display_name", "gender", "gender_source"])
        writer.writeheader()
        for username, display_name in sorted(users.items(), key=lambda item: item[1].lower()):
            inferred = profiles.get(username, {})
            old = existing.get(username, {})
            old_gender = (old.get("gender") or "").strip()
            if old_gender and old_gender.lower() not in {"unknown", "unk", "未知"}:
                gender = old_gender
                source = old.get("gender_source") or "manual"
            else:
                gender = inferred.get("gender", "unknown")
                source = inferred.get("gender_source", "missing")
            writer.writerow({
                "username": username,
                "display_name": display_name,
                "gender": gender,
                "gender_source": source,
            })


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    app = AppContext()
    names = get_contact_names(app.cache, app.decrypted_dir)
    self_username = get_self_username(app.db_dir, app.cache, app.decrypted_dir)
    profiles = load_contact_profiles(app, names)
    rows = sorted(
        list(iter_private_messages(app, names, self_username)),
        key=lambda row: (row["datetime"], row["chat_username"], row["local_id"]),
    )

    msg_path = OUT / "private_messages.csv"
    fields = [
        "chat_username", "chat_name", "local_id", "datetime", "date", "month",
        "weekday", "hour", "sender_username", "sender_role", "base_type",
        "sub_type", "type",
    ]
    with msg_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)

    gender_path = OUT / "gender_labels.csv"
    write_gender_template(rows, gender_path, profiles)

    meta = {
        "self_username": self_username,
        "message_count": len(rows),
        "private_contact_count": len({row["chat_username"] for row in rows}),
        "first_time": rows[0]["datetime"] if rows else None,
        "last_time": rows[-1]["datetime"] if rows else None,
        "messages_csv": str(msg_path),
        "gender_template_csv": str(gender_path),
        "gender_note": "Gender is parsed from contact.extra_buffer protobuf field 2 when available: 1=男, 2=女, 0=unknown. Manual edits in gender_labels.csv are preserved.",
    }
    meta_path = OUT / "metadata.json"
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(meta, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
