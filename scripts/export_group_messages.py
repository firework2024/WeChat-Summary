import argparse
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

from wechat_cli.core.contacts import get_contact_names
from wechat_cli.core.context import AppContext
from wechat_cli.core.messages import (
    _iter_table_contexts,
    _load_name2id_maps,
    _parse_message_content,
    _split_msg_type,
    decompress_content,
    format_msg_type,
    resolve_chat_context,
)

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


def msg_type_label(local_type):
    base_type, _ = _split_msg_type(local_type)
    return TYPE_LABELS.get(base_type, format_msg_type(base_type))


def clean_text(content, is_group):
    sender_hint, text = _parse_message_content(content or "", 0, is_group)
    if not text:
        return sender_hint, ""
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return sender_hint, text


def iter_messages(ctx, names, app):
    for table_ctx in _iter_table_contexts(ctx):
        with closing(sqlite3.connect(table_ctx["db_path"])) as conn:
            id_to_username = _load_name2id_maps(conn)
            table = table_ctx["table_name"]
            rows = conn.execute(
                f"""
                SELECT local_id, local_type, create_time, real_sender_id,
                       message_content, WCDB_CT_message_content
                FROM [{table}]
                ORDER BY create_time ASC
                """
            )
            for local_id, local_type, create_time, real_sender_id, content, ct in rows:
                content = decompress_content(content, ct)
                if content is None:
                    content = ""
                base_type, sub_type = _split_msg_type(local_type)
                sender_hint, text = clean_text(content, ctx["is_group"])
                sender_username = (id_to_username.get(real_sender_id, "") or sender_hint or "").strip()
                sender_name = app.display_name_fn(sender_username, names) if sender_username else ""
                dt = datetime.fromtimestamp(create_time)
                yield {
                    "local_id": local_id,
                    "datetime": dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "date": dt.strftime("%Y-%m-%d"),
                    "month": dt.strftime("%Y-%m"),
                    "weekday": dt.weekday(),
                    "hour": dt.hour,
                    "sender_username": sender_username,
                    "sender_name": sender_name,
                    "base_type": base_type,
                    "sub_type": sub_type,
                    "type": msg_type_label(local_type),
                    "text_len": len(text),
                    "text_hash": hashlib.sha256(text.encode("utf-8")).hexdigest() if text else "",
                    "text": text,
                }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("chat_name")
    parser.add_argument("--out-dir", default="analysis/group_chat")
    args = parser.parse_args()

    out_dir = ROOT / args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    app = AppContext()
    names = get_contact_names(app.cache, app.decrypted_dir)
    ctx = resolve_chat_context(args.chat_name, app.msg_db_keys, app.cache, app.decrypted_dir)
    if not ctx:
        raise SystemExit(f"Chat not found: {args.chat_name}")
    if not ctx["is_group"]:
        raise SystemExit(f"Not a group chat: {ctx['display_name']} ({ctx['username']})")
    if not ctx["message_tables"]:
        raise SystemExit(f"No message tables found: {ctx['display_name']}")

    rows = sorted(list(iter_messages(ctx, names, app)), key=lambda row: row["datetime"])
    csv_path = out_dir / "messages.csv"
    json_path = out_dir / "metadata.json"
    fields = [
        "local_id", "datetime", "date", "month", "weekday", "hour",
        "sender_username", "sender_name", "base_type", "sub_type", "type",
        "text_len", "text_hash", "text",
    ]
    with csv_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)

    metadata = {
        "chat": ctx["display_name"],
        "username": ctx["username"],
        "message_count": len(rows),
        "first_time": rows[0]["datetime"] if rows else None,
        "last_time": rows[-1]["datetime"] if rows else None,
        "export_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "csv": str(csv_path),
    }
    json_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(metadata, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
