import csv
import json
import sys
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from wechat_cli.core.contacts import get_contact_names, get_self_username
from wechat_cli.core.context import AppContext

OUT = ROOT / "analysis" / "sns_self_likes"


def node_text(node, path, default=""):
    found = node.find(path)
    if found is None or found.text is None:
        return default
    return found.text


def unix_time(value):
    try:
        ts = int(value)
    except (TypeError, ValueError):
        return "", ""
    if ts <= 0:
        return "", ""
    dt = datetime.fromtimestamp(ts)
    return dt.strftime("%Y-%m-%d %H:%M:%S"), dt.strftime("%Y-%m")


def display_name(username, nickname, names):
    return names.get(username) or nickname or username


def parse_post(row, names):
    raw = row["content"] or ""
    try:
        root = ET.fromstring(raw)
    except ET.ParseError:
        return None

    timeline = root.find(".//TimelineObject")
    if timeline is None:
        return None

    create_time, month = unix_time(node_text(timeline, "createTime"))
    desc = node_text(timeline, "contentDesc")

    likes = []
    like_list = root.find(".//like_user_list")
    if like_list is not None:
        for item in like_list.findall("user_comment"):
            deleted = node_text(item, "b_deleted", "0")
            if deleted == "1":
                continue
            username = node_text(item, "username")
            nickname = node_text(item, "nickname")
            like_time, like_month = unix_time(node_text(item, "create_time"))
            likes.append({
                "post_tid": row["tid"],
                "post_id": node_text(timeline, "id"),
                "post_time": create_time,
                "post_month": month,
                "post_preview": desc.replace("\n", " ").replace("\r", " ")[:80],
                "liker_username": username,
                "liker_nickname": nickname,
                "liker_display_name": display_name(username, nickname, names),
                "like_time": like_time,
                "like_month": like_month,
            })

    comments = []
    comment_list = root.find(".//comment_user_list")
    if comment_list is not None:
        for item in comment_list.findall("user_comment"):
            deleted = node_text(item, "b_deleted", "0")
            if deleted == "1":
                continue
            username = node_text(item, "username")
            nickname = node_text(item, "nickname")
            comment_time, comment_month = unix_time(node_text(item, "create_time"))
            comments.append({
                "post_tid": row["tid"],
                "post_id": node_text(timeline, "id"),
                "post_time": create_time,
                "post_month": month,
                "post_preview": desc.replace("\n", " ").replace("\r", " ")[:80],
                "commenter_username": username,
                "commenter_nickname": nickname,
                "commenter_display_name": display_name(username, nickname, names),
                "comment_time": comment_time,
                "comment_month": comment_month,
                "comment_len": len(node_text(item, "content")),
            })

    media_types = [
        node_text(media, "type")
        for media in timeline.findall(".//mediaList/media")
        if node_text(media, "type")
    ]
    post = {
        "post_tid": row["tid"],
        "post_id": node_text(timeline, "id"),
        "post_time": create_time,
        "post_month": month,
        "content_len": len(desc),
        "post_preview": desc.replace("\n", " ").replace("\r", " ")[:80],
        "private": node_text(timeline, "private"),
        "media_count": len(media_types),
        "media_types": ",".join(sorted(set(media_types))),
        "like_count": len(likes),
        "comment_count": len(comments),
    }
    return post, likes, comments


def write_csv(path, rows, fields):
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    app = AppContext()
    sns_path = app.cache.get("sns\\sns.db")
    if not sns_path:
        raise SystemExit("sns\\sns.db was not found.")

    import sqlite3

    names = get_contact_names(app.cache, app.decrypted_dir)
    self_username = get_self_username(app.db_dir, app.cache, app.decrypted_dir)

    posts = []
    likes = []
    comments = []
    with sqlite3.connect(sns_path) as conn:
        conn.row_factory = sqlite3.Row
        for row in conn.execute(
            "SELECT tid, user_name, content FROM SnsTimeLine WHERE user_name = ?",
            (self_username,),
        ):
            parsed = parse_post(row, names)
            if parsed is None:
                continue
            post, post_likes, post_comments = parsed
            posts.append(post)
            likes.extend(post_likes)
            comments.extend(post_comments)

    posts.sort(key=lambda r: r["post_time"])
    likes.sort(key=lambda r: (r["post_time"], r["liker_display_name"]))
    comments.sort(key=lambda r: (r["post_time"], r["commenter_display_name"]))

    write_csv(
        OUT / "self_posts.csv",
        posts,
        [
            "post_tid", "post_id", "post_time", "post_month", "content_len",
            "post_preview", "private", "media_count", "media_types",
            "like_count", "comment_count",
        ],
    )
    write_csv(
        OUT / "self_post_likes.csv",
        likes,
        [
            "post_tid", "post_id", "post_time", "post_month", "post_preview",
            "liker_username", "liker_nickname", "liker_display_name",
            "like_time", "like_month",
        ],
    )
    write_csv(
        OUT / "self_post_comments.csv",
        comments,
        [
            "post_tid", "post_id", "post_time", "post_month", "post_preview",
            "commenter_username", "commenter_nickname", "commenter_display_name",
            "comment_time", "comment_month", "comment_len",
        ],
    )

    liker_counts = Counter(row["liker_display_name"] for row in likes)
    active_months = defaultdict(set)
    for row in likes:
        active_months[row["liker_display_name"]].add(row["post_month"])
    liker_rows = [
        {
            "rank": idx,
            "liker_display_name": name,
            "like_count": count,
            "liked_post_months": len(active_months[name]),
        }
        for idx, (name, count) in enumerate(liker_counts.most_common(), start=1)
    ]
    write_csv(
        OUT / "liker_ranking.csv",
        liker_rows,
        ["rank", "liker_display_name", "like_count", "liked_post_months"],
    )

    summary = {
        "self_username": self_username,
        "self_posts": len(posts),
        "posts_with_likes": sum(1 for row in posts if row["like_count"] > 0),
        "total_likes": len(likes),
        "unique_likers": len(liker_counts),
        "total_comments": len(comments),
        "first_post_time": posts[0]["post_time"] if posts else None,
        "last_post_time": posts[-1]["post_time"] if posts else None,
        "output_dir": str(OUT),
    }
    (OUT / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
