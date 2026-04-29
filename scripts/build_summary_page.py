import csv
import html
import json
import math
import re
import sqlite3
import sys
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict
from contextlib import closing
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(ROOT / "scripts"))

from check_summary_feasibility import (
    END_EXCLUSIVE,
    SESSION_GAP_MINUTES,
    START,
    build_sessions,
    collect_period_messages,
    extract_voice_ms,
    own_sns_posts,
    strip_group_sender,
    text_for_keyword,
    tokenize,
)
from wechat_cli.core.contacts import get_contact_names, get_self_username
from wechat_cli.core.context import AppContext

SUMMARY_DIR = ROOT / "summary"
TEMPLATE = SUMMARY_DIR / "summary.txt"
OUT_HTML = SUMMARY_DIR / "index.html"
OUT_DATA = SUMMARY_DIR / "summary_data.json"
OUT_FILLED = SUMMARY_DIR / "summary_filled.txt"

WEEKDAYS = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]
POSITIVE_WORDS = ["哈哈", "开心", "快乐", "喜欢", "期待", "不错", "太好", "棒", "爽", "幸福", "可爱", "笑死"]
NEGATIVE_WORDS = ["好累", "难受", "焦虑", "崩溃", "痛苦", "烦", "麻了", "emo", "失眠", "哭", "完蛋", "不想"]
PHRASE_STOP = {"哎", "嗯", "哦", "好", "是", "行", "对", "啊", "诶", "哈哈", "哈哈哈", "ok", "okok"}
SUMMARY_STOPWORDS = {
    "我", "你", "他", "她", "它", "我们", "你们", "他们", "自己", "大家", "这个", "那个", "这些", "那些",
    "就是", "但是", "然后", "因为", "所以", "一个", "没有", "不是", "可以", "感觉", "还是", "什么", "真的",
    "现在", "一下", "以及", "可能", "已经", "直接", "不是", "应该", "需要", "这样", "那样", "这里", "那里",
    "今天", "明天", "昨天", "时候", "一下子", "哈哈", "哈哈哈", "笑死", "笑死了", "抱拳", "破涕为笑", "偷笑",
    "捂脸", "玫瑰", "呲牙", "流泪", "旺柴", "让我看看", "皱眉", "裂开", "尴尬", "表情", "图片", "视频",
    "文件", "链接", "收到", "转账", "语音", "聊天", "消息", "微信", "朋友圈", "群聊", "私聊", "老师", "同学",
    "问题", "感觉", "知道", "觉得", "好像", "确实", "其实", "不过", "而且", "或者", "如果", "虽然",
    "好的", "不错", "不知道", "没事儿", "感谢", "谢谢", "我知道", "我不知道", "我看看", "我感觉",
    "问题不大", "怎么说", "所有人", "另外", "欢迎各位报名参加", "腾讯会议", "杨老师", "新年快乐",
    "好的好的", "没关系", "差不多", "是啊", "写的", "谢谢您", "不好说", "万分感谢", "身体健康",
    "借楼打扰一下各位",
    "the", "and", "you", "for", "with", "this", "that", "are", "not", "ok", "okok", "xs", "xswl", "hh",
    "ai", "agent", "prompt", "docx", "pdf", "http", "https", "com", "www", "me", "humans",
}


def fmt_int(value):
    return f"{int(value):,}"


def pct(value):
    return f"{value * 100:.1f}"


def hour_label(hour):
    return f"{hour:02d}:00-{hour:02d}:59"


def safe_text(value, limit=None):
    value = str(value or "").replace("\n", " ").replace("\r", " ").strip()
    if limit and len(value) > limit:
        return value[:limit] + "..."
    return value


def load_all_private_first_seen():
    path = ROOT / "analysis" / "private_chats" / "private_messages.csv"
    first = {}
    if not path.exists():
        return first
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            username = row["chat_username"]
            dt = datetime.fromisoformat(row["datetime"])
            if username not in first or dt < first[username][0]:
                first[username] = (dt, row["chat_name"])
    return first


def short_text(row):
    text = text_for_keyword(row["content"], row["local_type"], row["is_group"])
    return safe_text(text)


def build_complete_sessions(private_rows):
    by_chat = defaultdict(list)
    for row in private_rows:
        by_chat[row["chat_username"]].append(row)
    sessions = []
    for chat, part in by_chat.items():
        current = []
        last_time = None
        for row in sorted(part, key=lambda r: r["datetime"]):
            if last_time is not None and (row["datetime"] - last_time).total_seconds() / 60 > SESSION_GAP_MINUTES:
                if current:
                    sessions.append(current)
                current = []
            current.append(row)
            last_time = row["datetime"]
        if current:
            sessions.append(current)
    complete = [s for s in sessions if {"me", "other"}.issubset({r["sender_role"] for r in s})]
    return complete


def emoticon_key(content):
    text = strip_group_sender(content, ":\n" in str(content))
    for pattern in [r'<emoji[^>]+md5="([^"]+)"', r'<emoji[^>]+cdnurl="([^"]+)"', r'md5="([^"]+)"']:
        match = re.search(pattern, text)
        if match:
            return match.group(1)[:18]
    return safe_text(text, 18) or "未知表情"


def meaningful_keywords(rows, limit=10):
    counter = Counter()
    for row in rows:
        if row["base_type"] != 1:
            continue
        text = short_text(row)
        if not text or len(text) > 180:
            continue
        if re.search(r"https?://|www\.|\.com|\.cn|\.pdf|\.docx|\.xlsx", text, re.I):
            continue
        text = re.sub(r"\[[^\]]+\]", " ", text)
        text = re.sub(r"<[^>]+>", " ", text)
        for word in tokenize(text):
            word = word.strip().lower()
            if word in SUMMARY_STOPWORDS:
                continue
            if word.startswith(("我", "你", "他", "她")):
                continue
            if not re.search(r"[\u4e00-\u9fff]", word):
                continue
            if len(word) < 2 or len(word) > 4:
                continue
            if any(x in word for x in ("各位", "谢谢", "感谢", "打扰")):
                continue
            if re.fullmatch(r"([\u4e00-\u9fff])\1+", word):
                continue
            counter[word] += 1
    return " · ".join(w for w, _ in counter.most_common(limit))


def own_sns_posts_with_likers(app, names):
    sns_path = app.cache.get("sns\\sns.db")
    self_username = get_self_username(app.db_dir, app.cache, app.decrypted_dir)
    posts = []
    liker_counts = Counter()
    if not sns_path:
        return posts, liker_counts
    with closing(sqlite3.connect(sns_path)) as conn:
        conn.row_factory = sqlite3.Row
        for row in conn.execute("SELECT tid, user_name, content FROM SnsTimeLine WHERE user_name=?", (self_username,)):
            try:
                root = ET.fromstring(row["content"] or "")
            except ET.ParseError:
                continue
            t = root.findtext(".//TimelineObject/createTime")
            if not (t and t.isdigit()):
                continue
            dt = datetime.fromtimestamp(int(t))
            if not (START <= dt < END_EXCLUSIVE):
                continue
            desc = root.findtext(".//TimelineObject/contentDesc") or ""
            like_nodes = root.findall(".//like_user_list/user_comment")
            comment_nodes = root.findall(".//comment_user_list/user_comment")
            for node in like_nodes:
                username = node.findtext("username") or ""
                nickname = node.findtext("nickname") or names.get(username) or username or "未知用户"
                liker_counts[nickname] += 1
            posts.append({
                "time": dt,
                "preview": desc.replace("\n", " ")[:80],
                "like_count": len(like_nodes),
                "comment_count": len(comment_nodes),
                "hour": dt.hour,
            })
    return sorted(posts, key=lambda r: r["time"]), liker_counts


def build_data():
    app = AppContext()
    names = get_contact_names(app.cache, app.decrypted_dir)
    self_username = get_self_username(app.db_dir, app.cache, app.decrypted_dir)
    rows = collect_period_messages(app, names, self_username)

    private_rows = [r for r in rows if r["is_private"]]
    group_rows = [r for r in rows if r["is_group"]]
    own_rows = [r for r in rows if r["sender_role"] == "me"]
    received_rows = [r for r in rows if r["sender_role"] != "me"]
    own_private = [r for r in private_rows if r["sender_role"] == "me"]
    own_group = [r for r in group_rows if r["sender_role"] == "me"]

    own_text_chars = sum(len(short_text(r)) for r in own_rows)
    page_count = math.ceil(own_text_chars / 300)

    private_by_chat = Counter(r["chat_name"] for r in private_rows)
    group_by_own = Counter(r["chat_name"] for r in own_group)
    private_contacts_count = len({r["chat_username"] for r in private_rows})
    active_group_count = len({r["chat_username"] for r in own_group})
    top3_private_total = sum(c for _, c in private_by_chat.most_common(3))
    top3_private_share = top3_private_total / max(len(private_rows), 1)

    sessions = build_sessions(private_rows)
    own_started = sum(1 for s in sessions if s["sender_role"] == "me")
    active_ratio = own_started / max(len(sessions), 1)

    streaks = []
    max_unreplied = (0, "")
    by_chat = defaultdict(list)
    for row in private_rows:
        by_chat[row["chat_username"]].append(row)
    for part in by_chat.values():
        current_role = None
        count = 0
        last_chat_name = part[0]["chat_name"] if part else ""
        for row in sorted(part, key=lambda r: r["datetime"]):
            if row["sender_role"] == current_role:
                count += 1
            else:
                if current_role == "me":
                    streaks.append(count)
                    if count > max_unreplied[0]:
                        max_unreplied = (count, last_chat_name)
                current_role = row["sender_role"]
                count = 1
                last_chat_name = row["chat_name"]
        if current_role == "me":
            streaks.append(count)
            if count > max_unreplied[0]:
                max_unreplied = (count, last_chat_name)

    own_private_daily = Counter(r["date"] for r in own_private)
    daily_max_date, daily_max_count = own_private_daily.most_common(1)[0] if own_private_daily else ("", 0)

    private_chat_daily = Counter((r["date"], r["chat_name"]) for r in private_rows)
    (active_date, active_chat), active_chat_count = private_chat_daily.most_common(1)[0] if private_chat_daily else (("", ""), 0)

    text_private_me = [r for r in own_private if r["base_type"] == 1]
    longest_text = max(text_private_me, key=lambda r: len(strip_group_sender(r["content"], False)), default=None)
    longest_text_len = len(strip_group_sender(longest_text["content"], False)) if longest_text else 0
    longest_text_receiver = longest_text["chat_name"] if longest_text else ""

    complete_sessions = build_complete_sessions(private_rows)
    shortest_session = min(complete_sessions, key=len, default=[])
    shortest_chat = shortest_session[0]["chat_name"] if shortest_session else ""
    shortest_count = len(shortest_session)

    all_first_seen = load_all_private_first_seen()
    new_private_users = {
        user: info for user, info in all_first_seen.items()
        if START <= info[0] < END_EXCLUSIVE
    }
    if not new_private_users:
        period_first = {}
        for row in private_rows:
            user = row["chat_username"]
            if user not in period_first or row["datetime"] < period_first[user][0]:
                period_first[user] = (row["datetime"], row["chat_name"])
        new_private_users = period_first
    hottest_new = ("", 0)
    new_private_started_by_me = 0
    new_private_started_by_other = 0
    for user, (first_dt, name) in new_private_users.items():
        until = first_dt + timedelta(days=7)
        count = sum(1 for r in private_rows if r["chat_username"] == user and first_dt <= r["datetime"] < until)
        if count > hottest_new[1]:
            hottest_new = (name, count)
        first_rows = [r for r in private_rows if r["chat_username"] == user]
        if first_rows:
            first_row = min(first_rows, key=lambda r: r["datetime"])
            if first_row["sender_role"] == "me":
                new_private_started_by_me += 1
            else:
                new_private_started_by_other += 1

    group_first = {}
    for row in group_rows:
        user = row["chat_username"]
        if user not in group_first or row["datetime"] < group_first[user]:
            group_first[user] = row["datetime"]
    new_group_count = sum(1 for dt in group_first.values() if START <= dt < END_EXCLUSIVE)

    pos_counts = Counter()
    neg_counts = Counter()
    haha_total = haha_me = ai_total = ai_me = 0
    private_words = Counter()
    group_words = Counter()
    phrase_counter = Counter()
    for row in rows:
        text = short_text(row)
        if not text:
            continue
        for w in POSITIVE_WORDS:
            pos_counts[w] += text.count(w)
        for w in NEGATIVE_WORDS:
            neg_counts[w] += text.count(w)
        h_count = len(re.findall(r"哈", text)) + sum(len(m.group(0)) for m in re.finditer(r"h{2,}", text, re.I))
        a_count = text.count("哎")
        haha_total += h_count
        ai_total += a_count
        if row["sender_role"] == "me":
            haha_me += h_count
            ai_me += a_count
            if row["is_private"]:
                private_words.update(tokenize(text))
            elif row["is_group"]:
                group_words.update(tokenize(text))
            if (
                row["is_private"]
                and row["base_type"] == 1
                and 1 <= len(text) <= 20
                and text not in PHRASE_STOP
                and "[" not in text
                and "]" not in text
                and re.search(r"[\u4e00-\u9fff]", text)
                and not re.search(r"https?://|www\.|<[^>]+>", text, re.I)
            ):
                phrase_counter[text] += 1

    pos_word, pos_count = pos_counts.most_common(1)[0] if pos_counts else ("", 0)
    neg_word, neg_count = neg_counts.most_common(1)[0] if neg_counts else ("", 0)
    emotion_ratio = pos_count / max(neg_count, 1)

    private_hour = Counter(r["hour"] for r in own_private).most_common(1)
    group_hour = Counter(r["hour"] for r in own_group).most_common(1)
    private_week = Counter(r["weekday"] for r in own_private).most_common(1)
    group_week = Counter(r["weekday"] for r in own_group).most_common(1)
    latest_msg = max(own_rows, key=lambda r: (r["hour"], r["datetime"].minute, r["datetime"].second), default=None)

    sns_posts, liker_counts = own_sns_posts_with_likers(app, names)
    sns_interactions = sum(p["like_count"] + p["comment_count"] for p in sns_posts)
    top_sns = max(sns_posts, key=lambda p: p["like_count"], default=None)
    first_sns = sns_posts[0] if sns_posts else None
    last_sns = sns_posts[-1] if sns_posts else None
    sns_hour = Counter(p["hour"] for p in sns_posts).most_common(1)
    top_liker, top_liker_count = liker_counts.most_common(1)[0] if liker_counts else ("暂无", 0)

    all_dates = {(START + timedelta(days=i)).date().isoformat() for i in range((END_EXCLUSIVE - START).days)}
    active_dates = {r["date"] for r in rows}
    pat_count = sum(1 for r in rows if r["base_type"] == 11000 or "拍了拍" in str(r["content"]))
    own_stickers = [r for r in own_rows if r["base_type"] == 47]
    sticker_top = Counter(emoticon_key(r["content"]) for r in own_stickers).most_common(3)
    own_images = sum(1 for r in own_rows if r["base_type"] == 3)
    own_voice = sum(1 for r in own_rows if r["base_type"] == 34)
    own_video = sum(1 for r in own_rows if r["base_type"] == 43)

    mapping = {
        "本人总消息数": fmt_int(len(own_rows)),
        "本人私聊消息数": fmt_int(len(own_private)),
        "本人群聊消息数": fmt_int(len(own_group)),
        "本期扫描消息数": fmt_int(len(rows)),
        "本期收到消息数": fmt_int(len(received_rows)),
        "本人日均消息数": f"{len(own_rows) / max((END_EXCLUSIVE - START).days, 1):.1f}",
        "消息折算页数": fmt_int(page_count),
        "私聊对象总数": fmt_int(private_contacts_count),
        "私聊前三名消息占比": pct(top3_private_share),
        "私聊互动最多的人第1名": private_by_chat.most_common(3)[0][0] if len(private_by_chat) > 0 else "",
        "第1名私聊消息数": fmt_int(private_by_chat.most_common(3)[0][1]) if len(private_by_chat) > 0 else "0",
        "私聊互动最多的人第2名": private_by_chat.most_common(3)[1][0] if len(private_by_chat) > 1 else "",
        "第2名私聊消息数": fmt_int(private_by_chat.most_common(3)[1][1]) if len(private_by_chat) > 1 else "0",
        "私聊互动最多的人第3名": private_by_chat.most_common(3)[2][0] if len(private_by_chat) > 2 else "",
        "第3名私聊消息数": fmt_int(private_by_chat.most_common(3)[2][1]) if len(private_by_chat) > 2 else "0",
        "私聊平均连续发送条数": f"{sum(streaks) / max(len(streaks), 1):.2f}",
        "私聊会话总段数": fmt_int(len(sessions)),
        "私聊单日最高消息日期": daily_max_date,
        "私聊单日最高消息条数": fmt_int(daily_max_count),
        "本人主动发起私聊次数": fmt_int(own_started),
        "主动发起聊天占比": pct(active_ratio),
        "最长未回复仍连续发送对象": max_unreplied[1],
        "未回复仍连续发送条数": fmt_int(max_unreplied[0]),
        "私聊最长单条字数": fmt_int(longest_text_len),
        "私聊最长消息接收者": longest_text_receiver,
        "私聊最活跃日期": active_date,
        "私聊最活跃日期-沟通对象": active_chat,
        "私聊最活跃日期-消息数": fmt_int(active_chat_count),
        "最短完整双向会话对象": shortest_chat,
        "最短完整双向会话条数": fmt_int(shortest_count),
        "本期首次出现私聊对象数": fmt_int(len(new_private_users)),
        "本期本人开启新私聊数": fmt_int(new_private_started_by_me),
        "本期对方开启新私聊数": fmt_int(new_private_started_by_other),
        "本期首次出现后互动最多的人": hottest_new[0],
        "首次出现后7天互动消息数": fmt_int(hottest_new[1]),
        "本期首次出现群聊数": fmt_int(new_group_count),
        "正向情绪关键词": pos_word,
        "正向情绪Top3": " · ".join(f"{w}({fmt_int(c)})" for w, c in pos_counts.most_common(3)),
        "正向词出现频次": fmt_int(pos_count),
        "负向情绪关键词": neg_word,
        "负向情绪Top3": " · ".join(f"{w}({fmt_int(c)})" for w, c in neg_counts.most_common(3)),
        "负向词出现频次": fmt_int(neg_count),
        "哈的个数": fmt_int(haha_total),
        "你说的哈的占比": pct(haha_me / max(haha_total, 1)),
        "哎的个数": fmt_int(ai_total),
        "你说的哎的占比": pct(ai_me / max(ai_total, 1)),
        "正负情绪比": f"{emotion_ratio:.2f}",
        "群聊最活跃的群第1名": group_by_own.most_common(3)[0][0] if len(group_by_own) > 0 else "",
        "第1名群聊本人消息数": fmt_int(group_by_own.most_common(3)[0][1]) if len(group_by_own) > 0 else "0",
        "群聊最活跃的群第2名": group_by_own.most_common(3)[1][0] if len(group_by_own) > 1 else "",
        "第2名群聊本人消息数": fmt_int(group_by_own.most_common(3)[1][1]) if len(group_by_own) > 1 else "0",
        "群聊最活跃的群第3名": group_by_own.most_common(3)[2][0] if len(group_by_own) > 2 else "",
        "第3名群聊本人消息数": fmt_int(group_by_own.most_common(3)[2][1]) if len(group_by_own) > 2 else "0",
        "本人活跃群聊数": fmt_int(active_group_count),
        "私聊活跃时间段": hour_label(private_hour[0][0]) if private_hour else "",
        "群聊活跃时间段": hour_label(group_hour[0][0]) if group_hour else "",
        "私聊活跃星期": WEEKDAYS[private_week[0][0]] if private_week else "",
        "群聊活跃星期": WEEKDAYS[group_week[0][0]] if group_week else "",
        "最晚聊天时间": latest_msg["datetime"].strftime("%Y-%m-%d %H:%M:%S") if latest_msg else "",
        "最晚聊天对象或群": latest_msg["chat_name"] if latest_msg else "",
        "私聊高频关键词": meaningful_keywords(own_private, 10),
        "群聊高频关键词": meaningful_keywords(own_group, 10),
        "朋友圈总发布条数": fmt_int(len(sns_posts)),
        "朋友圈总互动数": fmt_int(sns_interactions),
        "最多点赞的一条朋友圈摘要": safe_text(top_sns["preview"], 42) if top_sns else "",
        "最多点赞数": fmt_int(top_sns["like_count"]) if top_sns else "0",
        "给我点赞最多的人": top_liker,
        "给我点赞最多次数": fmt_int(top_liker_count),
        "最早一条朋友圈时间": first_sns["time"].strftime("%Y-%m-%d %H:%M:%S") if first_sns else "",
        "最早一条朋友圈摘要": safe_text(first_sns["preview"], 42) if first_sns else "",
        "最晚一条朋友圈时间": last_sns["time"].strftime("%Y-%m-%d %H:%M:%S") if last_sns else "",
        "朋友圈发布时间高峰时段": hour_label(sns_hour[0][0]) if sns_hour else "",
        "总聊天天数": fmt_int(len(active_dates)),
        "零聊天天数": fmt_int(len(all_dates - active_dates)),
        "拍一拍总次数": fmt_int(pat_count),
        "本人表情消息数": fmt_int(len(own_stickers)),
        "本人图片消息数": fmt_int(own_images),
        "本人语音消息数": fmt_int(own_voice),
        "本人视频消息数": fmt_int(own_video),
        "自己最常发的3个表情包": " · ".join(k for k, _ in sticker_top) if sticker_top else "暂无",
        "自己最常说的一句话": phrase_counter.most_common(1)[0][0] if phrase_counter else "",
    }
    return mapping


def fill_template(text, mapping):
    def repl(match):
        key = match.group(1)
        return mapping.get(key, match.group(0))
    return re.sub(r"【([^】]+)】", repl, text)


def split_pages(text):
    chunks = re.split(r"(?=Page\d+)", text.strip())
    return [chunk.strip() for chunk in chunks if chunk.strip()]


def page_to_html(page_text, index):
    lines = page_text.splitlines()
    title = lines[0].strip()
    body_lines = lines[1:]
    blocks = []
    paragraph = []
    for line in body_lines:
        if line.strip():
            paragraph.append(line.strip())
        elif paragraph:
            blocks.append("\n".join(paragraph))
            paragraph = []
    if paragraph:
        blocks.append("\n".join(paragraph))

    kicker = f"Page {index:02d}"
    if blocks and blocks[-1].startswith("口径说明："):
        note = blocks.pop()
    else:
        note = ""
    h1 = blocks.pop(0) if blocks else title

    def escape_lines(value):
        return html.escape(value).replace("\n", "<br>")

    body = []
    body.append(f'        <p class="kicker">{kicker}</p>')
    body.append(f"        <h1>{escape_lines(h1)}</h1>")
    for block in blocks:
        cls = " class=\"line\"" if ("第 1 名" in block or "第 2 名" in block or "第 3 名" in block) else ""
        body.append(f"        <p{cls}>{escape_lines(block)}</p>")
    if note:
        body.append(f'        <p class="note">{escape_lines(note)}</p>')
    return "\n".join(body)


def build_html(filled_text):
    pages = split_pages(filled_text)
    sections = []
    for i, page in enumerate(pages, start=1):
        active = " is-active" if i == 1 else ""
        sections.append(
            f'''    <section class="slide{active}" data-page="{i}" style="--bg: url('pictures/{i}.png')">
      <div class="copy">
{page_to_html(page, i)}
      </div>
    </section>'''
        )
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>2026 微信 1-4 月总结</title>
  <link rel="stylesheet" href="styles.css">
</head>
<body>
  <main class="deck" id="deck" aria-live="polite">
{chr(10).join(sections)}
  </main>

  <nav class="pager" aria-label="页面导航">
    <button class="nav-btn" id="prevBtn" type="button" aria-label="上一页">‹</button>
    <div class="dots" id="dots"></div>
    <button class="nav-btn" id="nextBtn" type="button" aria-label="下一页">›</button>
  </nav>

  <div class="progress" aria-hidden="true"><span id="progressBar"></span></div>
  <script src="app.js"></script>
</body>
</html>
"""


def main():
    template = TEMPLATE.read_text(encoding="utf-8")
    mapping = build_data()
    filled = fill_template(template, mapping)
    OUT_DATA.write_text(json.dumps(mapping, ensure_ascii=False, indent=2), encoding="utf-8")
    OUT_FILLED.write_text(filled, encoding="utf-8")
    OUT_HTML.write_text(build_html(filled), encoding="utf-8")
    print(json.dumps({
        "data": str(OUT_DATA),
        "filled_text": str(OUT_FILLED),
        "html": str(OUT_HTML),
        "fields": len(mapping),
        "unfilled": re.findall(r"【([^】]+)】", filled),
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
