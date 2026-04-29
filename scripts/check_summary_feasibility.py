import csv
import hashlib
import json
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

from wechat_cli.core.contacts import get_contact_names, get_self_username
from wechat_cli.core.context import AppContext
from wechat_cli.core.messages import _load_name2id_maps, _split_msg_type, decompress_content

OUT = ROOT / "summary" / "feasibility"
START = datetime(2026, 1, 1)
END_EXCLUSIVE = datetime(2026, 4, 29)
SESSION_GAP_MINUTES = 60

POSITIVE_WORDS = ["哈哈", "开心", "快乐", "喜欢", "期待", "不错", "太好", "棒", "爽", "幸福", "可爱", "笑死"]
NEGATIVE_WORDS = ["好累", "难受", "焦虑", "崩溃", "痛苦", "烦", "麻了", "emo", "失眠", "哭", "完蛋", "不想"]
STOPWORDS = {
    "我", "你", "他", "她", "它", "我们", "你们", "他们", "这个", "那个", "就是", "但是", "然后", "因为", "所以",
    "一个", "没有", "不是", "可以", "感觉", "还是", "什么", "真的", "现在", "一下", "以及", "可能", "已经",
}


def ts(dt):
    return int(dt.timestamp())


def display_name(username, names):
    return names.get(username, username)


def table_hash(username):
    return f"Msg_{hashlib.md5(username.encode()).hexdigest()}"


def base_type(local_type):
    return _split_msg_type(local_type)[0]


def is_private(username):
    return username and not username.endswith("@chatroom") and not username.startswith("gh_") and username not in {
        "filehelper", "notifymessage", "medianote", "newsapp", "weixin", "floatbottle"
    }


def strip_group_sender(content, is_group):
    if not isinstance(content, str):
        return ""
    if is_group and ":\n" in content:
        return content.split(":\n", 1)[1]
    return content


def extract_voice_ms(content):
    if not isinstance(content, str) or "<voicemsg" not in content:
        return None
    try:
        root = ET.fromstring(strip_group_sender(content, ":\n" in content))
    except ET.ParseError:
        return None
    node = root.find(".//voicemsg")
    if node is None:
        return None
    for key in ["voicelength", "length"]:
        val = node.attrib.get(key)
        if val and val.isdigit():
            return int(val)
    return None


def text_for_keyword(content, local_type, is_group):
    text = strip_group_sender(content, is_group)
    b = base_type(local_type)
    if b == 1:
        return text
    if b == 49 and "<appmsg" in text:
        try:
            root = ET.fromstring(text)
        except ET.ParseError:
            return ""
        title = root.findtext(".//appmsg/title") or ""
        desc = root.findtext(".//appmsg/des") or ""
        return f"{title} {desc}".strip()
    if b == 10000:
        return text
    return ""


def tokenize(text):
    try:
        import jieba
        words = jieba.lcut(text)
    except Exception:
        words = re.findall(r"[\u4e00-\u9fff]{2,}|[A-Za-z]{2,}", text)
    cleaned = []
    for word in words:
        word = word.strip().lower()
        if len(word) < 2 or word in STOPWORDS:
            continue
        if re.fullmatch(r"\d+", word):
            continue
        cleaned.append(word)
    return cleaned


def discover_contexts(app):
    contexts = []
    seen = set()
    for rel in app.msg_db_keys:
        path = app.cache.get(rel)
        if not path:
            continue
        with closing(sqlite3.connect(path)) as conn:
            names = [row[0] for row in conn.execute("SELECT user_name FROM Name2Id").fetchall() if row[0]]
            tables = {
                row[0]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'Msg_%'"
                ).fetchall()
            }
            for username in names:
                tbl = table_hash(username)
                if tbl not in tables:
                    continue
                key = (path, tbl, username)
                if key in seen:
                    continue
                seen.add(key)
                contexts.append({"db_path": path, "table_name": tbl, "username": username})
    return contexts


def collect_period_messages(app, names, self_username):
    rows = []
    start_ts = ts(START)
    end_ts = ts(END_EXCLUSIVE)
    for ctx in discover_contexts(app):
        username = ctx["username"]
        is_group = username.endswith("@chatroom")
        if username.startswith("gh_") or username in {"notifymessage", "newsapp", "weixin"}:
            continue
        with closing(sqlite3.connect(ctx["db_path"])) as conn:
            idmap = _load_name2id_maps(conn)
            query = f"""
                SELECT local_id, local_type, create_time, real_sender_id,
                       message_content, WCDB_CT_message_content
                FROM [{ctx['table_name']}]
                WHERE create_time >= ? AND create_time < ?
                ORDER BY create_time
            """
            for local_id, local_type, create_time, real_sender_id, content, ct in conn.execute(query, (start_ts, end_ts)):
                sender_username = (idmap.get(real_sender_id, "") or "").strip()
                if sender_username == self_username:
                    role = "me"
                elif is_group:
                    role = "other" if sender_username else "unknown"
                elif sender_username == username:
                    role = "other"
                else:
                    role = "unknown"
                content = decompress_content(content, ct)
                dt = datetime.fromtimestamp(create_time)
                b, sub = _split_msg_type(local_type)
                rows.append({
                    "chat_username": username,
                    "chat_name": display_name(username, names),
                    "is_group": is_group,
                    "is_private": is_private(username),
                    "local_id": local_id,
                    "datetime": dt,
                    "date": dt.date().isoformat(),
                    "weekday": dt.weekday(),
                    "hour": dt.hour,
                    "sender_username": sender_username,
                    "sender_role": role,
                    "local_type": local_type,
                    "base_type": b,
                    "sub_type": sub,
                    "content": content or "",
                })
    rows.sort(key=lambda r: (r["datetime"], r["chat_username"], r["local_id"]))
    return rows


def build_sessions(rows, private_only=True):
    sessions = []
    by_chat = defaultdict(list)
    for row in rows:
        if private_only and not row["is_private"]:
            continue
        by_chat[row["chat_username"]].append(row)
    for chat, part in by_chat.items():
        last_time = None
        for row in sorted(part, key=lambda r: r["datetime"]):
            if last_time is None or (row["datetime"] - last_time).total_seconds() / 60 > SESSION_GAP_MINUTES:
                sessions.append(row)
            last_time = row["datetime"]
    return sessions


def own_sns_posts(app):
    sns_path = app.cache.get("sns\\sns.db")
    self_username = get_self_username(app.db_dir, app.cache, app.decrypted_dir)
    posts = []
    if not sns_path:
        return posts
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
            like_count = len(root.findall(".//like_user_list/user_comment"))
            comment_count = len(root.findall(".//comment_user_list/user_comment"))
            posts.append({
                "time": dt,
                "preview": desc.replace("\n", " ")[:80],
                "like_count": like_count,
                "comment_count": comment_count,
                "hour": dt.hour,
            })
    return sorted(posts, key=lambda r: r["time"])


def contact_db_findings(app):
    path = app.cache.get("contact/contact.db")
    if not path:
        return {}
    with closing(sqlite3.connect(path)) as conn:
        tables = {
            row[0]: [c[1] for c in conn.execute(f"PRAGMA table_info([{row[0]}])").fetchall()]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        return {
            "contact_columns": tables.get("contact", []),
            "chat_room_columns": tables.get("chat_room", []),
            "ticket_info_count": conn.execute("SELECT COUNT(*) FROM ticket_info").fetchone()[0] if "ticket_info" in tables else 0,
            "chatroom_member_count": conn.execute("SELECT COUNT(*) FROM chatroom_member").fetchone()[0] if "chatroom_member" in tables else 0,
        }


def emoticon_findings(app):
    path = app.cache.get("emoticon/emoticon.db")
    if not path:
        return {}
    with closing(sqlite3.connect(path)) as conn:
        tables = [row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")]
        return {
            "tables": tables,
            "fav_count": conn.execute("SELECT COUNT(*) FROM kNonStoreEmoticonTable").fetchone()[0] if "kNonStoreEmoticonTable" in tables else None,
            "has_caption": "kStoreEmoticonCaptionsTable" in tables,
        }


def add_item(items, page, metric, status, source, method, caveat, sample):
    items.append({
        "page": page,
        "metric": metric,
        "status": status,
        "source": source,
        "method": method,
        "caveat": caveat,
        "sample_result": sample,
    })


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    app = AppContext()
    names = get_contact_names(app.cache, app.decrypted_dir)
    self_username = get_self_username(app.db_dir, app.cache, app.decrypted_dir)
    rows = collect_period_messages(app, names, self_username)

    private_rows = [r for r in rows if r["is_private"]]
    group_rows = [r for r in rows if r["is_group"]]
    own_rows = [r for r in rows if r["sender_role"] == "me"]
    own_private = [r for r in private_rows if r["sender_role"] == "me"]
    own_group = [r for r in group_rows if r["sender_role"] == "me"]
    text_private_me = [r for r in own_private if r["base_type"] == 1]
    text_all = [(r, text_for_keyword(r["content"], r["local_type"], r["is_group"])) for r in rows]
    text_all = [(r, t) for r, t in text_all if t]

    private_by_chat = Counter(r["chat_name"] for r in private_rows)
    group_by_chat_own = Counter(r["chat_name"] for r in own_group)
    active_dates = {r["date"] for r in rows}
    all_dates = {(START + timedelta(days=i)).date().isoformat() for i in range((END_EXCLUSIVE - START).days)}

    sessions = build_sessions(private_rows)
    own_started = sum(1 for s in sessions if s["sender_role"] == "me")
    contact_started = sum(1 for s in sessions if s["sender_role"] == "other")

    streaks = []
    max_unreplied = (0, "")
    by_chat = defaultdict(list)
    for r in private_rows:
        by_chat[r["chat_username"]].append(r)
    for chat, part in by_chat.items():
        current_role = None
        count = 0
        for row in sorted(part, key=lambda r: r["datetime"]):
            if row["sender_role"] == current_role:
                count += 1
            else:
                if current_role == "me":
                    streaks.append(count)
                    if count > max_unreplied[0]:
                        max_unreplied = (count, row["chat_name"])
                current_role = row["sender_role"]
                count = 1
        if current_role == "me":
            streaks.append(count)
            if count > max_unreplied[0]:
                max_unreplied = (count, part[0]["chat_name"])

    own_private_daily = Counter(r["date"] for r in own_private)
    private_chat_daily = Counter((r["date"], r["chat_name"]) for r in private_rows)
    longest_text = max(
        text_private_me,
        key=lambda r: len(strip_group_sender(r["content"], False)),
        default=None,
    )
    voice_private = [r for r in own_private if r["base_type"] == 34]
    voice_lengths = [(extract_voice_ms(r["content"]) or 0, r) for r in voice_private]
    longest_voice = max(voice_lengths, default=(0, None), key=lambda x: x[0])

    pos_counts = Counter()
    neg_counts = Counter()
    haha_total = 0
    ai_total = 0
    haha_me = 0
    ai_me = 0
    for row, text in text_all:
        for w in POSITIVE_WORDS:
            c = text.count(w)
            if c:
                pos_counts[w] += c
        for w in NEGATIVE_WORDS:
            c = text.count(w)
            if c:
                neg_counts[w] += c
        h = len(re.findall(r"哈", text)) + sum(len(m.group(0)) for m in re.finditer(r"h{2,}", text, re.I))
        a = text.count("哎")
        haha_total += h
        ai_total += a
        if row["sender_role"] == "me":
            haha_me += h
            ai_me += a

    private_words = Counter()
    group_words = Counter()
    for row, text in text_all:
        if row["sender_role"] != "me":
            continue
        if row["is_private"]:
            private_words.update(tokenize(text))
        elif row["is_group"]:
            group_words.update(tokenize(text))

    sns_posts = own_sns_posts(app)
    contact_info = contact_db_findings(app)
    emoticon_info = emoticon_findings(app)

    add_friend_hits = [r for r, t in text_all if "通过了你的朋友验证" in t or "你已添加了" in t]
    new_group_candidates = [
        name for name, first in {
            chat: min(r["datetime"] for r in rows if r["chat_username"] == chat)
            for chat in {r["chat_username"] for r in group_rows}
        }.items()
        if START <= first < END_EXCLUSIVE
    ]

    items = []
    add_item(items, 1, "私聊/群聊/总消息数、折算页数", "可直接计算", "message/message_*.db", "按 create_time、chat_username 是否 @chatroom、sender_role 汇总。", "需定义是统计本人发送还是收发总量；建议排除公众号 gh_。", f"本人私聊 {len(own_private):,}，本人群聊 {len(own_group):,}，本人合计 {len(own_rows):,}")
    add_item(items, 2, "私聊互动最多 Top3", "可直接计算", "message/message_*.db + contact.db", "一对一会话按 chat_username 聚合收发消息量。", "可按收发总量或本人发送量，两种口径需统一。", "; ".join([f"{k}:{v}" for k, v in private_by_chat.most_common(3)]))
    add_item(items, 3, "平均连续私聊发送条数、单日最高、主动发起比例、未回复连续发送峰值", "可计算/需口径", "message/message_*.db", f"以 {SESSION_GAP_MINUTES} 分钟沉默切会话；连续 sender_role=me 计 streak。", "主动发起依赖会话切分阈值；未回复峰值容易被文件/表情刷屏放大。", f"平均 streak {sum(streaks)/len(streaks):.2f}；主动发起 {own_started}/{len(sessions)}；最长未回连发 {max_unreplied[0]} 给 {max_unreplied[1]}")
    daily_max = own_private_daily.most_common(1)[0] if own_private_daily else ("", 0)
    add_item(items, 3, "私聊单日最高消息数及日期", "可直接计算", "message/message_*.db", "本人私聊消息按 date 聚合。", "只反映本机已保存记录。", f"{daily_max[0]}: {daily_max[1]} 条")
    if longest_text:
        lt_text = strip_group_sender(longest_text["content"], False)
        add_item(items, 4, "私聊最长单条字数及接收者", "可直接计算", "message/message_*.db.message_content", "筛选本人私聊文本消息，len(text)。", "长链接/XML/转发内容需排除；建议只用 base_type=1 文本。", f"{len(lt_text)} 字 -> {longest_text['chat_name']}")
    voice_share = len(voice_private) / max(len(own_private), 1) * 100
    add_item(items, 4, "语音消息占比、最长语音时长", "可计算", "message_content XML voicemsg", "base_type=34；解析 voicemsg@voicelength 毫秒。", "少量语音可能只有媒体文件无 XML 时长；需兜底。", f"本人私聊语音 {len(voice_private)} 条，占 {voice_share:.2f}%；最长 {longest_voice[0]/1000:.1f} 秒")
    active_day = private_chat_daily.most_common(1)[0] if private_chat_daily else (("", ""), 0)
    add_item(items, 4, "最活跃私聊日期、对象、消息数", "可直接计算", "message/message_*.db", "私聊按 date+chat 聚合收发消息。", "若要只统计本人发送可调整。", f"{active_day[0][0]} 和 {active_day[0][1]}：{active_day[1]} 条")
    add_item(items, 4, "最短对话来回条数/惜字如金", "弱可行", "message/message_*.db", "可按会话切分后找最短双向会话或最短文本。", "文案中“几个字”定义混乱：是回合条数还是字数？需要改口径。", "建议改为：最短但完整的一次双向会话。")
    add_item(items, 5, "新加好友数量", "部分可行", "系统消息/FTS/contact.db", "搜索“通过了你的朋友验证”“你已添加了”等系统消息。", "contact 表无可靠 add_time；漏掉无聊天或清理过的验证记录。", f"系统消息命中 {len(add_friend_hits)} 条；contact 表字段无创建时间")
    add_item(items, 5, "添加好友最多的群聊来源、常用打招呼、升温最快", "基本不可直接获得", "无稳定字段", "可从验证系统消息和后续首聊近似推断。", "数据库没有可靠“来源群聊/添加时间/验证语”结构化字段；只能做启发式。", "建议降级为：1-4月首次聊天的新联系人 Top。")
    add_item(items, 5, "新加入群聊数量", "可近似推断", "message tables + contact.chat_room", "统计本期首次出现消息的 chatroom，或系统入群消息。", "如果老群首次在本期说话会误判；join time 不稳定。", f"本期首次出现群聊候选 {len(new_group_candidates)} 个")
    add_item(items, 6, "正/负向情绪关键词频次", "可计算/需词表", "message_content/FTS", "按预设词表扫描文本、链接标题、系统文本。", "不是严格情绪识别；词表决定结果。", f"正向Top {pos_counts.most_common(3)}；负向Top {neg_counts.most_common(3)}")
    add_item(items, 6, "哈哈/哎数量、本人占比、峰值星期", "可直接计算", "message_content", "正则扫描“哈/hh”和“哎”；按 sender_role/date 聚合。", "英文 h 可能误伤技术文本，需过滤。", f"哈总 {haha_total}，本人占 {haha_me/max(haha_total,1):.1%}；哎总 {ai_total}，本人占 {ai_me/max(ai_total,1):.1%}")
    add_item(items, 7, "群聊最活跃 Top3", "可直接计算", "message/message_*.db", "群聊按本人发送或群内总消息聚合。", "建议用本人发送量，更贴合“你的声音”。", "; ".join([f"{k}:{v}" for k, v in group_by_chat_own.most_common(3)]))
    add_item(items, 7, "私聊/群聊活跃时段、星期、最晚聊天", "可直接计算", "message/message_*.db", "按 hour、weekday 聚合；最晚可按 hour/minute 或凌晨时段定义。", "“最晚”需定义是自然日最晚还是凌晨最晚。", f"活跃天数 {len(active_dates)}，零聊天 {len(all_dates-active_dates)}")
    add_item(items, 8, "私聊/群聊高频关键词、词云", "可计算", "message_content + jieba", "分私聊/群聊、本人发送文本分词去停用词。", "需要停用词和排除长文/链接污染。", f"私聊Top {private_words.most_common(5)}；群聊Top {group_words.most_common(5)}")
    if sns_posts:
        top_sns = max(sns_posts, key=lambda r: r["like_count"])
        first_sns = sns_posts[0]
        last_sns = max(sns_posts, key=lambda r: r["time"].hour * 60 + r["time"].minute)
        hour_top = Counter(p["hour"] for p in sns_posts).most_common(1)[0]
        add_item(items, 9, "朋友圈发布数、点赞评论互动、最高赞、最早/最晚、发布时间段", "可直接计算", "sns/sns.db SnsTimeLine.content", "筛选本人 TimelineObject，解析 createTime、like_user_list、comment_user_list。", "前提是客户端已向前刷新，sns.db 只保存本地缓存过的朋友圈。", f"本期本人朋友圈 {len(sns_posts)} 条，互动 {sum(p['like_count']+p['comment_count'] for p in sns_posts)}；最高赞 {top_sns['like_count']}")
    add_item(items, 10, "总聊天天数/零聊天天数", "可直接计算", "message/message_*.db", "按 date 是否有任意消息。", "可区分本人发送或收发总量。", f"有聊天 {len(active_dates)} 天；零聊天 {len(all_dates-active_dates)} 天")
    pat_count = sum(1 for r in rows if r["base_type"] == 11000 or "拍了拍" in str(r["content"]))
    add_item(items, 10, "拍一拍总次数", "可计算", "message_content/system/type 11000", "扫描 base_type=11000 或文本“拍了拍”。", "不同版本可能以系统消息存储，需同时扫文本。", f"命中 {pat_count} 条")
    sticker_me = [r for r in own_rows if r["base_type"] == 47]
    add_item(items, 10, "最常用3个表情包", "部分可行", "message type 47 + emoticon.db", "可按表情消息 XML 中 md5/cdnurl 聚合，并用 emoticon.db 查 caption。", "很多表情没有可读名称，只能显示 md5/缩略图；需要额外解析媒体。", f"本人表情消息 {len(sticker_me)}；收藏表情表 {emoticon_info.get('fav_count')} 条")
    phrase_counter = Counter()
    for r in text_private_me:
        text = strip_group_sender(r["content"], False).strip()
        if 1 <= len(text) <= 20:
            phrase_counter[text] += 1
    add_item(items, 10, "自己最常说的一句话", "可计算", "message_content", "本人文本消息中过滤短句后计数。", "需去掉“哈哈哈/嗯/好”等无意义词，否则结果会很口语但可能无趣。", str(phrase_counter.most_common(5)))

    fields = ["page", "metric", "status", "source", "method", "caveat", "sample_result"]
    with (OUT / "metric_feasibility.csv").open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(items)

    summary = {
        "period": f"{START:%Y-%m-%d} to {(END_EXCLUSIVE - timedelta(days=1)):%Y-%m-%d}",
        "self_username": self_username,
        "messages_scanned": len(rows),
        "own_messages": len(own_rows),
        "own_private_messages": len(own_private),
        "own_group_messages": len(own_group),
        "private_messages_all_roles": len(private_rows),
        "group_messages_all_roles": len(group_rows),
        "sns_own_posts": len(sns_posts),
        "contact_db_findings": contact_info,
        "emoticon_db_findings": emoticon_info,
        "output_dir": str(OUT),
    }
    (OUT / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    status_counts = Counter(item["status"] for item in items)
    lines = [
        "# 微信 2026 年 1-4 月总结文案数据可行性盘点",
        "",
        f"- 时间范围: {summary['period']}",
        f"- 扫描消息: {len(rows):,} 条",
        f"- 本人发送: {len(own_rows):,} 条，其中私聊 {len(own_private):,}，群聊 {len(own_group):,}",
        f"- 本人朋友圈: {len(sns_posts):,} 条",
        f"- 可行性概览: {dict(status_counts)}",
        "",
        "## 指标明细",
        "",
        "| 页 | 指标 | 可行性 | 数据源 | 方法 | 风险/口径 | 样例 |",
        "|---|---|---|---|---|---|---|",
    ]
    for item in items:
        lines.append(
            f"| {item['page']} | {item['metric']} | {item['status']} | `{item['source']}` | {item['method']} | {item['caveat']} | {item['sample_result']} |"
        )
    (OUT / "report.md").write_text("\n".join(lines), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
