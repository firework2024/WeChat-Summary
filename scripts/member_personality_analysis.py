import math
import json
import re
from collections import Counter, defaultdict
from pathlib import Path

import jieba
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from matplotlib import font_manager
from wordcloud import WordCloud

ROOT = Path(__file__).resolve().parents[1]
BASE = ROOT / "analysis" / "group_chat"
CSV = BASE / "messages.csv"
OUT = BASE / "member_profiles"


STOPWORDS = {
    "一个", "一些", "一下", "一直", "不会", "不是", "不要", "东西", "为了", "为啥", "也是",
    "了解", "事情", "事儿", "什么", "今天", "他们", "但是", "你们", "出来", "刚刚", "别人",
    "可能", "可以", "因为", "大家", "如果", "就是", "已经", "应该", "怎么", "感觉",
    "我们", "所以", "时候", "是不是", "有点", "没有", "然后", "现在", "真的", "知道", "自己",
    "还是", "这么", "这个", "这里", "这样", "那个", "那些", "进行", "觉得", "确实",
    "比较", "或者", "以及", "对于", "的话", "里面", "之前", "之后", "其实", "直接",
    "属于", "看到", "起来", "这种", "一下子",  "可怜", "捂脸",  
    "还有", "不能", "一样", "不过", "好像", "有人", "完全", "很多", "看看", "为什么",
    "肯定", "开始",  "而且", "一点", "每次", "一堆", "那么", "特别", "估计",
    "最近", "只有",  "这是", "其他", "地方", "the", "self", "and", "for", "with",
    "that", "this", "from", "you", "are", "微信", "电脑版","tmd", "tm", "md", "nmsl", "wcnm", "cnm","形容词","副词","不然",
    "can", "but", "was", "were", "they", "them", "their", "our", "ours", "his", "her", "she",
    "him", "has", "have", "had", "will", "would", "could", "should", "not", "all", "any",
    "some", "there", "here", "what", "when", "where", "who", "why", "how", "than", "then",
    "also", "just", "like", "about", "into", "over", "after", "before", "more", "most",
}

EMOJI_WORD = re.compile(r"^[\[\(（]?(旺柴|偷笑|破涕为笑|苦涩|可怜|捂脸|流泪|裂开)[\]\)）]?$")


def setup_style():
    font_candidates = [
        r"C:\Windows\Fonts\NotoSansSC-VF.ttf",
        r"C:\Windows\Fonts\msyh.ttc",
        r"C:\Windows\Fonts\simhei.ttf",
    ]
    font_path = None
    for path in font_candidates:
        if Path(path).exists():
            font_manager.fontManager.addfont(path)
            name = font_manager.FontProperties(fname=path).get_name()
            plt.rcParams["font.sans-serif"] = [name]
            font_path = path
            break
    plt.rcParams["axes.unicode_minus"] = False
    sns.set_theme(style="whitegrid", font=plt.rcParams["font.sans-serif"][0])
    return font_path


def canonical_sender_names(df):
    mapping = {}
    for username, part in df.groupby("sender_username", dropna=False):
        names = part["sender_name"].fillna("").astype(str).str.strip()
        readable = names[names.ne("") & ~names.str.startswith("wxid_") & names.ne(username)]
        if not readable.empty:
            mapping[username] = readable.value_counts().idxmax()
        else:
            mapping[username] = username or "未知"
    return mapping


def tokenize_text(text):
    if not isinstance(text, str) or not text.strip():
        return []
    text = re.sub(r"\[[^\]]{1,8}\]", " ", text)
    tokens = list(jieba.cut(text))
    tokens.extend(re.findall(r"[A-Za-z][A-Za-z0-9_+-]{2,}", text))
    cleaned = []
    for token in tokens:
        token = token.strip().lower()
        if (
            token
            and len(token) >= 2
            and token not in STOPWORDS
            and not EMOJI_WORD.match(token)
            and re.search(r"[\u4e00-\u9fffA-Za-z0-9]", token)
            and not token.startswith("wxid")
            and not token.isdigit()
        ):
            cleaned.append(token)
    return cleaned


def is_catchphrase_source(row):
    text = row.text
    if row.base_type != 1 or not isinstance(text, str):
        return False
    text = text.strip()
    if not text:
        return False
    # Catchphrases should come from short conversational utterances, not pasted
    # articles, prompts, code blocks, XML/link metadata, or long translated text.
    if len(text) > 180:
        return False
    if "\n" in text or "<msgsource" in text or "http://" in text or "https://" in text:
        return False
    letters = sum(ch.isascii() and ch.isalpha() for ch in text)
    cjk = sum("\u4e00" <= ch <= "\u9fff" for ch in text)
    if letters > 24 and letters > cjk:
        return False
    return True


def distinctive_terms(df):
    member_counts = {}
    doc_freq = Counter()
    for sender, part in df.groupby("sender"):
        tokens = []
        eligible = part[part.apply(is_catchphrase_source, axis=1)]
        for text in eligible["text"].dropna().astype(str):
            tokens.extend(tokenize_text(text))
        counts = Counter(tokens)
        member_counts[sender] = counts
        doc_freq.update(counts.keys())

    n_members = len(member_counts)
    scored = {}
    for sender, counts in member_counts.items():
        rows = []
        total = sum(counts.values()) or 1
        for word, count in counts.items():
            # Remove words used by most members, then score by frequency and distinctiveness.
            if doc_freq[word] >= max(3, math.ceil(n_members * 0.5)):
                continue
            tf = count / total
            idf = math.log((n_members + 1) / (doc_freq[word] + 0.5)) + 1
            score = count * idf * (1 + math.log1p(count))
            if count >= 2:
                rows.append((word, count, doc_freq[word], score, tf))
        rows.sort(key=lambda x: x[3], reverse=True)
        scored[sender] = rows
    return scored


def build_reply_stats(df, max_minutes=10):
    df = df.sort_values("datetime").copy()
    reply_to = defaultdict(Counter)
    reply_total = Counter()
    initiated_after_silence = Counter()
    prev = None
    for row in df.itertuples(index=False):
        sender = row.sender
        if not sender or pd.isna(sender):
            continue
        if prev is not None:
            gap = (row.datetime - prev.datetime).total_seconds() / 60
            if 0 <= gap <= max_minutes and sender != prev.sender:
                reply_to[sender][prev.sender] += 1
                reply_total[sender] += 1
            elif gap > 60:
                initiated_after_silence[sender] += 1
        prev = row
    return reply_to, reply_total, initiated_after_silence


def save_member_figure(member, df, all_df, terms, reply_to, reply_total, silence_start, font_path):
    safe_name = re.sub(r'[\\/:*?"<>|]+', "_", member)[:80]
    member_df = df[df["sender"].eq(member)].copy()
    total = len(member_df)
    text_total = int(member_df["base_type"].eq(1).sum())
    catchphrase_samples = int(member_df.apply(is_catchphrase_source, axis=1).sum())
    avg_len = member_df.loc[member_df["text_len"] > 0, "text_len"].mean()
    active_days = member_df["date"].nunique()
    first = member_df["datetime"].min().strftime("%Y-%m-%d")
    last = member_df["datetime"].max().strftime("%Y-%m-%d")

    fig = plt.figure(figsize=(18, 13))
    gs = fig.add_gridspec(3, 3, height_ratios=[0.9, 1.2, 1.15])
    fig.suptitle(f"{member} 的群聊个性化画像", fontsize=22, y=0.98)

    ax0 = fig.add_subplot(gs[0, 0])
    ax0.axis("off")
    overview = [
        ["消息数", f"{total:,}"],
        ["文本消息", f"{text_total:,}"],
        ["口头禅样本", f"{catchphrase_samples:,}"],
        ["活跃天数", f"{active_days:,}"],
        ["平均文本长度", f"{avg_len:.1f}" if pd.notna(avg_len) else "0"],
        ["首次/最近", f"{first} / {last}"],
        ["回复他人次数", f"{reply_total.get(member, 0):,}"],
        ["沉寂后发起", f"{silence_start.get(member, 0):,}"],
    ]
    table = ax0.table(cellText=overview, colLabels=["指标", "数值"], cellLoc="center", loc="center")
    table.auto_set_font_size(False)
    table.set_fontsize(11)
    table.scale(1, 1.6)
    ax0.set_title("概览")

    ax1 = fig.add_subplot(gs[0, 1:])
    top_terms = terms.get(member, [])[:18]
    if top_terms:
        term_df = pd.DataFrame(top_terms, columns=["word", "count", "member_df", "score", "tf"])
        sns.barplot(data=term_df.head(14), y="word", x="score", ax=ax1, color="#4C78A8")
        ax1.set_title("个人特色词（已去常见词和高重合词）")
        ax1.set_xlabel("区分度得分")
        ax1.set_ylabel("")
    else:
        ax1.text(0.5, 0.5, "特色词不足", ha="center", va="center")
        ax1.set_axis_off()

    ax2 = fig.add_subplot(gs[1, 0])
    freqs = {word: score for word, count, member_df_, score, tf in terms.get(member, [])[:100]}
    if freqs:
        wc = WordCloud(
            width=800, height=600, background_color="white",
            font_path=font_path, max_words=100, random_state=42, colormap="viridis"
        ).generate_from_frequencies(freqs)
        ax2.imshow(wc, interpolation="bilinear")
        ax2.set_axis_off()
        ax2.set_title("个性化词云")
    else:
        ax2.text(0.5, 0.5, "无足够词汇", ha="center", va="center")
        ax2.set_axis_off()

    ax3 = fig.add_subplot(gs[1, 1])
    hourly = member_df.groupby("hour").size().reindex(range(24), fill_value=0).reset_index(name="messages")
    sns.barplot(data=hourly, x="hour", y="messages", ax=ax3, color="#F58518")
    ax3.set_title("经常出没时间（小时）")
    ax3.set_xlabel("小时")
    ax3.set_ylabel("消息数")
    ax3.tick_params(axis="x", labelrotation=90)

    ax4 = fig.add_subplot(gs[1, 2])
    weekday_order = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]
    heat = member_df.pivot_table(index="weekday_cn", columns="hour", values="local_id", aggfunc="count", fill_value=0)
    heat = heat.reindex(weekday_order).fillna(0)
    sns.heatmap(heat, cmap="YlGnBu", ax=ax4, cbar=False)
    ax4.set_title("星期 x 小时热力图")
    ax4.set_xlabel("小时")
    ax4.set_ylabel("")

    ax5 = fig.add_subplot(gs[2, 0])
    type_df = member_df["type"].value_counts().reset_index()
    type_df.columns = ["type", "messages"]
    sns.barplot(data=type_df, y="type", x="messages", ax=ax5, color="#54A24B")
    ax5.set_title("发送消息类型")
    ax5.set_xlabel("消息数")
    ax5.set_ylabel("")

    ax6 = fig.add_subplot(gs[2, 1])
    replies = reply_to.get(member, Counter())
    reply_df = pd.DataFrame(replies.most_common(), columns=["target", "count"])
    if not reply_df.empty:
        sns.barplot(data=reply_df, y="target", x="count", ax=ax6, color="#E45756")
        ax6.set_title("在谁发言后更常回复（10分钟内）")
        ax6.set_xlabel("回复次数")
        ax6.set_ylabel("")
    else:
        ax6.text(0.5, 0.5, "无明显回复记录", ha="center", va="center")
        ax6.set_axis_off()

    ax7 = fig.add_subplot(gs[2, 2])
    days_by_member = {
        other: set(part["date"].dt.strftime("%Y-%m-%d"))
        for other, part in all_df.groupby("sender")
        if other != member
    }
    mine = set(member_df["date"].dt.strftime("%Y-%m-%d"))
    overlap = [
        (other, len(mine & days), len(mine & days) / len(mine) * 100 if mine else 0)
        for other, days in days_by_member.items()
    ]
    overlap_df = pd.DataFrame(overlap, columns=["member", "days", "pct"]).sort_values("days", ascending=False)
    if not overlap_df.empty:
        sns.barplot(data=overlap_df, y="member", x="days", ax=ax7, color="#B279A2")
        ax7.set_title("与其他成员同日活跃")
        ax7.set_xlabel("共同活跃天数")
        ax7.set_ylabel("")
    else:
        ax7.text(0.5, 0.5, "无共同活跃数据", ha="center", va="center")
        ax7.set_axis_off()

    fig.tight_layout(rect=[0, 0, 1, 0.96])
    fig.savefig(OUT / f"{safe_name}.png", dpi=180, bbox_inches="tight")
    plt.close(fig)

    return {
        "member": member,
        "messages": int(total),
        "text_messages": int(text_total),
        "catchphrase_samples": int(catchphrase_samples),
        "active_days": int(active_days),
        "avg_text_len": round(float(avg_len), 2) if pd.notna(avg_len) else 0,
        "reply_total": int(reply_total.get(member, 0)),
        "initiated_after_silence": int(silence_start.get(member, 0)),
        "distinctive_terms": [{"word": w, "count": int(c), "member_overlap": int(dfreq), "score": round(float(s), 3)} for w, c, dfreq, s, tf in terms.get(member, [])[:30]],
        "reply_to": [{"member": k, "count": int(v)} for k, v in reply_to.get(member, Counter()).most_common()],
    }


def main():
    font_path = setup_style()
    OUT.mkdir(parents=True, exist_ok=True)
    df = pd.read_csv(CSV, encoding="utf-8-sig")
    df["datetime"] = pd.to_datetime(df["datetime"])
    df["date"] = pd.to_datetime(df["date"])
    df["sender_username"] = df["sender_username"].fillna("").astype(str).str.strip()
    df["sender_name"] = df["sender_name"].fillna("").astype(str).str.strip()
    df = df[df["sender_username"].ne("")].copy()
    names = canonical_sender_names(df)
    df["sender"] = df["sender_username"].map(names)
    weekday_cn = {0: "周一", 1: "周二", 2: "周三", 3: "周四", 4: "周五", 5: "周六", 6: "周日"}
    df["weekday_cn"] = df["weekday"].map(weekday_cn)

    terms = distinctive_terms(df)
    reply_to, reply_total, silence_start = build_reply_stats(df)

    summaries = []
    for member in df["sender"].value_counts().index:
        summaries.append(save_member_figure(member, df, df, terms, reply_to, reply_total, silence_start, font_path))

    pd.DataFrame([
        {
            "member": s["member"],
            "messages": s["messages"],
            "text_messages": s["text_messages"],
            "catchphrase_samples": s["catchphrase_samples"],
            "active_days": s["active_days"],
            "avg_text_len": s["avg_text_len"],
            "reply_total": s["reply_total"],
            "initiated_after_silence": s["initiated_after_silence"],
        }
        for s in summaries
    ]).to_csv(OUT / "member_summary.csv", index=False, encoding="utf-8-sig")

    (OUT / "member_profiles.json").write_text(
        json.dumps(summaries, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"Generated {len(summaries)} member profile figures in {OUT}")


if __name__ == "__main__":
    main()
