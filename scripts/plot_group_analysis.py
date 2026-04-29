import json
import re
from collections import Counter
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
import jieba
from matplotlib import font_manager
from wordcloud import WordCloud

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "analysis" / "group_chat"
CSV = OUT / "messages.csv"
META = OUT / "metadata.json"


def setup_style():
    candidates = [
        r"C:\Windows\Fonts\NotoSansSC-VF.ttf",
        r"C:\Windows\Fonts\msyh.ttc",
        r"C:\Windows\Fonts\simhei.ttf",
    ]
    for path in candidates:
        if Path(path).exists():
            font_manager.fontManager.addfont(path)
            name = font_manager.FontProperties(fname=path).get_name()
            plt.rcParams["font.sans-serif"] = [name]
            break
    plt.rcParams["axes.unicode_minus"] = False
    sns.set_theme(style="whitegrid", font=plt.rcParams["font.sans-serif"][0])


def savefig(name):
    plt.tight_layout()
    plt.savefig(OUT / name, dpi=180, bbox_inches="tight")
    plt.close()


def pct(part, total):
    return part / total * 100 if total else 0


def markdown_table(frame):
    if frame.empty:
        return "_无数据_"
    cols = list(frame.columns)
    lines = [
        "| " + " | ".join(map(str, cols)) + " |",
        "| " + " | ".join(["---"] * len(cols)) + " |",
    ]
    for _, row in frame.iterrows():
        lines.append("| " + " | ".join(str(row[col]) for col in cols) + " |")
    return "\n".join(lines)


def tokenize(texts):
    stop = {
        "一个", "一些", "一下", "一直", "不会", "不是", "不要", "东西", "为了", "为啥", "也是",
        "了解", "事情", "什么", "今天", "他们", "他们", "但是", "你们", "出来", "刚刚", "别人",
        "可能", "可以", "因为", "大家", "如果", "它们", "就是", "已经", "应该", "怎么", "感觉",
        "我们", "所以", "时候", "是不是", "有点", "没有", "然后", "现在", "真的", "知道", "自己",
        "还是", "这么", "这个", "这里", "这样", "那个", "那些", "进行", "还是", "觉得", "确实",
        "比较", "或者", "以及", "对于", "的话", "的话", "里面", "之前", "之后", "其实", "直接",
        "属于", "出来", "看到", "起来", "这种", "的话", "一下子", "哈哈", "哈哈哈", "笑死",
        "还有", "不能", "一样", "不过", "好像", "有人", "完全", "很多", "看看", "为什么",
        "肯定", "开始", "需要", "而且", "一点", "每次", "一堆", "那个", "这种", "这么",
        "笑死了", "笑死我了", "旺柴", "偷笑", "破涕为笑", "苦涩", "可怜", "捂脸", "bushi",
        "xsl", "the", "self", "and", "for", "with", "that", "this", "from", "you", "are",
        "微信", "电脑版",
    }
    bracket_noise = re.compile(r"^[\[\(（]?(旺柴|偷笑|破涕为笑|苦涩|可怜|捂脸|流泪|裂开)[\]\)）]?$")
    words = []
    for text in texts.dropna().astype(str):
        text = re.sub(r"\[[^\]]{1,8}\]", " ", text)
        tokens = list(jieba.cut(text))
        tokens.extend(re.findall(r"[A-Za-z][A-Za-z0-9_+-]{2,}", text))
        for token in tokens:
            token = token.strip().lower()
            if (
                token
                and token not in stop
                and len(token) >= 2
                and not bracket_noise.match(token)
                and not token.startswith("wxid")
                and not token.isdigit()
            ):
                words.append(token)
    return Counter(words)


def canonical_sender_names(df):
    mapping = {}
    for username, part in df.groupby("sender_username", dropna=False):
        username = "" if pd.isna(username) else str(username)
        names = part["sender_name"].fillna("").astype(str)
        readable = names[
            names.ne("")
            & ~names.str.startswith("wxid_")
            & names.ne(username)
        ]
        if not readable.empty:
            mapping[username] = readable.value_counts().idxmax()
        elif username:
            mapping[username] = username
        else:
            mapping[username] = "未知"
    return mapping


def main():
    setup_style()
    OUT.mkdir(parents=True, exist_ok=True)
    meta = json.loads(META.read_text(encoding="utf-8"))
    df = pd.read_csv(CSV, encoding="utf-8-sig")
    df["datetime"] = pd.to_datetime(df["datetime"])
    df["sender_username"] = df["sender_username"].fillna("").astype(str).str.strip()
    df["sender_name"] = df["sender_name"].fillna("").astype(str).str.strip()
    df["date"] = pd.to_datetime(df["date"])
    df["weekday_name"] = df["datetime"].dt.day_name()
    weekday_cn = {0: "周一", 1: "周二", 2: "周三", 3: "周四", 4: "周五", 5: "周六", 6: "周日"}
    df["weekday_cn"] = df["weekday"].map(weekday_cn)
    df["is_text"] = df["base_type"].eq(1)
    sender_name_map = canonical_sender_names(df)
    df["sender"] = df["sender_username"].fillna("").astype(str).map(sender_name_map).fillna("未知")

    total = len(df)
    sender_count = df["sender_username"].replace("", pd.NA).nunique()
    first_time = df["datetime"].min()
    last_time = df["datetime"].max()
    days = max((last_time.normalize() - first_time.normalize()).days + 1, 1)
    active_days = df["date"].nunique()

    summary = {
        "群聊": meta["chat"],
        "群ID": meta["username"],
        "消息总数": int(total),
        "参与发言人数": int(sender_count),
        "首条时间": str(first_time),
        "末条时间": str(last_time),
        "跨度天数": int(days),
        "有消息天数": int(active_days),
        "日均消息": round(total / days, 2),
        "活跃日均消息": round(total / active_days, 2) if active_days else 0,
        "文本消息占比": round(pct(df["is_text"].sum(), total), 2),
        "平均文本长度": round(df.loc[df["text_len"] > 0, "text_len"].mean(), 2) if (df["text_len"] > 0).any() else 0,
    }
    (OUT / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    # 1. Overview cards as a small table.
    fig, ax = plt.subplots(figsize=(12, 4.8))
    ax.axis("off")
    card_items = [
        ("消息总数", f"{summary['消息总数']:,}"),
        ("参与人数", f"{summary['参与发言人数']:,}"),
        ("有消息天数", f"{summary['有消息天数']:,}"),
        ("活跃日均", f"{summary['活跃日均消息']:,}"),
        ("文本占比", f"{summary['文本消息占比']}%"),
        ("平均文本长度", f"{summary['平均文本长度']}"),
    ]
    table = ax.table(cellText=card_items, colLabels=["指标", "数值"], loc="center", cellLoc="center")
    table.auto_set_font_size(False)
    table.set_fontsize(13)
    table.scale(1, 2.1)
    ax.set_title(f"{meta['chat']} 群聊概览", fontsize=18, pad=18)
    savefig("01_overview.png")

    # 2. Daily trend.
    daily = df.groupby(df["datetime"].dt.date).size().rename("messages").reset_index()
    daily["date"] = pd.to_datetime(daily["datetime"])
    plt.figure(figsize=(14, 5))
    sns.lineplot(data=daily, x="date", y="messages", marker="o", linewidth=2)
    plt.title("每日消息量趋势")
    plt.xlabel("日期")
    plt.ylabel("消息数")
    savefig("02_daily_trend.png")

    # 3. Monthly trend.
    monthly = df.groupby(df["datetime"].dt.to_period("M").astype(str)).size().reset_index(name="messages")
    plt.figure(figsize=(13, 5))
    sns.barplot(data=monthly, x="datetime", y="messages", color="#4C78A8")
    plt.title("月度消息量")
    plt.xlabel("月份")
    plt.ylabel("消息数")
    plt.xticks(rotation=45, ha="right")
    savefig("03_monthly_volume.png")

    # 4. Type distribution.
    types = df["type"].value_counts().reset_index()
    types.columns = ["type", "messages"]
    plt.figure(figsize=(10, 5))
    sns.barplot(data=types, y="type", x="messages", palette="viridis", hue="type", legend=False)
    plt.title("消息类型分布")
    plt.xlabel("消息数")
    plt.ylabel("")
    savefig("04_type_distribution.png")

    # 5. Hour x weekday heatmap.
    heat = df.pivot_table(index="weekday_cn", columns="hour", values="local_id", aggfunc="count", fill_value=0)
    heat = heat.reindex(["周一", "周二", "周三", "周四", "周五", "周六", "周日"])
    plt.figure(figsize=(15, 5))
    sns.heatmap(heat, cmap="YlOrRd", linewidths=.4, linecolor="white")
    plt.title("星期 x 小时 活跃热力图")
    plt.xlabel("小时")
    plt.ylabel("")
    savefig("05_weekday_hour_heatmap.png")

    # 6. Hourly distribution.
    hourly = df.groupby("hour").size().reindex(range(24), fill_value=0).reset_index(name="messages")
    plt.figure(figsize=(12, 5))
    sns.barplot(data=hourly, x="hour", y="messages", color="#F58518")
    plt.title("24小时消息分布")
    plt.xlabel("小时")
    plt.ylabel("消息数")
    savefig("06_hourly_distribution.png")

    # 7. Top senders.
    sender_df = df[df["sender_username"].ne("")]
    top_senders = sender_df.groupby(["sender_username", "sender"]).size().reset_index(name="messages")
    top_senders = top_senders.sort_values("messages", ascending=False).head(20)
    top_senders = top_senders[["sender", "messages"]]
    top_senders.columns = ["sender", "messages"]
    plt.figure(figsize=(11, 8))
    sns.barplot(data=top_senders, y="sender", x="messages", color="#54A24B")
    plt.title("发言者排行 Top 20")
    plt.xlabel("消息数")
    plt.ylabel("")
    savefig("07_top_senders.png")

    # 8. Sender concentration.
    sender_counts = sender_df.groupby("sender_username").size().sort_values(ascending=False)
    cumulative = sender_counts.cumsum() / sender_counts.sum() * 100
    plt.figure(figsize=(11, 5))
    sns.lineplot(x=range(1, len(cumulative) + 1), y=cumulative.values, marker="o")
    plt.axhline(80, color="red", linestyle="--", linewidth=1)
    plt.title("发言集中度：累计贡献曲线")
    plt.xlabel("按发言量排序的成员数")
    plt.ylabel("累计消息占比 (%)")
    savefig("08_sender_concentration.png")

    # 9. Text length distribution.
    text_df = df[df["text_len"] > 0].copy()
    plt.figure(figsize=(12, 5))
    sns.histplot(text_df["text_len"].clip(upper=text_df["text_len"].quantile(0.99)), bins=40, color="#B279A2")
    plt.title("文本长度分布（截断至99分位）")
    plt.xlabel("字符数")
    plt.ylabel("消息数")
    savefig("09_text_length_distribution.png")

    # 10. Word frequency.
    freq = tokenize(df.loc[df["is_text"], "text"])
    word_rows = pd.DataFrame(freq.most_common(30), columns=["word", "count"])
    word_rows.to_csv(OUT / "word_frequency.csv", index=False, encoding="utf-8-sig")
    if not word_rows.empty:
        plt.figure(figsize=(10, 8))
        sns.barplot(data=word_rows.head(25), y="word", x="count", color="#E45756")
        plt.title("文本高频词 Top 25")
        plt.xlabel("出现次数")
        plt.ylabel("")
        savefig("10_word_frequency.png")

        font_path = next(
            (p for p in [
                r"C:\Windows\Fonts\NotoSansSC-VF.ttf",
                r"C:\Windows\Fonts\msyh.ttc",
                r"C:\Windows\Fonts\simhei.ttf",
            ] if Path(p).exists()),
            None,
        )
        wc = WordCloud(
            width=1400,
            height=800,
            background_color="white",
            font_path=font_path,
            colormap="viridis",
            max_words=160,
            random_state=42,
        ).generate_from_frequencies(dict(freq))
        plt.figure(figsize=(14, 8))
        plt.imshow(wc, interpolation="bilinear")
        plt.axis("off")
        plt.title("文本词云")
        savefig("12_wordcloud.png")

    # 11. Type over month stacked.
    type_month = df.groupby([df["datetime"].dt.to_period("M").astype(str), "type"]).size().unstack(fill_value=0)
    ax = type_month.plot(kind="bar", stacked=True, figsize=(14, 6), colormap="tab20")
    ax.set_title("月度消息类型构成")
    ax.set_xlabel("月份")
    ax.set_ylabel("消息数")
    plt.xticks(rotation=45, ha="right")
    savefig("11_monthly_type_stack.png")

    top_day = daily.sort_values("messages", ascending=False).head(10)
    top_day.to_csv(OUT / "top_days.csv", index=False, encoding="utf-8-sig")
    top_senders.to_csv(OUT / "top_senders.csv", index=False, encoding="utf-8-sig")
    types.to_csv(OUT / "type_distribution.csv", index=False, encoding="utf-8-sig")

    report = [
        f"# {meta['chat']} 群聊统计分析",
        "",
        f"- 群ID: `{meta['username']}`",
        f"- 数据范围: {summary['首条时间']} 至 {summary['末条时间']}",
        f"- 消息总数: {summary['消息总数']:,}",
        f"- 参与发言人数: {summary['参与发言人数']:,}",
        f"- 时间跨度: {summary['跨度天数']} 天，其中 {summary['有消息天数']} 天有消息",
        f"- 活跃日均消息: {summary['活跃日均消息']}",
        f"- 文本消息占比: {summary['文本消息占比']}%",
        f"- 平均文本长度: {summary['平均文本长度']} 字符",
        "",
        "## 最活跃日期 Top 10",
        "",
        markdown_table(top_day[["date", "messages"]]),
        "",
        "## 发言者 Top 20",
        "",
        markdown_table(top_senders),
        "",
        "## 消息类型",
        "",
        markdown_table(types),
        "",
        "## 图表文件",
        "",
    ]
    for png in sorted(OUT.glob("*.png")):
        report.append(f"- `{png.name}`")
    (OUT / "report.md").write_text("\n".join(report), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
