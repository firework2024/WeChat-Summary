import json
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from matplotlib import font_manager

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "analysis" / "private_chats"
OUT = ROOT / "analysis" / "private_chats_top100_gender"
MSG_CSV = SRC / "private_messages.csv"
GENDER_CSV = SRC / "gender_labels.csv"
TOP_N = 100


def setup_style():
    for path in [
        r"C:\Windows\Fonts\NotoSansSC-VF.ttf",
        r"C:\Windows\Fonts\msyh.ttc",
        r"C:\Windows\Fonts\simhei.ttf",
    ]:
        if Path(path).exists():
            font_manager.fontManager.addfont(path)
            name = font_manager.FontProperties(fname=path).get_name()
            plt.rcParams["font.sans-serif"] = [name]
            break
    plt.rcParams["axes.unicode_minus"] = False
    sns.set_theme(style="whitegrid", font=plt.rcParams["font.sans-serif"][0])


def normalize_gender(value):
    value = str(value or "").strip().lower()
    if value in {"男", "male", "m"}:
        return "男"
    if value in {"女", "female", "f"}:
        return "女"
    return "unknown"


def savefig(name):
    plt.tight_layout()
    plt.savefig(OUT / name, dpi=180, bbox_inches="tight")
    plt.close()


def month_range(df):
    start = df["datetime"].min().to_period("M")
    end = df["datetime"].max().to_period("M")
    return pd.period_range(start, end, freq="M").astype(str)


def load_top100():
    df = pd.read_csv(MSG_CSV, encoding="utf-8-sig")
    df["datetime"] = pd.to_datetime(df["datetime"])
    df["month"] = df["datetime"].dt.to_period("M").astype(str)
    df["chat_name"] = df["chat_name"].fillna(df["chat_username"])

    genders = pd.read_csv(GENDER_CSV, encoding="utf-8-sig")
    genders["gender"] = genders["gender"].map(normalize_gender)
    gender_map = genders.set_index("username")["gender"].to_dict()
    source_map = genders.set_index("username")["gender_source"].fillna("").to_dict()

    top = (
        df.groupby(["chat_username", "chat_name"]).size()
        .reset_index(name="messages")
        .sort_values("messages", ascending=False)
        .head(TOP_N)
        .copy()
    )
    top["rank"] = range(1, len(top) + 1)
    top["gender"] = top["chat_username"].map(gender_map).fillna("unknown")
    top["gender_source"] = top["chat_username"].map(source_map).fillna("missing")

    top_df = df[df["chat_username"].isin(top["chat_username"])].copy()
    top_df["gender"] = top_df["chat_username"].map(gender_map).fillna("unknown")
    return df, top_df, top


def plot_summary(top_df, top):
    order = ["男", "女", "unknown"]
    contact_summary = (
        top.groupby("gender").agg(contacts=("chat_username", "nunique"), messages=("messages", "sum"))
        .reindex(order, fill_value=0)
        .reset_index()
    )
    contact_summary.to_csv(OUT / "top100_gender_summary.csv", index=False, encoding="utf-8-sig")

    fig, axes = plt.subplots(1, 2, figsize=(13, 5))
    sns.barplot(data=contact_summary, x="gender", y="contacts", ax=axes[0], color="#4C78A8")
    axes[0].set_title(f"Top {TOP_N} 私聊对象性别人数")
    axes[0].set_xlabel("性别")
    axes[0].set_ylabel("人数")

    sns.barplot(data=contact_summary, x="gender", y="messages", ax=axes[1], color="#F58518")
    axes[1].set_title(f"Top {TOP_N} 私聊对象消息量按性别")
    axes[1].set_xlabel("性别")
    axes[1].set_ylabel("消息数")
    savefig("00_top100_gender_summary.png")

    plt.figure(figsize=(13, 5))
    sns.histplot(data=top, x="rank", hue="gender", multiple="stack", bins=20)
    plt.title(f"Top {TOP_N} 排名段中的性别构成")
    plt.xlabel("按聊天量排序的排名")
    plt.ylabel("人数")
    savefig("01_rank_segment_gender.png")


def plot_timeseries(top_df):
    months = month_range(top_df)
    order = ["男", "女", "unknown"]

    monthly_messages = (
        top_df.groupby(["month", "gender"]).size().unstack(fill_value=0)
        .reindex(months, fill_value=0)
        .reindex(columns=order, fill_value=0)
    )
    monthly_contacts = (
        top_df.groupby(["month", "gender"])["chat_username"].nunique().unstack(fill_value=0)
        .reindex(months, fill_value=0)
        .reindex(columns=order, fill_value=0)
    )
    monthly_msg_share = monthly_messages.div(monthly_messages.sum(axis=1).replace(0, pd.NA), axis=0) * 100
    monthly_contact_share = monthly_contacts.div(monthly_contacts.sum(axis=1).replace(0, pd.NA), axis=0) * 100

    monthly_messages.to_csv(OUT / "monthly_messages_by_gender.csv", encoding="utf-8-sig")
    monthly_contacts.to_csv(OUT / "monthly_active_contacts_by_gender.csv", encoding="utf-8-sig")
    monthly_msg_share.to_csv(OUT / "monthly_message_share_by_gender.csv", encoding="utf-8-sig")
    monthly_contact_share.to_csv(OUT / "monthly_contact_share_by_gender.csv", encoding="utf-8-sig")

    ax = monthly_messages.plot(figsize=(16, 6), marker="o", linewidth=2)
    ax.set_title(f"Top {TOP_N} 私聊对象：月度消息量按性别")
    ax.set_xlabel("月份")
    ax.set_ylabel("消息数")
    plt.xticks(rotation=60)
    savefig("02_monthly_messages_by_gender.png")

    ax = monthly_contacts.plot(figsize=(16, 6), marker="o", linewidth=2)
    ax.set_title(f"Top {TOP_N} 私聊对象：月度活跃对象数按性别")
    ax.set_xlabel("月份")
    ax.set_ylabel("人数")
    plt.xticks(rotation=60)
    savefig("03_monthly_active_contacts_by_gender.png")

    ax = monthly_msg_share.plot(figsize=(16, 6), marker="o", linewidth=2)
    ax.set_title(f"Top {TOP_N} 私聊对象：月度消息量性别占比")
    ax.set_xlabel("月份")
    ax.set_ylabel("占比 (%)")
    plt.xticks(rotation=60)
    savefig("04_monthly_message_share_by_gender.png")

    ax = monthly_contact_share.plot(figsize=(16, 6), marker="o", linewidth=2)
    ax.set_title(f"Top {TOP_N} 私聊对象：月度活跃对象性别占比")
    ax.set_xlabel("月份")
    ax.set_ylabel("占比 (%)")
    plt.xticks(rotation=60)
    savefig("05_monthly_contact_share_by_gender.png")

    fig, axes = plt.subplots(2, 1, figsize=(16, 10), sharex=True)
    monthly_messages.plot.area(ax=axes[0], alpha=0.75)
    axes[0].set_title("月度消息量性别构成（堆叠）")
    axes[0].set_ylabel("消息数")
    monthly_contacts.plot.area(ax=axes[1], alpha=0.75)
    axes[1].set_title("月度活跃对象性别构成（堆叠）")
    axes[1].set_ylabel("人数")
    axes[1].set_xlabel("月份")
    plt.xticks(rotation=60)
    savefig("06_monthly_gender_stacked_area.png")


def plot_top_users(top):
    plt.figure(figsize=(12, 14))
    sns.barplot(data=top.sort_values("messages", ascending=True), y="chat_name", x="messages", hue="gender", dodge=False)
    plt.title(f"Top {TOP_N} 私聊对象聊天量与性别")
    plt.xlabel("消息数")
    plt.ylabel("")
    savefig("07_top100_users_gender_bar.png")


def write_report(full_df, top_df, top):
    summary = pd.read_csv(OUT / "top100_gender_summary.csv", encoding="utf-8-sig")
    top_preview = top[["rank", "chat_name", "messages", "gender", "gender_source"]].head(30)
    lines = [
        f"# Top {TOP_N} 私聊对象性别比例分析",
        "",
        f"- 口径: 按一对一私聊消息量排序，只保留 Top {TOP_N} 聊天对象。",
        f"- 全量私聊对象数: {full_df['chat_username'].nunique():,}",
        f"- Top {TOP_N} 对象消息数: {len(top_df):,}",
        f"- Top {TOP_N} 覆盖全量私聊消息占比: {len(top_df) / len(full_df) * 100:.2f}%",
        f"- 数据范围: {top_df['datetime'].min()} 至 {top_df['datetime'].max()}",
        "",
        "## 性别汇总",
        "",
        summary.to_markdown(index=False),
        "",
        "## Top 30 预览",
        "",
        top_preview.to_markdown(index=False),
        "",
        "## 图表",
        "",
    ]
    for png in sorted(OUT.glob("*.png")):
        lines.append(f"- `{png.name}`")
    (OUT / "report.md").write_text("\n".join(lines), encoding="utf-8")


def main():
    setup_style()
    OUT.mkdir(parents=True, exist_ok=True)
    full_df, top_df, top = load_top100()
    top.to_csv(OUT / "top100_private_users_with_gender.csv", index=False, encoding="utf-8-sig")
    plot_summary(top_df, top)
    plot_timeseries(top_df)
    plot_top_users(top)
    write_report(full_df, top_df, top)
    result = {
        "top_n": TOP_N,
        "full_private_contacts": int(full_df["chat_username"].nunique()),
        "top_contacts": int(top["chat_username"].nunique()),
        "full_messages": int(len(full_df)),
        "top_messages": int(len(top_df)),
        "coverage_pct": round(len(top_df) / len(full_df) * 100, 2),
        "output_dir": str(OUT),
    }
    (OUT / "summary.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
