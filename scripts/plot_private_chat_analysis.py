import json
import re
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from matplotlib import font_manager

ROOT = Path(__file__).resolve().parents[1]
BASE = ROOT / "analysis" / "private_chats"
MSG_CSV = BASE / "private_messages.csv"
GENDER_CSV = BASE / "gender_labels.csv"
OUT = BASE / "figures"
TOP20 = OUT / "top20_users"

SESSION_GAP_MINUTES = 60


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


def safe_name(name):
    return re.sub(r'[\\/:*?"<>|]+', "_", str(name))[:90]


def normalize_gender(value):
    value = str(value or "").strip().lower()
    if value in {"男", "male", "m", "man", "boy"}:
        return "男"
    if value in {"女", "female", "f", "woman", "girl"}:
        return "女"
    return "unknown"


def month_range(df):
    start = df["datetime"].min().to_period("M")
    end = df["datetime"].max().to_period("M")
    return pd.period_range(start, end, freq="M").astype(str)


def savefig(path):
    plt.tight_layout()
    plt.savefig(path, dpi=180, bbox_inches="tight")
    plt.close()


def load_data():
    df = pd.read_csv(MSG_CSV, encoding="utf-8-sig")
    df["datetime"] = pd.to_datetime(df["datetime"])
    df["date"] = pd.to_datetime(df["date"])
    df["month"] = df["datetime"].dt.to_period("M").astype(str)
    df["chat_name"] = df["chat_name"].fillna(df["chat_username"])

    genders = pd.read_csv(GENDER_CSV, encoding="utf-8-sig")
    genders["gender"] = genders["gender"].map(normalize_gender)
    gender_map = genders.set_index("username")["gender"].to_dict()
    df["gender"] = df["chat_username"].map(gender_map).fillna("unknown")
    return df, genders


def build_sessions(df):
    rows = []
    for username, part in df.sort_values("datetime").groupby("chat_username"):
        last_time = None
        for row in part.itertuples(index=False):
            if last_time is None:
                is_start = True
            else:
                gap = (row.datetime - last_time).total_seconds() / 60
                is_start = gap > SESSION_GAP_MINUTES
            if is_start:
                rows.append({
                    "chat_username": username,
                    "chat_name": row.chat_name,
                    "month": row.month,
                    "datetime": row.datetime,
                    "initiator": row.sender_role if row.sender_role in {"me", "contact"} else "unknown",
                    "gender": row.gender,
                })
            last_time = row.datetime
    return pd.DataFrame(rows)


def plot_overview(df, sessions):
    OUT.mkdir(parents=True, exist_ok=True)
    months = month_range(df)
    monthly_messages = df.groupby("month").size().reindex(months, fill_value=0).reset_index(name="messages")
    monthly_contacts = df.groupby("month")["chat_username"].nunique().reindex(months, fill_value=0).reset_index(name="contacts")

    fig, axes = plt.subplots(2, 2, figsize=(16, 10))
    fig.suptitle("私聊整体趋势", fontsize=20)
    sns.lineplot(data=monthly_messages, x="index", y="messages", marker="o", ax=axes[0, 0])
    axes[0, 0].set_title("月度私聊消息量")
    axes[0, 0].set_xlabel("月份")
    axes[0, 0].set_ylabel("消息数")
    axes[0, 0].tick_params(axis="x", rotation=60)

    sns.lineplot(data=monthly_contacts, x="index", y="contacts", marker="o", color="#54A24B", ax=axes[0, 1])
    axes[0, 1].set_title("月度活跃私聊对象数")
    axes[0, 1].set_xlabel("月份")
    axes[0, 1].set_ylabel("人数")
    axes[0, 1].tick_params(axis="x", rotation=60)

    type_counts = df["type"].value_counts().head(12).reset_index()
    type_counts.columns = ["type", "messages"]
    sns.barplot(data=type_counts, y="type", x="messages", ax=axes[1, 0], color="#4C78A8")
    axes[1, 0].set_title("私聊消息类型分布")
    axes[1, 0].set_xlabel("消息数")
    axes[1, 0].set_ylabel("")

    init_counts = sessions["initiator"].value_counts().reset_index()
    init_counts.columns = ["initiator", "sessions"]
    sns.barplot(data=init_counts, x="initiator", y="sessions", ax=axes[1, 1], color="#F58518")
    axes[1, 1].set_title(f"主动发起对话次数（沉默>{SESSION_GAP_MINUTES}分钟）")
    axes[1, 1].set_xlabel("")
    axes[1, 1].set_ylabel("次数")
    savefig(OUT / "00_private_overview.png")


def plot_gender(df, sessions):
    months = month_range(df)
    gender_order = ["男", "女", "unknown"]

    msg_gender = (
        df.groupby(["month", "gender"]).size().unstack(fill_value=0)
        .reindex(months, fill_value=0)
        .reindex(columns=gender_order, fill_value=0)
    )
    contact_gender = (
        df.groupby(["month", "gender"])["chat_username"].nunique().unstack(fill_value=0)
        .reindex(months, fill_value=0)
        .reindex(columns=gender_order, fill_value=0)
    )
    init_gender = (
        sessions[sessions["initiator"].eq("contact")]
        .groupby(["month", "gender"]).size().unstack(fill_value=0)
        .reindex(months, fill_value=0)
        .reindex(columns=gender_order, fill_value=0)
    )

    fig, axes = plt.subplots(3, 1, figsize=(16, 14), sharex=True)
    msg_gender.plot(ax=axes[0], linewidth=2)
    axes[0].set_title("按性别分组的月度私聊消息量")
    axes[0].set_ylabel("消息数")

    contact_gender.plot(ax=axes[1], linewidth=2)
    axes[1].set_title("按性别分组的月度活跃私聊对象数")
    axes[1].set_ylabel("人数")

    init_gender.plot(ax=axes[2], linewidth=2)
    axes[2].set_title("对方主动发起对话次数（月度，按性别）")
    axes[2].set_ylabel("次数")
    axes[2].set_xlabel("月份")
    axes[2].tick_params(axis="x", rotation=60)
    savefig(OUT / "01_gender_timeseries.png")

    ratio = pd.DataFrame({
        "gender": gender_order,
        "contacts": [
            df.loc[df["gender"].eq(g), "chat_username"].nunique()
            for g in gender_order
        ],
        "messages": [
            int(df["gender"].eq(g).sum())
            for g in gender_order
        ],
    })
    ratio.to_csv(BASE / "gender_ratio_summary.csv", index=False, encoding="utf-8-sig")

    plt.figure(figsize=(10, 5))
    sns.barplot(data=ratio, x="gender", y="contacts", color="#B279A2")
    plt.title("私聊对象性别比例（基于 gender_labels.csv）")
    plt.xlabel("性别")
    plt.ylabel("私聊对象数")
    savefig(OUT / "02_gender_contact_ratio.png")


def top_users(df, n=20):
    users = (
        df.groupby(["chat_username", "chat_name"]).size()
        .reset_index(name="messages")
        .sort_values("messages", ascending=False)
        .head(n)
    )
    users.to_csv(BASE / f"top{n}_private_users.csv", index=False, encoding="utf-8-sig")
    return users


def plot_top10_compare(df, users):
    top10 = users.head(10)
    months = month_range(df)
    plt.figure(figsize=(17, 8))
    for row in top10.itertuples(index=False):
        part = df[df["chat_username"].eq(row.chat_username)]
        monthly = part.groupby("month").size().reindex(months, fill_value=0)
        plt.plot(months, monthly.values, marker="o", linewidth=2, label=row.chat_name)
    plt.title("Top 10 私聊对象月度聊天量趋势对比")
    plt.xlabel("月份")
    plt.ylabel("消息数")
    plt.xticks(rotation=60)
    plt.legend(loc="upper left", bbox_to_anchor=(1.01, 1))
    savefig(OUT / "03_top10_message_trend_compare.png")


def plot_user_profile(df, sessions, username, display_name):
    TOP20.mkdir(parents=True, exist_ok=True)
    months = month_range(df)
    part = df[df["chat_username"].eq(username)].copy()
    user_sessions = sessions[sessions["chat_username"].eq(username)].copy()

    fig, axes = plt.subplots(3, 1, figsize=(16, 15), sharex=True)
    fig.suptitle(f"{display_name} 私聊时序统计", fontsize=20)

    monthly = part.groupby("month").size().reindex(months, fill_value=0).reset_index(name="messages")
    sns.lineplot(data=monthly, x="index", y="messages", marker="o", ax=axes[0], color="#4C78A8")
    axes[0].set_title("聊天量（月度）")
    axes[0].set_ylabel("消息数")
    axes[0].set_xlabel("")

    type_month = (
        part.groupby(["month", "type"]).size().unstack(fill_value=0)
        .reindex(months, fill_value=0)
    )
    top_types = part["type"].value_counts().head(8).index.tolist()
    type_month = type_month.reindex(columns=top_types, fill_value=0)
    type_month.plot(kind="bar", stacked=True, ax=axes[1], colormap="tab20")
    axes[1].set_title("各类信息量（月度堆叠）")
    axes[1].set_ylabel("消息数")
    axes[1].set_xlabel("")
    axes[1].legend(loc="upper left", bbox_to_anchor=(1.01, 1))

    init_month = (
        user_sessions.groupby(["month", "initiator"]).size().unstack(fill_value=0)
        .reindex(months, fill_value=0)
        .reindex(columns=["me", "contact", "unknown"], fill_value=0)
    )
    init_month.plot(ax=axes[2], linewidth=2, marker="o")
    axes[2].set_title(f"主动发起对话次数（月度，沉默>{SESSION_GAP_MINUTES}分钟）")
    axes[2].set_ylabel("次数")
    axes[2].set_xlabel("月份")
    axes[2].tick_params(axis="x", rotation=60)
    axes[2].legend(["我发起", "对方发起", "未知"], loc="upper left")

    savefig(TOP20 / f"{safe_name(display_name)}.png")


def write_report(df, sessions, users):
    gender_counts = pd.read_csv(BASE / "gender_ratio_summary.csv", encoding="utf-8-sig")
    first_time = df["datetime"].min().strftime("%Y-%m-%d %H:%M:%S")
    last_time = df["datetime"].max().strftime("%Y-%m-%d %H:%M:%S")
    lines = [
        "# 私聊统计分析",
        "",
        f"- 数据范围: {first_time} 至 {last_time}",
        f"- 私聊消息总数: {len(df):,}",
        f"- 私聊对象数: {df['chat_username'].nunique():,}",
        f"- 主动发起对话定义: 私聊沉默超过 {SESSION_GAP_MINUTES} 分钟后的第一条消息",
        "- 性别统计来源: `gender_labels.csv`。默认由 `contact.extra_buffer` 的 protobuf field 2 解析得到，`1=男`、`2=女`、`0/缺失=unknown`；手工修改会被保留。",
        "",
        "## 性别汇总",
        "",
        gender_counts.to_markdown(index=False),
        "",
        "## Top 20 私聊对象",
        "",
        users.to_markdown(index=False),
        "",
        "## 图表",
        "",
        "- `figures/00_private_overview.png`",
        "- `figures/01_gender_timeseries.png`",
        "- `figures/02_gender_contact_ratio.png`",
        "- `figures/03_top10_message_trend_compare.png`",
        "- `figures/top20_users/*.png`",
    ]
    (BASE / "report.md").write_text("\n".join(lines), encoding="utf-8")


def main():
    setup_style()
    OUT.mkdir(parents=True, exist_ok=True)
    TOP20.mkdir(parents=True, exist_ok=True)
    df, genders = load_data()
    sessions = build_sessions(df)
    sessions.to_csv(BASE / "conversation_starts.csv", index=False, encoding="utf-8-sig")

    plot_overview(df, sessions)
    plot_gender(df, sessions)
    users = top_users(df, 20)
    plot_top10_compare(df, users)
    for row in users.itertuples(index=False):
        plot_user_profile(df, sessions, row.chat_username, row.chat_name)
    write_report(df, sessions, users)

    summary = {
        "message_count": int(len(df)),
        "private_contact_count": int(df["chat_username"].nunique()),
        "top20_count": int(len(users)),
        "figure_dir": str(OUT),
        "gender_unknown_contacts": int((genders["gender"].map(normalize_gender) == "unknown").sum()),
        "gender_known_contacts": int((genders["gender"].map(normalize_gender) != "unknown").sum()),
    }
    (BASE / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
