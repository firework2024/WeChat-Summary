import json
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from matplotlib import font_manager

ROOT = Path(__file__).resolve().parents[1]
BASE = ROOT / "analysis" / "private_chats"
SCAN = BASE / "laughter_scan"
HITS_CSV = SCAN / "laughter_hits.csv"
GENDER_CSV = BASE / "gender_labels.csv"
OUT = SCAN / "figures"

H_BUCKETS = [f"h{i}" for i in range(2, 8)] + ["h8+"]
HA_BUCKETS = ["哈哈", "哈哈哈", "哈哈哈哈", "哈哈哈哈哈", "哈哈哈哈哈哈", "哈哈哈哈哈哈哈", "哈8+"]
BUCKET_ORDER = H_BUCKETS + HA_BUCKETS


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


def load_hits():
    df = pd.read_csv(HITS_CSV, encoding="utf-8-sig")
    if df.empty:
        return df
    df["datetime"] = pd.to_datetime(df["datetime"])
    df["date"] = pd.to_datetime(df["date"])
    df["month"] = df["datetime"].dt.to_period("M").astype(str)
    df["bucket"] = pd.Categorical(df["bucket"], categories=BUCKET_ORDER, ordered=True)
    genders = pd.read_csv(GENDER_CSV, encoding="utf-8-sig")
    genders["gender"] = genders["gender"].map(normalize_gender)
    gender_map = genders.set_index("username")["gender"].to_dict()
    df["gender"] = df["chat_username"].map(gender_map).fillna("unknown")
    return df


def month_range(df):
    start = df["datetime"].min().to_period("M")
    end = df["datetime"].max().to_period("M")
    return pd.period_range(start, end, freq="M").astype(str)


def plot_length_distribution(df):
    counts = df["bucket"].value_counts().reindex(BUCKET_ORDER, fill_value=0).reset_index()
    counts.columns = ["bucket", "count"]
    plt.figure(figsize=(13, 5))
    sns.barplot(data=counts, x="bucket", y="count", hue="bucket", legend=False, palette="viridis")
    plt.title("哈哈哈 / hh 长度分布")
    plt.xlabel("连续长度类别")
    plt.ylabel("出现次数")
    savefig("00_length_distribution.png")

    kind_counts = df["kind"].value_counts().reset_index()
    kind_counts.columns = ["kind", "count"]
    plt.figure(figsize=(7, 5))
    sns.barplot(data=kind_counts, x="kind", y="count", hue="kind", legend=False, palette="Set2")
    plt.title("中文笑声 vs h笑声 总量")
    plt.xlabel("类型")
    plt.ylabel("出现次数")
    savefig("01_kind_counts.png")


def plot_role_gender(df):
    role = df.groupby(["bucket", "sender_role"], observed=False).size().reset_index(name="count")
    plt.figure(figsize=(15, 6))
    sns.barplot(data=role, x="bucket", y="count", hue="sender_role")
    plt.title("不同长度笑声：我 vs 对方")
    plt.xlabel("长度类别")
    plt.ylabel("出现次数")
    plt.xticks(rotation=35, ha="right")
    savefig("02_bucket_by_sender_role.png")

    gender = df.groupby(["bucket", "gender"], observed=False).size().reset_index(name="count")
    plt.figure(figsize=(15, 6))
    sns.barplot(data=gender, x="bucket", y="count", hue="gender")
    plt.title("不同长度笑声：按私聊对象性别")
    plt.xlabel("长度类别")
    plt.ylabel("出现次数")
    plt.xticks(rotation=35, ha="right")
    savefig("03_bucket_by_gender.png")


def plot_timeseries(df):
    months = month_range(df)
    kind_month = (
        df.groupby(["month", "kind"]).size().unstack(fill_value=0)
        .reindex(months, fill_value=0)
    )
    ax = kind_month.plot(figsize=(16, 6), marker="o", linewidth=2)
    ax.set_title("哈哈哈 / hh 月度趋势")
    ax.set_xlabel("月份")
    ax.set_ylabel("出现次数")
    plt.xticks(rotation=60)
    savefig("04_kind_monthly_trend.png")

    top_buckets = df["bucket"].value_counts().head(8).index.tolist()
    bucket_month = (
        df[df["bucket"].isin(top_buckets)]
        .groupby(["month", "bucket"], observed=False).size().unstack(fill_value=0)
        .reindex(months, fill_value=0)
        .reindex(columns=top_buckets, fill_value=0)
    )
    ax = bucket_month.plot(figsize=(16, 7), marker="o", linewidth=2)
    ax.set_title("主要长度类别月度趋势")
    ax.set_xlabel("月份")
    ax.set_ylabel("出现次数")
    plt.xticks(rotation=60)
    savefig("05_bucket_monthly_trend.png")


def plot_heatmaps(df):
    hour_heat = (
        df.pivot_table(index="bucket", columns="hour", values="local_id", aggfunc="count", fill_value=0, observed=False)
        .reindex(BUCKET_ORDER)
    )
    plt.figure(figsize=(15, 7))
    sns.heatmap(hour_heat, cmap="YlOrRd", linewidths=.3, linecolor="white")
    plt.title("长度类别 x 小时热力图")
    plt.xlabel("小时")
    plt.ylabel("长度类别")
    savefig("06_bucket_hour_heatmap.png")

    weekday_cn = {0: "周一", 1: "周二", 2: "周三", 3: "周四", 4: "周五", 5: "周六", 6: "周日"}
    df = df.copy()
    df["weekday_cn"] = df["weekday"].map(weekday_cn)
    week_heat = (
        df.pivot_table(index="weekday_cn", columns="kind", values="local_id", aggfunc="count", fill_value=0)
        .reindex(["周一", "周二", "周三", "周四", "周五", "周六", "周日"])
    )
    plt.figure(figsize=(8, 6))
    sns.heatmap(week_heat, cmap="GnBu", annot=True, fmt=".0f", linewidths=.3, linecolor="white")
    plt.title("星期 x 笑声类型热力图")
    plt.xlabel("类型")
    plt.ylabel("")
    savefig("07_kind_weekday_heatmap.png")


def plot_top_chats(df):
    top = (
        df.groupby(["chat_username", "chat_name"]).size()
        .reset_index(name="count")
        .sort_values("count", ascending=False)
        .head(40)
    )
    top.to_csv(SCAN / "top_laughter_chats.csv", index=False, encoding="utf-8-sig")

    plt.figure(figsize=(11, 10))
    sns.barplot(data=top.head(30), y="chat_name", x="count", color="#54A24B")
    plt.title("哈哈哈 / hh 出现最多的一对一聊天 Top 30")
    plt.xlabel("出现次数")
    plt.ylabel("")
    savefig("08_top_laughter_chats.png")

    top20 = top.head(20)["chat_username"].tolist()
    matrix = (
        df[df["chat_username"].isin(top20)]
        .pivot_table(index="chat_name", columns="bucket", values="local_id", aggfunc="count", fill_value=0, observed=False)
        .reindex(columns=BUCKET_ORDER, fill_value=0)
    )
    order = matrix.sum(axis=1).sort_values(ascending=False).index
    matrix = matrix.reindex(order)
    plt.figure(figsize=(14, 9))
    sns.heatmap(matrix, cmap="YlGnBu", linewidths=.3, linecolor="white")
    plt.title("Top 20 私聊对象：不同长度笑声矩阵")
    plt.xlabel("长度类别")
    plt.ylabel("")
    savefig("09_top_chats_bucket_matrix.png")

    freq = (
        df.groupby(["chat_username", "chat_name"])
        .agg(laughter_count=("local_id", "count"), active_messages=("local_id", "count"))
        .reset_index()
    )
    # active_messages here is hit rows only; keep the filename explicit to avoid
    # implying it is normalized by all messages.
    freq.sort_values("laughter_count", ascending=False).to_csv(
        SCAN / "laughter_chat_counts.csv", index=False, encoding="utf-8-sig"
    )


def write_report(df):
    totals = df["bucket"].value_counts().reindex(BUCKET_ORDER, fill_value=0).reset_index()
    totals.columns = ["bucket", "count"]
    kind_totals = df["kind"].value_counts().reset_index()
    kind_totals.columns = ["kind", "count"]
    role_totals = df.groupby("sender_role").size().reset_index(name="count")
    gender_totals = df.groupby("gender").size().reset_index(name="count")
    top_chats = pd.read_csv(SCAN / "top_laughter_chats.csv", encoding="utf-8-sig").head(20)
    lines = [
        "# 私聊哈哈哈 / hh 专题统计",
        "",
        f"- 命中行数: {len(df):,}",
        f"- 覆盖私聊对象: {df['chat_username'].nunique():,}",
        f"- 时间范围: {df['datetime'].min()} 至 {df['datetime'].max()}",
        "- 口径: 连续 `h` 至少 2 个，且不嵌在英文单词里；连续 `哈` 至少 2 个。",
        "",
        "## 类型总量",
        "",
        kind_totals.to_markdown(index=False),
        "",
        "## 长度分布",
        "",
        totals.to_markdown(index=False),
        "",
        "## 我 vs 对方",
        "",
        role_totals.to_markdown(index=False),
        "",
        "## 按私聊对象性别",
        "",
        gender_totals.to_markdown(index=False),
        "",
        "## Top 私聊对象",
        "",
        top_chats.to_markdown(index=False),
        "",
        "## 图表",
        "",
    ]
    for png in sorted(OUT.glob("*.png")):
        lines.append(f"- `figures/{png.name}`")
    (SCAN / "report.md").write_text("\n".join(lines), encoding="utf-8")


def main():
    setup_style()
    OUT.mkdir(parents=True, exist_ok=True)
    df = load_hits()
    if df.empty:
        print("No laughter hits found.")
        return
    plot_length_distribution(df)
    plot_role_gender(df)
    plot_timeseries(df)
    plot_heatmaps(df)
    plot_top_chats(df)
    write_report(df)
    summary = {
        "hit_rows": int(len(df)),
        "private_contacts_with_hits": int(df["chat_username"].nunique()),
        "figure_dir": str(OUT),
    }
    (SCAN / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
