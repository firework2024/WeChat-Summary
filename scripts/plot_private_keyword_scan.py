import json
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from matplotlib import font_manager

ROOT = Path(__file__).resolve().parents[1]
BASE = ROOT / "analysis" / "private_chats"
SCAN = BASE / "keyword_scan"
HITS_CSV = SCAN / "keyword_hits.csv"
GENDER_CSV = BASE / "gender_labels.csv"
OUT = SCAN / "figures"

KEYWORD_ORDER = ["哎", "笑死", "笑死了", "笑死我了", "xswl", "xs"]


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
    df["keyword"] = pd.Categorical(df["keyword"], categories=KEYWORD_ORDER, ordered=True)
    genders = pd.read_csv(GENDER_CSV, encoding="utf-8-sig")
    genders["gender"] = genders["gender"].map(normalize_gender)
    gender_map = genders.set_index("username")["gender"].to_dict()
    df["gender"] = df["chat_username"].map(gender_map).fillna("unknown")
    return df


def month_range(df):
    start = df["datetime"].min().to_period("M")
    end = df["datetime"].max().to_period("M")
    return pd.period_range(start, end, freq="M").astype(str)


def plot_overview(df):
    counts = df.groupby("keyword", observed=False)["count"].sum().reindex(KEYWORD_ORDER, fill_value=0).reset_index()
    plt.figure(figsize=(10, 5))
    sns.barplot(data=counts, x="keyword", y="count", color="#4C78A8")
    plt.title("私聊关键词总出现次数")
    plt.xlabel("关键词")
    plt.ylabel("出现次数")
    savefig("00_keyword_total_counts.png")

    role_counts = (
        df.groupby(["keyword", "sender_role"], observed=False)["count"].sum()
        .reset_index()
    )
    plt.figure(figsize=(12, 6))
    sns.barplot(data=role_counts, x="keyword", y="count", hue="sender_role")
    plt.title("我 vs 对方：关键词出现次数")
    plt.xlabel("关键词")
    plt.ylabel("出现次数")
    savefig("01_keyword_by_sender_role.png")

    gender_counts = (
        df.groupby(["keyword", "gender"], observed=False)["count"].sum()
        .reset_index()
    )
    plt.figure(figsize=(12, 6))
    sns.barplot(data=gender_counts, x="keyword", y="count", hue="gender")
    plt.title("按私聊对象性别分组的关键词出现次数")
    plt.xlabel("关键词")
    plt.ylabel("出现次数")
    savefig("02_keyword_by_gender.png")


def plot_timeseries(df):
    months = month_range(df)
    monthly = (
        df.groupby(["month", "keyword"], observed=False)["count"].sum()
        .unstack(fill_value=0)
        .reindex(months, fill_value=0)
        .reindex(columns=KEYWORD_ORDER, fill_value=0)
    )
    ax = monthly.plot(figsize=(16, 7), marker="o", linewidth=2)
    ax.set_title("关键词月度趋势")
    ax.set_xlabel("月份")
    ax.set_ylabel("出现次数")
    plt.xticks(rotation=60)
    savefig("03_keyword_monthly_trend.png")

    role_month = (
        df.groupby(["month", "sender_role"])["count"].sum()
        .unstack(fill_value=0)
        .reindex(months, fill_value=0)
    )
    ax = role_month.plot(figsize=(16, 6), marker="o", linewidth=2)
    ax.set_title("关键词出现次数月度趋势：我 vs 对方")
    ax.set_xlabel("月份")
    ax.set_ylabel("出现次数")
    plt.xticks(rotation=60)
    savefig("04_role_monthly_trend.png")


def plot_heatmaps(df):
    weekday_cn = {0: "周一", 1: "周二", 2: "周三", 3: "周四", 4: "周五", 5: "周六", 6: "周日"}
    df = df.copy()
    df["weekday_cn"] = df["weekday"].map(weekday_cn)
    heat = (
        df.pivot_table(index="keyword", columns="hour", values="count", aggfunc="sum", fill_value=0, observed=False)
        .reindex(KEYWORD_ORDER)
    )
    plt.figure(figsize=(14, 5))
    sns.heatmap(heat, cmap="YlOrRd", linewidths=.3, linecolor="white")
    plt.title("关键词 x 小时热力图")
    plt.xlabel("小时")
    plt.ylabel("关键词")
    savefig("05_keyword_hour_heatmap.png")

    week_heat = (
        df.pivot_table(index="weekday_cn", columns="keyword", values="count", aggfunc="sum", fill_value=0, observed=False)
        .reindex(["周一", "周二", "周三", "周四", "周五", "周六", "周日"])
        .reindex(columns=KEYWORD_ORDER, fill_value=0)
    )
    plt.figure(figsize=(10, 6))
    sns.heatmap(week_heat, cmap="GnBu", annot=True, fmt=".0f", linewidths=.3, linecolor="white")
    plt.title("星期 x 关键词热力图")
    plt.xlabel("关键词")
    plt.ylabel("")
    savefig("06_keyword_weekday_heatmap.png")


def plot_top_chats(df):
    top = (
        df.groupby(["chat_username", "chat_name"])["count"].sum()
        .reset_index(name="count")
        .sort_values("count", ascending=False)
        .head(30)
    )
    top.to_csv(SCAN / "top_keyword_chats.csv", index=False, encoding="utf-8-sig")

    plt.figure(figsize=(11, 9))
    sns.barplot(data=top, y="chat_name", x="count", color="#54A24B")
    plt.title("关键词出现最多的私聊对象 Top 30")
    plt.xlabel("出现次数")
    plt.ylabel("")
    savefig("07_top_keyword_chats.png")

    top20_users = top.head(20)["chat_username"].tolist()
    matrix = (
        df[df["chat_username"].isin(top20_users)]
        .pivot_table(index="chat_name", columns="keyword", values="count", aggfunc="sum", fill_value=0, observed=False)
        .reindex(columns=KEYWORD_ORDER, fill_value=0)
    )
    order = matrix.sum(axis=1).sort_values(ascending=False).index
    matrix = matrix.reindex(order)
    plt.figure(figsize=(11, 8))
    sns.heatmap(matrix, cmap="YlGnBu", annot=True, fmt=".0f", linewidths=.3, linecolor="white")
    plt.title("Top 20 私聊对象关键词构成")
    plt.xlabel("关键词")
    plt.ylabel("")
    savefig("08_top_chats_keyword_matrix.png")


def write_report(df):
    totals = df.groupby("keyword", observed=False)["count"].sum().reindex(KEYWORD_ORDER, fill_value=0).reset_index()
    roles = df.groupby("sender_role")["count"].sum().reset_index()
    genders = df.groupby("gender")["count"].sum().reset_index()
    top_chats = pd.read_csv(SCAN / "top_keyword_chats.csv", encoding="utf-8-sig").head(20)
    lines = [
        "# 私聊口癖关键词深度扫描",
        "",
        f"- 命中消息行数: {len(df):,}",
        f"- 总出现次数: {int(df['count'].sum()):,}",
        f"- 覆盖私聊对象: {df['chat_username'].nunique():,}",
        f"- 时间范围: {df['datetime'].min()} 至 {df['datetime'].max()}",
        "",
        "## 关键词总量",
        "",
        totals.to_markdown(index=False),
        "",
        "## 我 vs 对方",
        "",
        roles.to_markdown(index=False),
        "",
        "## 按私聊对象性别",
        "",
        genders.to_markdown(index=False),
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
        print("No keyword hits found.")
        return
    plot_overview(df)
    plot_timeseries(df)
    plot_heatmaps(df)
    plot_top_chats(df)
    write_report(df)
    summary = {
        "hit_rows": int(len(df)),
        "total_occurrences": int(df["count"].sum()),
        "private_contacts_with_hits": int(df["chat_username"].nunique()),
        "figure_dir": str(OUT),
    }
    (SCAN / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
