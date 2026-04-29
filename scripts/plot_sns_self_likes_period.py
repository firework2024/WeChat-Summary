import json
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from matplotlib import font_manager

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "analysis" / "sns_self_likes"
OUT = ROOT / "analysis" / "sns_self_likes_2020_2026"
FIG = OUT / "figures"

START = "2020-01-01"
END_EXCLUSIVE = "2027-01-01"
LABEL = "2020-2026"


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


def savefig(name):
    plt.tight_layout()
    plt.savefig(FIG / name, dpi=180, bbox_inches="tight")
    plt.close()


def write_csv(df, name):
    df.to_csv(OUT / name, index=False, encoding="utf-8-sig")


def month_range(posts):
    start = posts["post_time"].min().to_period("M")
    end = posts["post_time"].max().to_period("M")
    return pd.period_range(start, end, freq="M").astype(str)


def main():
    setup_style()
    OUT.mkdir(parents=True, exist_ok=True)
    FIG.mkdir(parents=True, exist_ok=True)
    for old_png in FIG.glob("*.png"):
        old_png.unlink()

    posts = pd.read_csv(SRC / "self_posts.csv", encoding="utf-8-sig")
    likes = pd.read_csv(SRC / "self_post_likes.csv", encoding="utf-8-sig")
    comments = pd.read_csv(SRC / "self_post_comments.csv", encoding="utf-8-sig")

    posts["post_time"] = pd.to_datetime(posts["post_time"])
    likes["post_time"] = pd.to_datetime(likes["post_time"])
    comments["post_time"] = pd.to_datetime(comments["post_time"])

    start = pd.Timestamp(START)
    end = pd.Timestamp(END_EXCLUSIVE)
    posts = posts[(posts["post_time"] >= start) & (posts["post_time"] < end)].copy()
    likes = likes[(likes["post_time"] >= start) & (likes["post_time"] < end)].copy()
    comments = comments[(comments["post_time"] >= start) & (comments["post_time"] < end)].copy()

    for df in [posts, likes, comments]:
        df["post_year"] = df["post_time"].dt.year

    ranking = (
        likes.groupby("liker_display_name")
        .agg(
            like_count=("post_tid", "size"),
            liked_posts=("post_tid", "nunique"),
            active_months=("post_month", "nunique"),
            first_like_post_time=("post_time", "min"),
            last_like_post_time=("post_time", "max"),
        )
        .sort_values(["like_count", "liked_posts"], ascending=False)
        .reset_index()
    )
    ranking.insert(0, "rank", range(1, len(ranking) + 1))
    ranking["first_like_post_time"] = ranking["first_like_post_time"].dt.strftime("%Y-%m-%d %H:%M:%S")
    ranking["last_like_post_time"] = ranking["last_like_post_time"].dt.strftime("%Y-%m-%d %H:%M:%S")

    yearly = (
        posts.groupby("post_year")
        .agg(
            posts=("post_tid", "size"),
            total_likes=("like_count", "sum"),
            avg_likes=("like_count", "mean"),
            median_likes=("like_count", "median"),
            total_comments=("comment_count", "sum"),
            avg_comments=("comment_count", "mean"),
        )
        .reindex(range(2020, 2027), fill_value=0)
        .reset_index()
    )
    yearly[["avg_likes", "median_likes", "avg_comments"]] = yearly[
        ["avg_likes", "median_likes", "avg_comments"]
    ].round(2)

    write_csv(posts, "self_posts_2020_2026.csv")
    write_csv(likes, "self_post_likes_2020_2026.csv")
    write_csv(comments, "self_post_comments_2020_2026.csv")
    write_csv(ranking, "liker_ranking_2020_2026.csv")
    write_csv(yearly, "yearly_summary_2020_2026.csv")

    top = ranking.head(30).copy()
    plt.figure(figsize=(12, 11))
    sns.barplot(
        data=top.sort_values("like_count", ascending=True),
        y="liker_display_name",
        x="like_count",
        color="#4C78A8",
    )
    plt.title(f"{LABEL} 给我朋友圈点赞累计排名 Top 30")
    plt.xlabel("点赞次数")
    plt.ylabel("")
    savefig("00_top_likers_2020_2026.png")

    fig, axes = plt.subplots(2, 1, figsize=(14, 8), sharex=True)
    sns.barplot(data=yearly, x="post_year", y="posts", ax=axes[0], color="#54A24B")
    axes[0].set_title(f"{LABEL} 本人朋友圈发布数量")
    axes[0].set_xlabel("")
    axes[0].set_ylabel("朋友圈条数")
    sns.barplot(data=yearly, x="post_year", y="total_likes", ax=axes[1], color="#F58518")
    axes[1].set_title(f"{LABEL} 本人朋友圈收到点赞数")
    axes[1].set_xlabel("年份")
    axes[1].set_ylabel("点赞数")
    savefig("01_yearly_posts_and_likes.png")

    fig, axes = plt.subplots(2, 1, figsize=(16, 9), sharex=True)
    months = month_range(posts)
    monthly_posts = posts.groupby("post_month").size().reindex(months, fill_value=0)
    monthly_likes = likes.groupby("post_month").size().reindex(months, fill_value=0)
    monthly_posts.plot(ax=axes[0], marker="o", linewidth=2, color="#54A24B")
    axes[0].set_title(f"{LABEL} 我发朋友圈数量（月度）")
    axes[0].set_ylabel("朋友圈条数")
    monthly_likes.plot(ax=axes[1], marker="o", linewidth=2, color="#F58518")
    axes[1].set_title(f"{LABEL} 我的朋友圈收到点赞数（月度）")
    axes[1].set_ylabel("点赞数")
    axes[1].set_xlabel("月份")
    axes[1].tick_params(axis="x", rotation=60)
    savefig("02_monthly_posts_and_likes.png")

    plt.figure(figsize=(11, 6))
    sns.histplot(posts["like_count"], bins=30, color="#B279A2")
    plt.title(f"{LABEL} 每条朋友圈收到的点赞数分布")
    plt.xlabel("单条朋友圈点赞数")
    plt.ylabel("朋友圈条数")
    savefig("03_likes_per_post_distribution.png")

    top_posts = posts.sort_values("like_count", ascending=False).head(25).copy()
    top_posts["label"] = (
        top_posts["post_time"].dt.strftime("%Y-%m-%d")
        + " | "
        + top_posts["post_preview"].fillna("").str.slice(0, 24)
    )
    plt.figure(figsize=(13, 10))
    sns.barplot(
        data=top_posts.sort_values("like_count", ascending=True),
        y="label",
        x="like_count",
        color="#72B7B2",
    )
    plt.title(f"{LABEL} 收到点赞最多的本人朋友圈 Top 25")
    plt.xlabel("点赞数")
    plt.ylabel("")
    savefig("04_top_posts_by_likes.png")

    top_likers = ranking.head(25)["liker_display_name"].tolist()
    matrix = (
        likes[likes["liker_display_name"].isin(top_likers)]
        .groupby(["liker_display_name", "post_year"])
        .size()
        .unstack(fill_value=0)
        .reindex(index=top_likers)
        .reindex(columns=range(2020, 2027), fill_value=0)
    )
    plt.figure(figsize=(12, 10))
    sns.heatmap(matrix, cmap="YlGnBu", linewidths=.3, linecolor="white", annot=True, fmt=".0f")
    plt.title(f"{LABEL} Top 25 点赞者年度热力图")
    plt.xlabel("年份")
    plt.ylabel("")
    savefig("05_top_likers_year_heatmap.png")

    if not comments.empty:
        commenter = (
            comments.groupby("commenter_display_name")
            .size()
            .sort_values(ascending=False)
            .head(25)
            .reset_index(name="comment_count")
        )
        write_csv(commenter, "commenter_ranking_2020_2026.csv")
        plt.figure(figsize=(11, 9))
        sns.barplot(
            data=commenter.sort_values("comment_count", ascending=True),
            y="commenter_display_name",
            x="comment_count",
            color="#E45756",
        )
        plt.title(f"{LABEL} 给我朋友圈评论累计排名 Top 25")
        plt.xlabel("评论次数")
        plt.ylabel("")
        savefig("06_top_commenters.png")

    report = [
        f"# 本人朋友圈点赞统计（{LABEL}）",
        "",
        f"- 本人朋友圈条数: {len(posts):,}",
        f"- 收到点赞总数: {len(likes):,}",
        f"- 独立点赞者: {ranking['liker_display_name'].nunique():,}",
        f"- 收到评论总数: {len(comments):,}",
        f"- 数据范围: {posts['post_time'].min()} 至 {posts['post_time'].max()}",
        "",
        "## 年度汇总",
        "",
        yearly.to_markdown(index=False),
        "",
        "## 点赞 Top 30",
        "",
        ranking.head(30).to_markdown(index=False),
        "",
        "## 图表",
        "",
    ]
    for png in sorted(FIG.glob("*.png")):
        report.append(f"- `figures/{png.name}`")
    (OUT / "report.md").write_text("\n".join(report), encoding="utf-8")

    result = {
        "period": LABEL,
        "post_count": int(len(posts)),
        "like_count": int(len(likes)),
        "unique_likers": int(ranking["liker_display_name"].nunique()),
        "comment_count": int(len(comments)),
        "first_post_time": posts["post_time"].min().strftime("%Y-%m-%d %H:%M:%S") if len(posts) else None,
        "last_post_time": posts["post_time"].max().strftime("%Y-%m-%d %H:%M:%S") if len(posts) else None,
        "output_dir": str(OUT),
    }
    (OUT / "summary.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
