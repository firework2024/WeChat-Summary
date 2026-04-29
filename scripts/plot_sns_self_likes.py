import json
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from matplotlib import font_manager

ROOT = Path(__file__).resolve().parents[1]
BASE = ROOT / "analysis" / "sns_self_likes"
FIG = BASE / "figures"


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


def month_range(series):
    start = series.min().to_period("M")
    end = series.max().to_period("M")
    return pd.period_range(start, end, freq="M").astype(str)


def main():
    setup_style()
    FIG.mkdir(parents=True, exist_ok=True)

    posts = pd.read_csv(BASE / "self_posts.csv", encoding="utf-8-sig")
    likes = pd.read_csv(BASE / "self_post_likes.csv", encoding="utf-8-sig")
    ranking = pd.read_csv(BASE / "liker_ranking.csv", encoding="utf-8-sig")
    comments = pd.read_csv(BASE / "self_post_comments.csv", encoding="utf-8-sig")

    posts["post_time"] = pd.to_datetime(posts["post_time"])
    likes["post_time"] = pd.to_datetime(likes["post_time"])
    if "like_time" in likes.columns:
        likes["like_time"] = pd.to_datetime(likes["like_time"], errors="coerce")

    top = ranking.head(30).copy()
    plt.figure(figsize=(12, 11))
    sns.barplot(
        data=top.sort_values("like_count", ascending=True),
        y="liker_display_name",
        x="like_count",
        color="#4C78A8",
    )
    plt.title("给我朋友圈点赞累计排名 Top 30")
    plt.xlabel("点赞次数")
    plt.ylabel("")
    savefig("00_top_likers.png")

    fig, axes = plt.subplots(2, 1, figsize=(16, 9), sharex=True)
    months = month_range(posts["post_time"])
    monthly_posts = posts.groupby("post_month").size().reindex(months, fill_value=0)
    monthly_likes = likes.groupby("post_month").size().reindex(months, fill_value=0)
    monthly_posts.plot(ax=axes[0], marker="o", linewidth=2, color="#54A24B")
    axes[0].set_title("我发朋友圈数量（月度）")
    axes[0].set_ylabel("朋友圈条数")
    monthly_likes.plot(ax=axes[1], marker="o", linewidth=2, color="#F58518")
    axes[1].set_title("我的朋友圈收到的点赞数（月度，按朋友圈发布时间归属）")
    axes[1].set_ylabel("点赞数")
    axes[1].set_xlabel("月份")
    axes[1].tick_params(axis="x", rotation=60)
    savefig("01_monthly_posts_and_likes.png")

    plt.figure(figsize=(11, 6))
    sns.histplot(posts["like_count"], bins=min(20, max(5, posts["like_count"].nunique())), color="#B279A2")
    plt.title("每条朋友圈收到的点赞数分布")
    plt.xlabel("单条朋友圈点赞数")
    plt.ylabel("朋友圈条数")
    savefig("02_likes_per_post_distribution.png")

    post_rank = posts.sort_values("like_count", ascending=False).head(20).copy()
    post_rank["label"] = post_rank["post_time"].dt.strftime("%Y-%m-%d") + " | " + post_rank["post_preview"].fillna("").str.slice(0, 24)
    plt.figure(figsize=(13, 9))
    sns.barplot(
        data=post_rank.sort_values("like_count", ascending=True),
        y="label",
        x="like_count",
        color="#72B7B2",
    )
    plt.title("收到点赞最多的本人朋友圈 Top 20")
    plt.xlabel("点赞数")
    plt.ylabel("")
    savefig("03_top_posts_by_likes.png")

    top_likers = ranking.head(20)["liker_display_name"].tolist()
    matrix = (
        likes[likes["liker_display_name"].isin(top_likers)]
        .groupby(["liker_display_name", "post_month"])
        .size()
        .unstack(fill_value=0)
        .reindex(index=top_likers)
        .reindex(columns=months, fill_value=0)
    )
    plt.figure(figsize=(16, 9))
    sns.heatmap(matrix, cmap="YlGnBu", linewidths=.3, linecolor="white")
    plt.title("Top 20 点赞者按月份的点赞热力图")
    plt.xlabel("朋友圈发布时间月份")
    plt.ylabel("")
    savefig("04_top_likers_month_heatmap.png")

    if not comments.empty:
        commenter = (
            comments.groupby("commenter_display_name")
            .size()
            .sort_values(ascending=False)
            .head(20)
            .reset_index(name="comment_count")
        )
        plt.figure(figsize=(11, 8))
        sns.barplot(
            data=commenter.sort_values("comment_count", ascending=True),
            y="commenter_display_name",
            x="comment_count",
            color="#E45756",
        )
        plt.title("给我朋友圈评论累计排名 Top 20")
        plt.xlabel("评论次数")
        plt.ylabel("")
        savefig("05_top_commenters.png")

    report = [
        "# 本人朋友圈点赞统计",
        "",
        f"- 本人朋友圈条数: {len(posts):,}",
        f"- 收到点赞总数: {len(likes):,}",
        f"- 独立点赞者: {ranking['liker_display_name'].nunique():,}",
        f"- 收到评论总数: {len(comments):,}",
        f"- 数据范围: {posts['post_time'].min()} 至 {posts['post_time'].max()}",
        "",
        "## 点赞 Top 20",
        "",
        ranking.head(20).to_markdown(index=False),
        "",
        "## 图表",
        "",
    ]
    for png in sorted(FIG.glob("*.png")):
        report.append(f"- `figures/{png.name}`")
    (BASE / "report.md").write_text("\n".join(report), encoding="utf-8")

    result = {
        "post_count": int(len(posts)),
        "like_count": int(len(likes)),
        "unique_likers": int(ranking["liker_display_name"].nunique()),
        "comment_count": int(len(comments)),
        "figure_dir": str(FIG),
    }
    (BASE / "plot_summary.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
