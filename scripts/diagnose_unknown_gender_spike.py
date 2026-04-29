import json
import sqlite3
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from matplotlib import font_manager

ROOT = Path(__file__).resolve().parents[1]

BASE = ROOT / "analysis" / "private_chats"
OUT = BASE / "unknown_gender_diagnostics"
MSG_CSV = BASE / "private_messages.csv"
GENDER_CSV = BASE / "gender_labels.csv"
SESSION_CSV = BASE / "conversation_starts.csv"


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


def find_contact_cache():
    candidates = sorted(
        Path.home().joinpath("AppData", "Local", "Temp", "wechat_cli_cache").glob("*.db"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    for path in candidates:
        try:
            with sqlite3.connect(path) as conn:
                tables = {
                    row[0]
                    for row in conn.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'"
                    ).fetchall()
                }
                if "contact" in tables:
                    return path
        except sqlite3.Error:
            continue
    return None


def load_contact_profiles():
    path = find_contact_cache()
    if path is None:
        return pd.DataFrame(columns=["username", "alias", "verify_flag", "remark", "nick_name"])
    with sqlite3.connect(path) as conn:
        return pd.read_sql_query(
            "SELECT username, alias, verify_flag, remark, nick_name FROM contact",
            conn,
        )


def account_kind(verify_flag):
    try:
        flag = int(verify_flag)
    except (TypeError, ValueError):
        flag = 0
    return "verified_or_service" if flag != 0 else "ordinary_contact"


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    setup_style()

    messages = pd.read_csv(MSG_CSV, encoding="utf-8-sig")
    sessions = pd.read_csv(SESSION_CSV, encoding="utf-8-sig")
    labels = pd.read_csv(GENDER_CSV, encoding="utf-8-sig")
    contacts = load_contact_profiles()

    labels["gender"] = labels["gender"].map(normalize_gender)
    gender_map = labels.set_index("username")["gender"].to_dict()

    profiles = contacts.drop_duplicates("username").copy()
    profiles["account_kind"] = profiles["verify_flag"].map(account_kind)

    messages["gender"] = messages["chat_username"].map(gender_map).fillna("unknown")
    sessions["gender"] = sessions["chat_username"].map(gender_map).fillna("unknown")

    messages = messages.merge(
        profiles[["username", "alias", "verify_flag", "account_kind"]],
        left_on="chat_username",
        right_on="username",
        how="left",
    )
    sessions = sessions.merge(
        profiles[["username", "alias", "verify_flag", "account_kind"]],
        left_on="chat_username",
        right_on="username",
        how="left",
    )
    messages["account_kind"] = messages["account_kind"].fillna("ordinary_contact")
    sessions["account_kind"] = sessions["account_kind"].fillna("ordinary_contact")

    unknown_messages = messages[messages["gender"].eq("unknown")].copy()
    unknown_sessions = sessions[sessions["gender"].eq("unknown")].copy()

    by_contact = (
        unknown_messages.groupby(["chat_username", "chat_name", "account_kind", "alias", "verify_flag"], dropna=False)
        .size()
        .reset_index(name="messages")
        .sort_values("messages", ascending=False)
    )
    by_contact.to_csv(OUT / "unknown_contacts_by_messages.csv", index=False, encoding="utf-8-sig")

    by_month_kind_messages = (
        unknown_messages.groupby(["month", "account_kind"]).size().unstack(fill_value=0).sort_index()
    )
    by_month_kind_sessions = (
        unknown_sessions[unknown_sessions["initiator"].eq("contact")]
        .groupby(["month", "account_kind"])
        .size()
        .unstack(fill_value=0)
        .sort_index()
    )
    by_month_kind_messages.to_csv(OUT / "unknown_monthly_messages_by_account_kind.csv", encoding="utf-8-sig")
    by_month_kind_sessions.to_csv(OUT / "unknown_monthly_contact_initiated_sessions_by_account_kind.csv", encoding="utf-8-sig")

    spike_rows = []
    for month in ["2026-02", "2026-03", "2026-04"]:
        part = unknown_sessions[
            unknown_sessions["month"].eq(month) & unknown_sessions["initiator"].eq("contact")
        ]
        ranked = (
            part.groupby(["chat_username", "chat_name", "account_kind", "alias", "verify_flag"], dropna=False)
            .size()
            .reset_index(name="contact_initiated_sessions")
            .sort_values("contact_initiated_sessions", ascending=False)
            .head(30)
        )
        ranked.insert(0, "month", month)
        spike_rows.append(ranked)
    spike = pd.concat(spike_rows, ignore_index=True)
    spike.to_csv(OUT / "unknown_spike_top_contacts_2026_02_to_04.csv", index=False, encoding="utf-8-sig")

    fig, axes = plt.subplots(2, 1, figsize=(15, 9), sharex=True)
    by_month_kind_messages.plot(ax=axes[0], linewidth=2, marker="o")
    axes[0].set_title("unknown 性别消息量来源拆分")
    axes[0].set_ylabel("消息数")
    by_month_kind_sessions.plot(ax=axes[1], linewidth=2, marker="o")
    axes[1].set_title("unknown 性别的对方主动发起次数来源拆分")
    axes[1].set_ylabel("次数")
    axes[1].set_xlabel("月份")
    axes[1].tick_params(axis="x", rotation=60)
    plt.tight_layout()
    plt.savefig(OUT / "unknown_source_breakdown.png", dpi=180, bbox_inches="tight")
    plt.close()

    summary = {
        "unknown_contacts": int(unknown_messages["chat_username"].nunique()),
        "unknown_messages": int(len(unknown_messages)),
        "unknown_contact_initiated_sessions": int(
            unknown_sessions["initiator"].eq("contact").sum()
        ),
        "verified_or_service_unknown_contacts": int(
            by_contact.loc[by_contact["account_kind"].eq("verified_or_service"), "chat_username"].nunique()
        ),
        "verified_or_service_unknown_messages": int(
            by_contact.loc[by_contact["account_kind"].eq("verified_or_service"), "messages"].sum()
        ),
        "ordinary_unknown_contacts": int(
            by_contact.loc[by_contact["account_kind"].eq("ordinary_contact"), "chat_username"].nunique()
        ),
        "ordinary_unknown_messages": int(
            by_contact.loc[by_contact["account_kind"].eq("ordinary_contact"), "messages"].sum()
        ),
        "output_dir": str(OUT),
    }
    (OUT / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
