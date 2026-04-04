# WeChat CLI

[中文文档](README_CN.md)

A command-line tool to query your local WeChat data — chat history, contacts, sessions, favorites, and more. Designed for LLM integration with JSON output by default.

## Features

- **Self-contained** — `pip install` + `wechat-cli init`, no external dependencies
- **11 commands** — sessions, history, search, contacts, members, stats, export, favorites, unread, new-messages, init
- **JSON by default** — structured output for programmatic access
- **Cross-platform** — macOS, Windows, Linux
- **On-the-fly decryption** — SQLCipher databases decrypted transparently with caching
- **Message type filtering** — filter by text, image, link, file, video, etc.
- **Chat statistics** — top senders, type breakdown, hourly activity distribution
- **Markdown export** — export conversations as markdown or plain text

## Quick Start

### Install

**Via pip (requires Python >= 3.10):**

```bash
pip install wechat-cli
```

**Via npm (standalone binary, no Python needed):**

```bash
npm install -g @canghe_ai/wechat-cli
```

> Currently only **macOS arm64** binary is available. Other platforms (macOS Intel, Linux, Windows) can use the pip install method. PRs with additional platform binaries are welcome.

Or install from source:

```bash
git clone https://github.com/freestylefly/wechat-cli.git
cd wechat-cli
pip install -e .
```

### Initialize

Make sure WeChat is running, then:

```bash
# macOS/Linux: may need sudo for memory scanning
sudo wechat-cli init

# Windows: run in a terminal with sufficient privileges
wechat-cli init
```

This will:
1. Auto-detect your WeChat data directory
2. Extract encryption keys from WeChat process memory
3. Save config and keys to `~/.wechat-cli/`

That's it — you're ready to go.

## Commands

### sessions — Recent Chats

```bash
wechat-cli sessions                        # Last 20 sessions (JSON)
wechat-cli sessions --limit 10             # Last 10
wechat-cli sessions --format text          # Human-readable
```

### history — Chat Messages

```bash
wechat-cli history "Alice"                 # Last 50 messages
wechat-cli history "Alice" --limit 100 --offset 50
wechat-cli history "Team" --start-time "2026-04-01" --end-time "2026-04-03"
wechat-cli history "Alice" --type link     # Only links/files
wechat-cli history "Alice" --format text
```

**Options:** `--limit`, `--offset`, `--start-time`, `--end-time`, `--type`, `--format`

### search — Search Messages

```bash
wechat-cli search "hello"                  # Global search
wechat-cli search "hello" --chat "Alice"   # In specific chat
wechat-cli search "meeting" --chat "TeamA" --chat "TeamB"  # Multiple chats
wechat-cli search "report" --type file     # Only files
wechat-cli search "hello" --start-time "2026-04-01" --limit 50
```

**Options:** `--chat` (repeatable), `--start-time`, `--end-time`, `--limit`, `--offset`, `--type`, `--format`

### contacts — Contact Search & Details

```bash
wechat-cli contacts --query "Li"           # Search contacts
wechat-cli contacts --detail "Alice"       # View contact details
wechat-cli contacts --detail "wxid_xxx"    # By WeChat ID
```

Details include: nickname, remark, WeChat ID (alias), bio, avatar URL, account type.

### members — Group Members

```bash
wechat-cli members "Team Group"            # List all members (JSON)
wechat-cli members "Team Group" --format text
```

Shows member list with display names and group owner.

### stats — Chat Statistics

```bash
wechat-cli stats "Team Group"
wechat-cli stats "Alice" --start-time "2026-04-01" --end-time "2026-04-03"
wechat-cli stats "Team Group" --format text
```

Returns: total messages, type breakdown, top 10 senders, 24-hour activity distribution.

### export — Export Conversations

```bash
wechat-cli export "Alice" --format markdown              # To stdout
wechat-cli export "Alice" --format txt --output chat.txt  # To file
wechat-cli export "Team" --start-time "2026-04-01" --limit 1000
```

**Options:** `--format markdown|txt`, `--output`, `--start-time`, `--end-time`, `--limit`

### favorites — WeChat Bookmarks

```bash
wechat-cli favorites                       # Recent bookmarks
wechat-cli favorites --type article        # Articles only
wechat-cli favorites --query "machine learning"  # Search
```

**Types:** text, image, article, card, video

### unread — Unread Sessions

```bash
wechat-cli unread                          # All unread sessions
wechat-cli unread --limit 10 --format text
```

### new-messages — Incremental New Messages

```bash
wechat-cli new-messages                    # First call: return unread + save state
wechat-cli new-messages                    # Subsequent: only new since last call
```

State persists at `~/.wechat-cli/last_check.json`. Delete it to reset.

## Message Type Filter

The `--type` option (available on `history` and `search`) accepts:

| Value | Description |
|-------|-------------|
| `text` | Text messages |
| `image` | Images |
| `voice` | Voice messages |
| `video` | Videos |
| `sticker` | Stickers/emojis |
| `location` | Location shares |
| `link` | Links and app messages |
| `file` | File attachments |
| `call` | Voice/video calls |
| `system` | System messages |

## Use Cases

### LLM / AI Tool Integration

```bash
# For Claude Code, Cursor, or any AI tool that can run shell commands
wechat-cli sessions --limit 5
wechat-cli history "Alice" --limit 20 --format text
wechat-cli search "deadline" --chat "Team" --type text
```

All commands output JSON by default, making them ideal for AI agent tool calls.

### Chat Analysis

```bash
# Who talks the most in a group?
wechat-cli stats "Team Group" --format text

# Find all shared links in a conversation
wechat-cli history "Alice" --type link --limit 50

# Search for a specific file
wechat-cli search "report.xlsx" --type file
```

### Data Backup

```bash
# Export important conversations
wechat-cli export "Team Group" --format markdown --output team_chat.md
wechat-cli export "Alice" --start-time "2026-01-01" --format txt --output alice_2026.txt
```

### Notification Monitoring

```bash
# Cron job to check for new messages every 5 minutes
*/5 * * * * wechat-cli new-messages --format text
```

## Platform Support

| Platform | Status | Notes |
|----------|--------|-------|
| macOS (Apple Silicon) | Supported | Bundled arm64 binary for key extraction |
| macOS (Intel) | Supported | x86_64 binary needed |
| Windows | Supported | Reads Weixin.exe process memory |
| Linux | Supported | Reads /proc/pid/mem, requires root |

## How It Works

WeChat stores chat data in SQLCipher-encrypted SQLite databases on your local machine. WeChat CLI:

1. **Extracts keys** — scans WeChat process memory to find encryption keys (`wechat-cli init`)
2. **Decrypts on-the-fly** — transparently decrypts databases with page-level AES-256-CBC + caching
3. **Queries locally** — all data stays on your machine, no network access required

## Requirements

- Python >= 3.10
- WeChat running locally (for `init` key extraction)

## License

[Apache License 2.0](LICENSE)
