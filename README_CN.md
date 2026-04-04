# WeChat CLI

[English](README.md)

命令行工具，查询本地微信数据——聊天记录、联系人、会话、收藏等。默认 JSON 输出，专为 LLM 集成设计。

## 功能亮点

- **开箱即用** — `pip install` + `wechat-cli init`，无外部依赖
- **11 个命令** — sessions、history、search、contacts、members、stats、export、favorites、unread、new-messages、init
- **默认 JSON** — 结构化输出，方便程序解析
- **跨平台** — macOS、Windows、Linux
- **即时解密** — SQLCipher 数据库透明解密，带缓存
- **消息类型过滤** — 按文本、图片、链接、文件、视频等过滤
- **聊天统计** — 发言排行、类型分布、24 小时活跃分布
- **Markdown 导出** — 将聊天记录导出为 markdown 或纯文本

## 快速开始

### 安装

**通过 pip（需要 Python >= 3.10）：**

```bash
pip install wechat-cli
```

**通过 npm（独立二进制，无需 Python）：**

```bash
npm install -g @canghe_ai/wechat-cli
```

> 目前仅提供 **macOS arm64** 二进制。其他平台（macOS Intel、Linux、Windows）可使用 pip 安装。欢迎提交其他平台的二进制 PR。

或从源码安装：

```bash
git clone https://github.com/freestylefly/wechat-cli.git
cd wechat-cli
pip install -e .
```

### 初始化

确保微信正在运行，然后：

```bash
# macOS/Linux: 可能需要 sudo 权限
sudo wechat-cli init

# Windows: 在有足够权限的终端中运行
wechat-cli init
```

这一步会：
1. 自动检测微信数据目录
2. 从微信进程内存中提取加密密钥
3. 将配置和密钥保存到 `~/.wechat-cli/`

完成后即可使用所有命令。

## 命令一览

### sessions — 最近会话

```bash
wechat-cli sessions                        # 最近 20 个会话 (JSON)
wechat-cli sessions --limit 10             # 最近 10 个
wechat-cli sessions --format text          # 纯文本输出
```

### history — 聊天记录

```bash
wechat-cli history "张三"                  # 最近 50 条消息
wechat-cli history "张三" --limit 100 --offset 50
wechat-cli history "交流群" --start-time "2026-04-01" --end-time "2026-04-03"
wechat-cli history "张三" --type link      # 只看链接/文件
wechat-cli history "张三" --format text
```

**选项：** `--limit`、`--offset`、`--start-time`、`--end-time`、`--type`、`--format`

### search — 搜索消息

```bash
wechat-cli search "Claude"                 # 全局搜索
wechat-cli search "Claude" --chat "交流群"  # 指定聊天搜索
wechat-cli search "开会" --chat "群A" --chat "群B"  # 多个聊天
wechat-cli search "报告" --type file        # 只搜文件
```

**选项：** `--chat`（可多次指定）、`--start-time`、`--end-time`、`--limit`、`--offset`、`--type`、`--format`

### contacts — 联系人搜索与详情

```bash
wechat-cli contacts --query "李"           # 搜索联系人
wechat-cli contacts --detail "张三"        # 查看详情
wechat-cli contacts --detail "wxid_xxx"    # 通过 wxid 查看
```

详情包括：昵称、备注、微信号、个性签名、头像 URL、账号类型。

### members — 群成员列表

```bash
wechat-cli members "AI交流群"              # 成员列表 (JSON)
wechat-cli members "AI交流群" --format text
```

显示成员昵称、wxid、备注和群主。

### stats — 聊天统计

```bash
wechat-cli stats "AI交流群"
wechat-cli stats "张三" --start-time "2026-04-01" --end-time "2026-04-03"
wechat-cli stats "AI交流群" --format text
```

返回：消息总数、类型分布、发言 Top 10、24 小时活跃分布（含柱状图）。

### export — 导出聊天记录

```bash
wechat-cli export "张三" --format markdown              # 输出到 stdout
wechat-cli export "张三" --format txt --output chat.txt  # 输出到文件
wechat-cli export "群聊" --start-time "2026-04-01" --limit 1000
```

**选项：** `--format markdown|txt`、`--output`、`--start-time`、`--end-time`、`--limit`

### favorites — 微信收藏

```bash
wechat-cli favorites                       # 最近收藏
wechat-cli favorites --type article        # 只看文章
wechat-cli favorites --query "计算机网络"    # 搜索收藏
```

**类型：** text、image、article、card、video

### unread — 未读会话

```bash
wechat-cli unread                          # 所有未读会话
wechat-cli unread --limit 10 --format text
```

### new-messages — 增量新消息

```bash
wechat-cli new-messages                    # 首次: 返回未读消息 + 保存状态
wechat-cli new-messages                    # 后续: 仅返回上次以来的新消息
```

状态保存在 `~/.wechat-cli/last_check.json`，删除此文件可重置。

## 消息类型过滤

`--type` 选项（适用于 `history` 和 `search`）：

| 值 | 说明 |
|---|------|
| `text` | 文本消息 |
| `image` | 图片 |
| `voice` | 语音 |
| `video` | 视频 |
| `sticker` | 表情 |
| `location` | 位置 |
| `link` | 链接/应用消息 |
| `file` | 文件 |
| `call` | 音视频通话 |
| `system` | 系统消息 |

## 使用场景

### AI 工具集成

```bash
# 供 Claude Code、Cursor 等 AI 工具调用
wechat-cli sessions --limit 5
wechat-cli history "张三" --limit 20 --format text
wechat-cli search "截止日期" --chat "项目组" --type text
```

所有命令默认输出 JSON，适合 AI Agent 工具调用。

### 聊天分析

```bash
# 群里谁最活跃？
wechat-cli stats "项目组" --format text

# 查看所有分享的链接
wechat-cli history "张三" --type link --limit 50

# 搜索特定文件
wechat-cli search "报告.xlsx" --type file
```

### 数据备份

```bash
wechat-cli export "项目组" --format markdown --output project.md
wechat-cli export "张三" --start-time "2026-01-01" --format txt --output chat.txt
```

### 消息监控

```bash
# 定时检查新消息
*/5 * * * * wechat-cli new-messages --format text
```

## 平台支持

| 平台 | 状态 | 说明 |
|------|------|------|
| macOS (Apple Silicon) | 支持 | 内置 arm64 二进制用于密钥提取 |
| macOS (Intel) | 支持 | 需要 x86_64 二进制 |
| Windows | 支持 | 读取 Weixin.exe 进程内存 |
| Linux | 支持 | 读取 /proc/pid/mem，需要 root |

## 工作原理

微信将聊天数据存储在本地的 SQLCipher 加密 SQLite 数据库中。WeChat CLI：

1. **提取密钥** — 扫描微信进程内存获取加密密钥（`wechat-cli init`）
2. **即时解密** — 透明解密数据库，使用页级 AES-256-CBC + 缓存
3. **本地查询** — 所有数据留在本机，无需网络访问

## 环境要求

- Python >= 3.10
- 微信在本地运行（用于 `init` 密钥提取）

## 开源协议

[Apache License 2.0](LICENSE)
