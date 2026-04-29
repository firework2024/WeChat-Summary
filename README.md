# WeChat Summary

一个基于本机微信数据生成「微信 1-4 月总结」页面的小工具。

本项目基于 [huohuoer/wechat-cli](https://github.com/huohuoer/wechat-cli) 开发，使用它提供的本地微信数据库读取、解密和联系人解析能力，在用户自己的电脑上生成一份 11 页的 HTML 总结页。

整个过程只读取本机数据，不需要上传聊天记录，也不会把微信数据发送到服务器，所有计算都在本地进行，无需担心隐私泄露。

请不要对本项目进行违法或违反微信操作规则的改造。

## 能生成什么

运行后会生成一个本地 HTML 页面，内容包括：

- 1-4 月本人发送消息总量、私聊/群聊数量
- 私聊互动最多的人、主动开启会话次数、最长连续发送
- 新出现的私聊对象和群聊
- 情绪词、笑声、常用关键词
- 群聊活跃排行和活跃时间段
- 朋友圈发布数、互动数、最多点赞动态、点赞最多的人
- 拍一拍、图片、语音、视频、表情等消息类型统计

页面模板在 `summary/summary.txt`，样式在 `summary/styles.css`，交互在 `summary/app.js`。

## 运行前准备

你需要：

- Windows / macOS / Linux
- Python 3.10 或更高版本
- 本机已登录微信
- 能正常访问本机微信数据目录

推荐先创建虚拟环境：

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -U pip
pip install -e .
```

如果你在 macOS 或 Linux 上，虚拟环境激活命令通常是：

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -U pip
pip install -e .
```

## 第一步：初始化微信数据

确保微信正在运行，然后执行：

```powershell
wechat-cli init
```

这一步会自动寻找本机微信数据目录，并尝试提取解密所需的信息。配置会保存在你的本机用户目录下，不会进入本项目仓库。

如果你有多个微信账号，本工具可能会让你选择当前要分析的账号。

macOS 用户可能需要给终端开启「完全磁盘访问权限」，否则无法读取微信数据目录。

## 第二步：准备背景图

总结页默认是 11 页，对应 11 张背景图：

```text
summary/pictures/1.png
summary/pictures/2.png
...
summary/pictures/11.png
```

如果你没有自己的背景图，也可以使用目前的默认背景图。

## 第三步：生成总结页面

在项目根目录运行：

```powershell
$env:PYTHONIOENCODING='utf-8'
python scripts\build_summary_page.py
```

macOS / Linux：

```bash
PYTHONIOENCODING=utf-8 python scripts/build_summary_page.py
```

运行成功后会生成：

```text
summary/index.html
summary/summary_filled.txt
summary/summary_data.json
```

其中：

- `summary/index.html` 是最终页面
- `summary/summary_filled.txt` 是填好真实数据后的文案
- `summary/summary_data.json` 是提取出来的统计字段

打开 `summary/index.html` 就可以查看页面。

注意：`summary/index.html` 是一个本地网页文件，不是在终端里显示的网站。你需要用浏览器打开它。

Windows 可以运行：

```powershell
start summary\index.html
```

macOS 可以运行：

```bash
open summary/index.html
```

Linux 可以运行：

```bash
xdg-open summary/index.html
```

也可以直接在文件管理器里进入 `summary/` 文件夹，双击 `index.html`。

## 常见问题

### 1. 为什么没有数据？

先确认：

- 微信正在运行
- 已经执行过 `wechat-cli init`
- 当前电脑确实有微信聊天记录
- 终端有权限访问微信数据目录

可以先运行：

```powershell
wechat-cli sessions
```

如果这个命令也没有结果，说明底层微信数据还没有配置好。

### 2. 为什么朋友圈数量不完整？

朋友圈数据来自微信本机缓存。微信客户端没有加载过的历史朋友圈，本机数据库里可能没有完整记录。

简单说：这个工具只能统计你电脑上已经缓存到的数据，不能凭空拿到云端所有历史朋友圈。

### 3. 可以改统计时间范围吗？

可以。当前时间范围定义在 `scripts/check_summary_feasibility.py`：

```python
START = datetime(2026, 1, 1)
END_EXCLUSIVE = datetime(2026, 4, 29)
```

例如想统计 2025 全年，可以改成：

```python
START = datetime(2025, 1, 1)
END_EXCLUSIVE = datetime(2026, 1, 1)
```

改完后重新运行：

```powershell
python scripts\build_summary_page.py
```

### 4. 可以改文案吗？

可以。直接编辑：

```text
summary/summary.txt
```

里面的 `【字段名】` 会在生成时自动替换成真实数据。

例如：

```text
你一共发出了【本人总消息数】条消息。
```

会生成类似：

```text
你一共发出了18,139条消息。
```

如果新增了一个不存在的字段，生成结果里会保留原占位符。你需要在 `scripts/build_summary_page.py` 里补上对应字段。

### 5. 页面不好看怎么办？

主要改这几个文件：

```text
summary/styles.css
summary/app.js
summary/pictures/
```

其中 `styles.css` 控制字体、颜色、布局；`app.js` 控制翻页；`pictures/` 是背景图。

## 隐私说明

请不要上传这些生成文件：

```text
summary/index.html
summary/summary_filled.txt
summary/summary_data.json
analysis/
contacts.json
sessions.json
week_group_*.json
epilepsy_*.json
*.db
```

这些文件可能包含真实联系人、群名、聊天内容、朋友圈内容和统计结果。

本仓库的 `.gitignore` 已经默认忽略这些文件。正常使用：

```powershell
git add .
```

不会把它们加进去。不要使用 `git add -f` 强行添加这些文件。


## 项目来源与致谢

本项目基于 [huohuoer/wechat-cli](https://github.com/huohuoer/wechat-cli) 开发，原项目使用 Apache License 2.0。

本仓库新增的主要内容是微信总结页面生成逻辑，包括：

- `summary/summary.txt`
- `summary/styles.css`
- `summary/app.js`
- `scripts/build_summary_page.py`
- `scripts/check_summary_feasibility.py`

感谢原项目提供本地微信数据读取和解密能力。

## 许可证

本项目沿用 Apache License 2.0。详见 `LICENSE`。
