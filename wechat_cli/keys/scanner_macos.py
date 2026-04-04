"""macOS 密钥提取 — 通过 C 二进制扫描微信进程内存"""

import os
import platform
import subprocess
import sys

from .common import collect_db_files, cross_verify_keys, save_results, scan_memory_for_keys


def _find_binary():
    """查找对应架构的 C 二进制。"""
    machine = platform.machine()
    if machine == "arm64":
        name = "find_all_keys_macos.arm64"
    elif machine == "x86_64":
        name = "find_all_keys_macos.x86_64"
    else:
        raise RuntimeError(f"不支持的 macOS 架构: {machine}")

    # PyInstaller 运行时：从临时解压目录查找
    if getattr(sys, 'frozen', False):
        base = sys._MEIPASS
    else:
        base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    bin_path = os.path.join(base, "wechat_cli", "bin", name)
    if os.path.isfile(bin_path):
        return bin_path

    # fallback: 直接在 bin/ 下
    bin_path = os.path.join(base, "bin", name)
    if os.path.isfile(bin_path):
        return bin_path

    raise RuntimeError(
        f"找不到密钥提取二进制: {bin_path}\n"
        "请确认安装包完整"
    )


def extract_keys(db_dir, output_path, pid=None):
    """通过 C 二进制提取 macOS 微信数据库密钥。

    C 二进制需要在微信数据目录的父目录下运行，
    因为它会自动检测 db_storage 子目录。
    输出 all_keys.json 到当前工作目录。

    Args:
        db_dir: 微信 db_storage 目录
        output_path: all_keys.json 输出路径
        pid: 未使用（C 二进制自动检测进程）

    Returns:
        dict: salt_hex -> enc_key_hex 映射
    """
    import re
    import json

    binary = _find_binary()

    # C 二进制的工作目录需要是 db_storage 的父目录
    work_dir = os.path.dirname(db_dir)
    if not os.path.isdir(work_dir):
        raise RuntimeError(f"微信数据目录不存在: {work_dir}")

    print(f"[+] 使用 C 二进制提取密钥: {binary}")
    print(f"[+] 工作目录: {work_dir}")

    try:
        result = subprocess.run(
            [binary],
            cwd=work_dir,
            capture_output=True,
            text=True,
            timeout=120,
        )
    except subprocess.TimeoutExpired:
        raise RuntimeError("密钥提取超时（120s）")
    except PermissionError:
        raise RuntimeError(
            f"无法执行 {binary}\n"
            "请确保文件有执行权限: chmod +x " + binary
        )

    # 打印 C 二进制的输出
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(result.stderr, file=sys.stderr)

    # C 二进制输出 all_keys.json 到 work_dir
    c_output = os.path.join(work_dir, "all_keys.json")
    if not os.path.exists(c_output):
        if "task_for_pid" in (result.stdout or "") + (result.stderr or ""):
            raise RuntimeError(
                "需要 root 权限才能读取微信进程内存。\n"
                "请使用: sudo wechat-cli init"
            )
        raise RuntimeError(
            "C 二进制未能生成密钥文件。\n"
            f"stdout: {result.stdout}\nstderr: {result.stderr}"
        )

    # 读取并转存到 output_path
    with open(c_output, encoding="utf-8") as f:
        keys_data = json.load(f)

    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(keys_data, f, indent=2, ensure_ascii=False)

    # 清理 C 二进制的临时输出
    if os.path.abspath(c_output) != os.path.abspath(output_path):
        os.remove(c_output)

    # 构建 salt -> key 映射
    key_map = {}
    for rel, info in keys_data.items():
        if isinstance(info, dict) and "enc_key" in info and "salt" in info:
            key_map[info["salt"]] = info["enc_key"]

    print(f"\n[+] 提取到 {len(key_map)} 个密钥，保存到: {output_path}")
    return key_map
