#!/usr/bin/env python3
"""
Hugging Face 镜像下载脚本 (USPTO 商标全量 Parquet 数据集)
支持国内镜像加速 (hf-mirror.com)、断点续传与多线程下载
"""

import os
import sys
import argparse

from setup_bak import BASE_DIR

def main():
    parser = argparse.ArgumentParser(description="从 Hugging Face 镜像下载 USPTO 商标全量数据集")
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    DEFAULT_OUTPUT = os.path.join(BASE_DIR, "uspto_trademarks_data")
    parser.add_argument(
        "--output_dir",
        type=str,
        default=DEFAULT_OUTPUT,
        help="数据保存的本地目录路径 (默认: ./uspto_trademarks_data)"
    )
    parser.add_argument(
        "--repo_id",
        type=str,
        default="illeatmyhat/uspto-trademarks",
        help="Hugging Face 仓库 ID (默认: illeatmyhat/uspto-trademarks)"
    )
    parser.add_argument(
        "--use_mirror",
        action="store_true",
        default=False,
        help="是否启用国内加速镜像源 hf-mirror.com (默认关闭)"
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="并发下载线程数 (默认: 4)"
    )
    args = parser.parse_args()

    # 1. 配置网络镜像环境（国内直连推荐开启）
    if args.use_mirror:
        print("[+] 已启用国内加速镜像: https://hf-mirror.com")
        os.environ["HF_ENDPOINT"] = "https://hf-mirror.com"

    # 2. 检查依赖库
    try:
        from huggingface_hub import snapshot_download
    except ImportError:
        print("[-] 未检测到 huggingface_hub，请先执行安装命令：")
        print("    pip install huggingface_hub")
        sys.exit(1)

    # 3. 创建输出目录
    save_path = os.path.abspath(args.output_dir)
    os.makedirs(save_path, exist_ok=True)

    print(f"[+] 目标数据集: {args.repo_id}")
    print(f"[+] 保存路径: {save_path}")
    print(f"[+] 开始下载（支持断点续传，中断后重新运行即可续传）...")

    try:
        downloaded_path = snapshot_download(
            repo_id=args.repo_id,
            repo_type="dataset",
            local_dir=save_path,
            max_workers=args.workers,
            resume_download=True
        )
        print("\n========================================================")
        print(f"[✓] 下载完成！文件已存放在: {downloaded_path}")
        print("========================================================")
    except Exception as e:
        print(f"\n[-] 下载遇到异常: {e}")
        print("    提示: 如遇网络连接中断，再次执行该脚本即可自动断点续传。")
        sys.exit(1)

if __name__ == "__main__":
    main()