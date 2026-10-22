# Trade Mark

一个基于 Python 的 USPTO 商标数据处理与检索工具，旨在将大规模公开商标数据从原始 Parquet 表下载、清洗、聚合、索引和查询，最终形成可检索、可导出的本地数据库。

Trade Mark 适用于处理 USPTO（美国专利商标局）商标数据，帮助用户快速完成：

- 按商标申请号查询
- 按商标名称关键词检索
- 按注册日期筛选
- 按条件导出 CSV 数据
- 按分片方式处理超大数据库文件

---

## 项目简介

USPTO 商标数据通常分散在多张 Parquet 表中，字段类型和结构较复杂，直接本地分析并不方便。本项目的目标是构建一套可重复使用的数据处理流程，将这些原始数据整合成统一的 DuckDB 数据库，并提供实用查询工具。

核心流程包括：

1. 从 Hugging Face 下载 USPTO 商标数据集
2. 读取并检查原始 Parquet 表结构
3. 将数据整合为统一的 `trademarks` 数据库表
4. 对大数据量进行分片存储，降低单文件复杂度
5. 支持本地检索、筛选与导出 CSV

---

## 主要功能

- 下载 USPTO 商标 Parquet 数据集
- 支持可选镜像加速（适合国内环境）
- 生成统一 DuckDB 商标数据库
- 聚合关键字段：
  - 商标名称
  - 申请号
  - 注册号
  - 申请日期
  - 注册日期
  - 公告日期
  - 状态信息
  - 国际分类
  - 持有人信息
  - 通讯地址
- 按申请号范围检索
- 按商标名称关键字检索
- 按注册日期区间筛选
- 导出查询结果为 CSV
- 将超大数据库按分片方式拆分，便于存储和管理
- 提供 GitHub Actions 自动化处理脚本

---

## 项目结构

```text
trade_mark/
├── .github/
│   └── workflows/
│       ├── process_data.yml
│       ├── build_window_exe.yml
│       └── main_trigger.yml
├── src/
│   ├── aaa.py
│   ├── all_data.py
│   ├── columns.py
│   ├── download.py
│   ├── fixe_for.py
│   ├── split_db.py
│   ├── test.py
│   ├── tiny_db.py
│   ├── to_duckdb.py
│   ├── to_duckdb_2.py
│   └── ...
├── setup.py
├── setup_bak.py
├── setup_split_bak.py
├── columns.md
├── uspto.md
├── .gitignore
├── README.md
└── ...
```

### 目录说明

- `src/download.py`：下载 USPTO 数据集
- `src/to_duckdb.py` / `src/to_duckdb_2.py`：将 Parquet 数据转换为 DuckDB
- `src/split_db.py`：将大数据库拆分为多个分片文件
- `src/columns.py`：检查数据表字段结构
- `src/test.py`：本地查询和测试脚本
- `setup.py`：交互式查询入口
- `columns.md` / `uspto.md`：字段说明文档

---

## 环境要求

- Python 3.10+
- DuckDB
- pandas
- requests
- beautifulsoup4
- huggingface_hub

安装依赖：

```bash
pip install duckdb pandas requests beautifulsoup4 huggingface_hub
```

如果你在国内环境，可以使用镜像源：

```bash
pip install duckdb pandas requests beautifulsoup4 huggingface_hub -i https://pypi.tuna.tsinghua.edu.cn/simple
```

---

## 快速开始

### 1. 克隆项目

```bash
git clone https://github.com/sundogya/trade_mark.git
cd trade_mark
```

### 2. 下载数据集

```bash
python src/download.py
```

可选：使用国内镜像：

```bash
python src/download.py --use_mirror
```

### 3. 构建 DuckDB 数据库

```bash
python src/to_duckdb.py
```

如果需要尝试另一版处理流程，也可以运行：

```bash
python src/to_duckdb_2.py
```

### 4. 分片处理大数据库

```bash
python src/split_db.py
```

该脚本会将超大数据库按行数切分为多个 `.duckdb` 文件，便于管理和查询。

### 5. 启动交互式查询工具

```bash
python setup.py
```

运行后可在命令行菜单中选择：

1. 根据申请号范围检索
2. 根据商标名称关键字检索
3. 根据注册日期范围检索
4. 从网络抓取最新数据并更新回写 DuckDB
5. 从网络抓取指定商标并导出 CSV
6. 根据申请号范围在线抓取并导出 CSV

---

## 典型使用场景

- 按商标申请号查询详细信息
- 按商标名称进行模糊搜索
- 按注册日期范围筛选商标
- 导出感兴趣商标数据到 CSV
- 建立本地商标数据库用于后续分析

---

## 注意事项

- 本项目依赖公开 USPTO 商标数据，使用前请确认符合相关数据使用规范。
- 大规模数据处理需要充足的磁盘空间和内存。
- 某些网络抓取流程会有请求间隔，避免被目标站点限制访问。
- 数据分片参数 `chunk_size` 建议根据实际数据量和机器配置进行调整。

---

## 许可证

本项目采用 MIT License。

```text
MIT License

Copyright (c) 2026 Trade Mark

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

---

## 结语

Trade Mark 项目将 USPTO 商标数据从“分散且大规模”的原始 Parquet 表整理成一套可控、可查询、可导出的本地数据处理链路。它不仅适合个人研究，也适合企业商标审查、商标数据分析和数据工程原型开发。

如果你希望进一步扩展该项目，建议优先考虑：

- 增加统一 `requirements.txt`
- 增加 CLI 命令入口
- 增加日志和重试机制
- 增加数据库版本管理
- 增加更完整的测试与校验流程
- 增加更规范的目录结构（如 `app/`、`data/`、`docs/`）
