import csv
import time
import os
import sys
import tarfile
import requests
from bs4 import BeautifulSoup
import duckdb
from huggingface_hub import hf_hub_download

# 兼容 PyInstaller 打包后的真实 exe 所在目录
if getattr(sys, 'frozen', False):
    BASE_DIR = os.path.dirname(os.path.abspath(sys.executable))
else:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))

DB_DIR = os.path.join(BASE_DIR, "db")
DB_PATH = os.path.join(DB_DIR, "uspto_trademarks.duckdb")
ARCHIVE_PATH = os.path.join(BASE_DIR, "uspto_trademarks.tar.xz")
CSV_DIR = os.path.join(BASE_DIR, "csv")  # 统一导出目录

def wait_for_exit(message="\n按任意键退出程序..."):
    """跨平台友好退出，防止 Windows 下双击 exe 报错一闪而过"""
    print(message)
    try:
        if os.name == 'nt':
            import msvcrt
            msvcrt.getch()
        else:
            input()
    except Exception:
        pass
    sys.exit(1)

def ensure_database_ready():
    """
    启动检测逻辑：
    1. 检查 db/uspto_trademarks.duckdb 是否存在，存在则继续
    2. 若不存在，检查同级下是否有 uspto_trademarks.tar.xz
    3. 存在压缩包则解压，不存在则报错并按任意键退出
    """
    if os.path.exists(DB_PATH):
        return

    print("=" * 60)
    print("未检测到数据库文件: db/uspto_trademarks.duckdb")
    print("正在检索本地是否具备初始压缩数据包...")

    if not os.path.exists(ARCHIVE_PATH):
        print(f"\n❌ [启动失败]: 未找到数据文件或压缩包！")
        print(f"缺失文件: {DB_PATH}")
        print(f"同级目录亦未找到压缩包: {ARCHIVE_PATH}")
        print("请将 uspto_trademarks.tar.xz 或解压后的 db 文件夹置于该 exe 同级目录下！")
        wait_for_exit()

    print(f" 找到本地压缩数据包: {ARCHIVE_PATH}")
    print(" 正在执行解压操作（解压可能需要 1~2 分钟，请稍候）...")

    try:
        with tarfile.open(ARCHIVE_PATH, "r:xz") as tar:
            tar.extractall(path=BASE_DIR)
        print(" 数据解压完成！")
    except Exception as e:
        print(f"\n❌ 解压过程中发生严重异常: {e}")
        wait_for_exit()

    if not os.path.exists(DB_PATH):
        print(f"\n❌ 解压完成，但未在目标路径检测到数据库: {DB_PATH}")
        print("请检查压缩包内部打包结构是否包含 db/uspto_trademarks.duckdb。")
        wait_for_exit()

    print("=" * 60 + "\n")

def getText(html, text=''):
    tag = html.find('div', string=text)
    if bool(tag):
        tag = tag.find_next('div')
    res = ''
    if bool(tag):
        res = tag.text.strip().replace('\r', '').replace('\n', ' ').replace('\t', '')
    return res

def query_duckdb_by_range(start_val, end_val, export_csv=True, chunk_size=2000):
    """
    选项 1：从 db/uspto_trademarks.duckdb 检索数据
    优化：以 chunk_size (默认 2000) 条为一组分卷导出到 csv/ 目录下
    """
    if not os.path.exists(DB_PATH):
        print(f"❌ 找不到数据库文件: {DB_PATH}")
        return

    con = duckdb.connect(DB_PATH, read_only=True)
    print(f"\n正在从 DuckDB 检索范围 [{start_val}] ~ [{end_val}] 内的商标数据...")

    query = """
    SELECT 
        mark,
        us_serial_number,
        us_registration_number,
        application_filing_date,
        registration_date,
        publication_date,
        status_code,
        status_date,
        for_goods_services,
        international_classes,
        owner_name,
        owner_address,
        legal_entity_type,
        state_or_country_where_organized,
        attorney_name,
        correspondent_name_address,
        earliest_date_can_be_filed,
        latest_date_without_additional_fee
    FROM trademarks
    WHERE (
        TRY_CAST(us_serial_number AS BIGINT) BETWEEN ? AND ?
        OR TRY_CAST(us_registration_number AS BIGINT) BETWEEN ? AND ?
    )
    ORDER BY us_serial_number ASC;
    """

    try:
        df = con.execute(query, [int(start_val), int(end_val), int(start_val), int(end_val)]).df()
        total_rows = len(df)
        print(f" 检索完成，共找到 {total_rows} 条记录！\n")

        if total_rows > 0:
            print("--- 数据预览 (前 5 条) ---")
            print(df.head(5).to_string(index=False))
            print("---------------------------\n")

            if export_csv:
                os.makedirs(CSV_DIR, exist_ok=True)
                total_parts = (total_rows + chunk_size - 1) // chunk_size

                print(f"正在导出 CSV（每组 {chunk_size} 条，共计 {total_parts} 个文件）...")
                for part_idx in range(total_parts):
                    start_i = part_idx * chunk_size
                    end_i = min(start_i + chunk_size, total_rows)
                    chunk_df = df.iloc[start_i:end_i]

                    # 仅一个文件时直接命名，多文件时追加 part 序号
                    if total_parts == 1:
                        filename = f"{start_val}_{end_val}.csv"
                    else:
                        filename = f"{start_val}_{end_val}_part{part_idx + 1}.csv"

                    filepath = os.path.join(CSV_DIR, filename)
                    chunk_df.to_csv(filepath, index=False, encoding='utf-8-sig')
                    print(f" [{part_idx + 1}/{total_parts}] 已导出: csv/{filename} ({len(chunk_df)} 条)")

                print(f"\n 全部 CSV 已成功保存至目录: {CSV_DIR}")
        else:
            print("该区间内未查询到匹配记录。")
    except Exception as e:
        print(f"查询或导出出错: {e}")
    finally:
        con.close()

def update_duckdb_trademarks(serial_numbers):
    """
    选项 2：从 DuckDB 读取特定商标，并通过 USPTO 抓取最新数据回写进 DuckDB
    """
    if not os.path.exists(DB_PATH):
        print(f"❌ 找不到数据库文件: {DB_PATH}")
        return

    con = duckdb.connect(DB_PATH)
    updated_count = 0

    print(f"\n准备在线刷新并更新指定商标，共 {len(serial_numbers)} 个待处理...")

    for sn in serial_numbers:
        sn = sn.strip()
        if not sn:
            continue
        try:
            url = f'https://tsdr.uspto.gov/statusview/sn{sn}'
            headers_agent = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
            page = requests.get(url, headers=headers_agent, timeout=15)
            print(f"[{sn}] 请求状态码: {page.status_code}")

            if page.status_code == 200:
                html = BeautifulSoup(page.text, 'html.parser')

                mark = getText(html, 'Mark:')
                reg_num = getText(html, 'US Registration Number:')
                status_dt = getText(html, 'Status Date:')
                pub_dt = getText(html, 'Publication Date:')
                for_goods = getText(html, 'For:')
                intl_class = getText(html, 'International Class(es):')
                own_name = getText(html, 'Owner Name:')
                own_addr = getText(html, 'Owner Address:')
                legal_type = getText(html, 'Legal Entity Type:')
                citizenship = getText(html, 'Citizenship:')
                corr_addr = getText(html, 'Correspondent Name/Address:')
                corr_email = getText(html, 'Correspondent e-mail:')
                corr_email_auth = getText(html, 'Correspondent e-mail Authorized:')

                con.execute("""
                INSERT OR REPLACE INTO trademarks (
                    mark,
                    us_serial_number,
                    us_registration_number,
                    status_date,
                    publication_date,
                    for_goods_services,
                    international_classes,
                    owner_name,
                    owner_address,
                    legal_entity_type,
                    state_or_country_where_organized,
                    correspondent_name_address,
                    correspondent_email,
                    correspondent_email_authorized
                ) VALUES (
                    ?, ?, ?, TRY_CAST(? AS DATE), TRY_CAST(? AS DATE),
                    ?, ?, ?, ?, ?, ?, ?, ?, ?
                );
                """, [
                    mark, sn, reg_num, status_dt, pub_dt,
                    for_goods, intl_class, own_name, own_addr, legal_type,
                    citizenship, corr_addr, corr_email, corr_email_auth
                ])

                print(f" [{sn}] 已更新入库: Mark={mark}, For={for_goods[:30]}...")
                updated_count += 1
            else:
                print(f"⚠️ [{sn}] 页面拉取未成功 (HTTP {page.status_code})")

            time.sleep(1.5)
        except Exception as e:
            print(f"❌ [{sn}] 更新异常: {e}")

    con.execute("CHECKPOINT;")
    con.close()
    print(f"\n全部执行完毕！成功更新回写 DuckDB: {updated_count} 条。")

# 命令行交互主入口
if __name__ == "__main__":
    ensure_database_ready()

    choice = input('请选择操作模式：\n'
                   '1、根据最大/最小值从 DuckDB 检索并按2000条分组导出 CSV\n'
                   '2、从网络抓取最新数据并更新回写进 DuckDB\n'
                   '请输入选项 (1/2): ').strip()

    if choice == '1':
        start = input('请输入最小值（如 90000000）: \n').strip()
        end = input('请输入最大值（如 90005000）: \n').strip()
        query_duckdb_by_range(start, end, chunk_size=2000)
    elif choice == '2':
        data = input('请输入需要在线更新的商标申请号（多个用英文逗号隔开，如 98689327,90007611）: \n').strip()
        rows = data.split(',')
        update_duckdb_trademarks(rows)
    else:
        print("无效选项，退出程序。")