import csv
import time
import os
import sys
import glob
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

DB_DIR = os.path.join(BASE_DIR, "db/split_files")
ARCHIVE_PATH = os.path.join(BASE_DIR, "uspto_trademarks.tar.xz")
CSV_DIR = os.path.join(BASE_DIR, "csv")

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
    1. 检查 db/split_files/ 目录下是否存在任意 .duckdb 数据库分卷
    2. 若不存在，检查同级下是否有 uspto_trademarks.tar.xz
    3. 存在压缩包则自动解压，不存在则报错退出
    """
    os.makedirs(DB_DIR, exist_ok=True)
    existing_dbs = glob.glob(os.path.join(DB_DIR, "*.duckdb"))
    if existing_dbs:
        return

    print("=" * 60)
    print("未在 db/split_files/ 目录下检测到任何分卷数据库文件！")
    print("正在检索同级目录下是否具备初始压缩数据包...")

    if not os.path.exists(ARCHIVE_PATH):
        print(f"\n❌ [启动失败]: 未找到数据文件或压缩包！")
        print(f"缺失目录: {DB_DIR}/*.duckdb")
        print(f"同级目录亦未找到压缩包: {ARCHIVE_PATH}")
        print("请将 uspto_trademarks.tar.xz 置于该 exe 同级目录下！")
        wait_for_exit()

    print(f" 找到本地压缩数据包: {ARCHIVE_PATH}")
    print(" 正在执行解压操作（分卷解压请稍候）...")

    try:
        with tarfile.open(ARCHIVE_PATH, "r:xz") as tar:
            tar.extractall(path=BASE_DIR)
        print(" 数据解压完成！")
    except Exception as e:
        print(f"\n❌ 解压过程中发生严重异常: {e}")
        wait_for_exit()

    existing_dbs = glob.glob(os.path.join(DB_DIR, "*.duckdb"))
    if not existing_dbs:
        print(f"\n❌ 解压完成，但仍未在 db/split_files/ 目录下检测到任何 .duckdb 文件！")
        print("请检查压缩包内部打包结构是否包含 db/split_files/ 路径。")
        wait_for_exit()

    print("=" * 60 + "\n")

def get_duckdb_connection():
    """
    获取全局连接：动态挂载 db/split_files/ 目录下所有的分卷库，并生成统一的 trademarks 视图
    """
    db_files = sorted(glob.glob(os.path.join(DB_DIR, "*.duckdb")))
    if not db_files:
        return None, "未找到任何 .duckdb 分卷数据库"

    if len(db_files) == 1:
        return duckdb.connect(db_files[0], read_only=True), None

    con = duckdb.connect()
    sub_selects = []
    for idx, fpath in enumerate(db_files):
        alias = f"part_{idx}"
        con.execute(f"ATTACH '{fpath}' AS {alias} (READ_ONLY);")
        sub_selects.append(f"SELECT * FROM {alias}.trademarks")

    con.execute(f"CREATE VIEW trademarks AS {' UNION ALL '.join(sub_selects)};")
    return con, None

def export_df_to_chunked_csv(df, prefix_name, chunk_size=4000):
    """
    统一的数据分页导出函数：自动将结果分卷存入 csv 文件夹
    """
    total_rows = len(df)
    if total_rows == 0:
        print("未检索到任何符合条件的记录。")
        return

    print("--- 数据预览 (前 5 条) ---")
    print(df.head(5).to_string(index=False))
    print("---------------------------\n")

    os.makedirs(CSV_DIR, exist_ok=True)
    total_parts = (total_rows + chunk_size - 1) // chunk_size

    print(f"正在导出 CSV（每组 {chunk_size} 条，共计 {total_parts} 个文件）...")
    for part_idx in range(total_parts):
        start_i = part_idx * chunk_size
        end_i = min(start_i + chunk_size, total_rows)
        chunk_df = df.iloc[start_i:end_i]

        if total_parts == 1:
            filename = f"{prefix_name}.csv"
        else:
            filename = f"{prefix_name}_part{part_idx + 1}.csv"

        filepath = os.path.join(CSV_DIR, filename)
        chunk_df.to_csv(filepath, index=False, encoding='utf-8-sig')
        print(f" [{part_idx + 1}/{total_parts}] 已导出: csv/{filename} ({len(chunk_df)} 条)")

    print(f"\n 全部 CSV 已成功保存至目录: {CSV_DIR}")

def query_by_serial_range(start_val, end_val, chunk_size=4000):
    """
    选项 1：根据申请号范围检索
    """
    con, err = get_duckdb_connection()
    if err:
        print(f"❌ 连接错误: {err}")
        return

    start_str = str(start_val).strip()
    end_str = str(end_val).strip()
    print(f"\n正在检索申请号范围 [{start_str}] ~ [{end_str}] ...")

    query = """
    SELECT 
        mark, us_serial_number, us_registration_number,
        application_filing_date, registration_date, publication_date,
        status_code, status_date, for_goods_services, international_classes,
        owner_name, owner_address, legal_entity_type,
        state_or_country_where_organized, attorney_name,
        correspondent_name_address, correspondent_email, earliest_date_can_be_filed,
        latest_date_without_additional_fee
    FROM trademarks
    WHERE us_serial_number >= ? AND us_serial_number <= ?
    ORDER BY us_serial_number ASC;
    """
    try:
        df = con.execute(query, [start_str, end_str]).df()
        print(f" 检索完成，共找到 {len(df)} 条记录！\n")
        export_df_to_chunked_csv(df, f"sn_{start_str}_{end_str}", chunk_size)
    except Exception as e:
        print(f"查询出错: {e}")
    finally:
        con.close()

def query_by_mark_keyword(keyword, chunk_size=4000):
    """
    选项 2：根据商标名称关键字模糊检索 (不区分大小写)
    """
    con, err = get_duckdb_connection()
    if err:
        print(f"❌ 连接错误: {err}")
        return

    kw = str(keyword).strip()
    if not kw:
        print("关键字不能为空！")
        return

    print(f"\n正在检索名称包含 [{kw}] 的商标数据（不区分大小写）...")

    query = """
    SELECT 
        mark, us_serial_number, us_registration_number,
        application_filing_date, registration_date, publication_date,
        status_code, status_date, for_goods_services, international_classes,
        owner_name, owner_address, legal_entity_type,
        state_or_country_where_organized, attorney_name,
        correspondent_name_address, correspondent_email, earliest_date_can_be_filed,
        latest_date_without_additional_fee
    FROM trademarks
    WHERE mark ILIKE ?
    ORDER BY us_serial_number ASC;
    """
    try:
        # 使用 ILIKE 进行全词包含匹配
        df = con.execute(query, [f"%{kw}%"]).df()
        print(f" 检索完成，共找到 {len(df)} 条包含 [{kw}] 的商标记录！\n")
        
        # 安全处理文件名字符
        safe_kw = "".join([c for c in kw if c.isalnum() or c in (' ', '_', '-')]).strip()
        export_df_to_chunked_csv(df, f"mark_{safe_kw}", chunk_size)
    except Exception as e:
        print(f"查询出错: {e}")
    finally:
        con.close()

def query_by_registration_date(start_date, end_date, chunk_size=4000):
    """
    选项 3：根据注册日期范围检索
    """
    con, err = get_duckdb_connection()
    if err:
        print(f"❌ 连接错误: {err}")
        return

    s_dt = str(start_date).strip()
    e_dt = str(end_date).strip()

    print(f"\n正在检索注册日期在 [{s_dt}] ~ [{e_dt}] 之间的商标数据...")

    query = """
    SELECT 
        mark, us_serial_number, us_registration_number,
        application_filing_date, registration_date, publication_date,
        status_code, status_date, for_goods_services, international_classes,
        owner_name, owner_address, legal_entity_type,
        state_or_country_where_organized, attorney_name,
        correspondent_name_address, correspondent_email, earliest_date_can_be_filed,
        latest_date_without_additional_fee
    FROM trademarks
    WHERE registration_date BETWEEN TRY_CAST(? AS DATE) AND TRY_CAST(? AS DATE)
    ORDER BY registration_date ASC, us_serial_number ASC;
    """
    try:
        df = con.execute(query, [s_dt, e_dt]).df()
        print(f" 检索完成，共找到 {len(df)} 条记录！\n")
        export_df_to_chunked_csv(df, f"reg_date_{s_dt}_to_{e_dt}", chunk_size)
    except Exception as e:
        print(f"查询出错（请确保日期格式为 YYYY-MM-DD，例如 2023-01-01）: {e}")
    finally:
        con.close()

def getText(html, text=''):
    tag = html.find('div', string=text)
    if bool(tag):
        tag = tag.find_next('div')
    res = ''
    if bool(tag):
        res = tag.text.strip().replace('\r', '').replace('\n', ' ').replace('\t', '')
    return res

def update_duckdb_trademarks(serial_numbers):
    """
    选项 4：在线抓取并更新指定商标
    """
    db_files = sorted(glob.glob(os.path.join(DB_DIR, "*.duckdb")))
    if not db_files:
        print(f"❌ 找不到数据库文件，无法更新")
        return

    # 针对分卷库更新：默认写入最新/最后一个分卷，或首个匹配分卷
    target_db = db_files[-1]
    con = duckdb.connect(target_db)
    updated_count = 0

    print(f"\n准备在线刷新并更新指定商标（写入目标库: {os.path.basename(target_db)}）...")

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
                   '1、根据【申请号范围】检索并按 4000 条分组导出 CSV\n'
                   '2、根据【商标名称关键字】检索并按 4000 条分组导出 CSV\n'
                   '3、根据【注册日期范围】检索并按 4000 条分组导出 CSV\n'
                   '4、从网络抓取最新数据并更新回写进 DuckDB\n'
                   '请输入选项 (1/2/3/4): ').strip()

    if choice == '1':
        start = input('请输入起始申请号（如 90000000）: \n').strip()
        end = input('请输入截止申请号（如 90005000）: \n').strip()
        query_by_serial_range(start, end, chunk_size=4000)

    elif choice == '2':
        kw = input('请输入商标名称包含的关键字（如 APPLE、TECH，不区分大小写）: \n').strip()
        query_by_mark_keyword(kw, chunk_size=4000)

    elif choice == '3':
        start_date = input('请输入起始注册日期（格式: YYYY-MM-DD，如 2023-01-01）: \n').strip()
        end_date = input('请输入截止注册日期（格式: YYYY-MM-DD，如 2023-12-31）: \n').strip()
        query_by_registration_date(start_date, end_date, chunk_size=4000)

    elif choice == '4':
        data = input('请输入需要在线更新的商标申请号（多个用英文逗号隔开，如 98689327,90007611）: \n').strip()
        rows = data.split(',')
        update_duckdb_trademarks(rows)

    else:
        print("无效选项，退出程序。")