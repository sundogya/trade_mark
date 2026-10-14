import csv
import time
import os
import requests
from bs4 import BeautifulSoup
import duckdb
from huggingface_hub import hf_hub_download

# 配置数据库绝对路径: 同级目录下的 db/uspto_trademarks.duckdb
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_DIR = os.path.join(BASE_DIR, "db")
DB_PATH = os.path.join(DB_DIR, "uspto_trademarks.duckdb")

def getText(html, text=''):
    tag = html.find('div', string=text)
    if bool(tag):
        tag = tag.find_next('div')
    res = ''
    if bool(tag):
        res = tag.text.strip().replace('\r', '').replace('\n', ' ').replace('\t', '')
    return res

def query_duckdb_by_range(start_val, end_val, export_csv=True):
    """
    选项 1：直接从 db/uspto_trademarks.duckdb 按编号范围毫秒级检索，并可选择导出 CSV
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
                output_file = f"{start_val}_{end_val}.csv"
                df.to_csv(output_file, index=False, encoding='utf-8-sig')
                print(f" 数据已导出至 CSV: {output_file}")
        else:
            print("该区间内未查询到匹配记录。")
    except Exception as e:
        print(f"查询出错: {e}")
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
                for_goods = getText(html, 'For:')  # 声明描述
                intl_class = getText(html, 'International Class(es):')
                own_name = getText(html, 'Owner Name:')
                own_addr = getText(html, 'Owner Address:')
                legal_type = getText(html, 'Legal Entity Type:')
                citizenship = getText(html, 'Citizenship:')
                corr_addr = getText(html, 'Correspondent Name/Address:')
                corr_email = getText(html, 'Correspondent e-mail:')
                corr_email_auth = getText(html, 'Correspondent e-mail Authorized:')

                # 将最新数据覆盖/插入 DuckDB
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

def update_from_huggingface(repo_id, filename, hf_token=None):
    """
    选项 3：从 Hugging Face 社区下载增量文件并合并进 db/uspto_trademarks.duckdb
    """
    os.makedirs(DB_DIR, exist_ok=True)

    print(f"\n[1/3] 正在从 Hugging Face 仓库 ({repo_id}) 下载增量文件: {filename} ...")
    try:
        local_file_path = hf_hub_download(
            repo_id=repo_id,
            filename=filename,
            repo_type="dataset",
            token=hf_token
        )
        print(f" 文件已准备就绪: {local_file_path}")
    except Exception as e:
        print(f"❌ Hugging Face 下载失败: {e}")
        return

    con = duckdb.connect(DB_PATH)

    print("\n[2/3] 检查数据库与目标表结构...")
    con.execute("""
    CREATE TABLE IF NOT EXISTS trademarks (
        mark VARCHAR,
        us_serial_number VARCHAR PRIMARY KEY,
        us_registration_number VARCHAR,
        application_filing_date DATE,
        registration_date DATE,
        publication_date DATE,
        status_code VARCHAR,
        status_date DATE,
        for_goods_services VARCHAR,
        international_classes VARCHAR,
        owner_name VARCHAR,
        owner_address VARCHAR,
        legal_entity_type VARCHAR,
        state_or_country_where_organized VARCHAR,
        attorney_name VARCHAR,
        docket_number VARCHAR,
        attorney_primary_email_address VARCHAR,
        attorney_email_authorized VARCHAR,
        correspondent_name_address VARCHAR,
        correspondent_phone VARCHAR,
        correspondent_email VARCHAR,
        correspondent_email_authorized VARCHAR,
        earliest_date_can_be_filed DATE,
        latest_date_without_additional_fee DATE,
        latest_date_with_additional_fee DATE
    );
    """)

    if local_file_path.endswith('.parquet'):
        read_expr = f"read_parquet('{local_file_path}')"
    else:
        read_expr = f"read_csv_auto('{local_file_path}', header=True)"

    print("\n[3/3] 正在执行增量合入与排重写入 (包含 for_goods_services)...")
    con.execute(f"""
    CREATE OR REPLACE TEMP TABLE temp_hf_incoming AS
    SELECT 
        COALESCE(COLUMNS('(?i)mark.*'), NULL)                                       AS mark,
        CAST(COALESCE(COLUMNS('(?i)(us_)?serial(_no|_number)?'), NULL) AS VARCHAR)  AS us_serial_number,
        CAST(COALESCE(COLUMNS('(?i)(us_)?reg(istration)?(_no|_number)?'), NULL) AS VARCHAR) AS us_registration_number,
        TRY_CAST(COALESCE(COLUMNS('(?i)(app.*filing|filing).*dt.*'), NULL) AS DATE) AS application_filing_date,
        TRY_CAST(COALESCE(COLUMNS('(?i)reg.*dt.*'), NULL) AS DATE)                  AS registration_date,
        TRY_CAST(COALESCE(COLUMNS('(?i)pub.*dt.*'), NULL) AS DATE)                  AS publication_date,
        CAST(COALESCE(COLUMNS('(?i)status(_cd|_code)?'), NULL) AS VARCHAR)          AS status_code,
        TRY_CAST(COALESCE(COLUMNS('(?i)status.*dt.*'), NULL) AS DATE)               AS status_date,
        CAST(COALESCE(COLUMNS('(?i)(for.*|statement.*text|goods.*service.*)'), NULL) AS VARCHAR) AS for_goods_services,
        CAST(COALESCE(COLUMNS('(?i)(intl_class.*|international.*)'), NULL) AS VARCHAR) AS international_classes,
        CAST(COALESCE(COLUMNS('(?i)own.*name.*'), NULL) AS VARCHAR)                 AS owner_name,
        CAST(COALESCE(COLUMNS('(?i)own.*addr.*'), NULL) AS VARCHAR)                 AS owner_address,
        CAST(COALESCE(COLUMNS('(?i)legal.*entity.*'), NULL) AS VARCHAR)             AS legal_entity_type,
        CAST(COALESCE(COLUMNS('(?i)(state.*country|citizenship).*'), NULL) AS VARCHAR) AS state_or_country_where_organized,
        CAST(COALESCE(COLUMNS('(?i)attorney.*name.*'), NULL) AS VARCHAR)            AS attorney_name,
        CAST(COALESCE(COLUMNS('(?i)cor.*(addr|name).*'), NULL) AS VARCHAR)          AS correspondent_name_address
    FROM {read_expr}
    WHERE us_serial_number IS NOT NULL;
    """)

    con.execute("""
    INSERT OR REPLACE INTO trademarks (
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
        latest_date_without_additional_fee,
        latest_date_with_additional_fee
    )
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
        registration_date + INTERVAL 5 YEAR,
        registration_date + INTERVAL 6 YEAR,
        registration_date + INTERVAL 6 YEAR + INTERVAL 6 MONTH
    FROM temp_hf_incoming;
    """)

    con.execute("CHECKPOINT;")
    incoming_count = con.execute("SELECT COUNT(*) FROM temp_hf_incoming").fetchone()[0]
    total_count = con.execute("SELECT COUNT(*) FROM trademarks").fetchone()[0]
    con.close()

    print(f"\n🎉 增量同步完成！本次吸纳/更新: {incoming_count} 条，数据库当前总量: {total_count} 条。")

# 命令行交互主入口
if __name__ == "__main__":
    choice = input('请选择操作模式：\n'
                   '1、根据最大/最小值从 DuckDB 检索并导出 CSV\n'
                   '2、从网络抓取最新数据并更新回写进 DuckDB\n'
                   '3、从 Hugging Face 下载增量包合并进 DuckDB\n'
                   '请输入选项 (1/2/3): ').strip()

    if choice == '1':
        start = input('请输入最小值（如 90000000）: \n').strip()
        end = input('请输入最大值（如 90001000）: \n').strip()
        query_duckdb_by_range(start, end)
    elif choice == '2':
        data = input('请输入需要在线更新的商标申请号（多个用英文逗号隔开，如 98689327,90007611）: \n').strip()
        rows = data.split(',')
        update_duckdb_trademarks(rows)
    # elif choice == '3':
    #     default_repo = "illeatmyhat/uspto-trademarks"
    #     default_file = "data/case_file/latest.parquet"

    #     repo = input(f"请输入 Hugging Face 数据集 Repo (直接回车默认: {default_repo}): \n").strip() or default_repo
    #     file_name = input(f"请输入仓库中的文件名 (直接回车默认: {default_file}): \n").strip() or default_file
    #     token = input("如果仓库是私有的，请输入 HF Token (公开仓库直接回车): \n").strip() or None

    #     update_from_huggingface(repo_id=repo, filename=file_name, hf_token=token)
    else:
        print("无效选项，退出程序。")