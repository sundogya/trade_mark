import os
import duckdb

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "db", "uspto_trademarks.duckdb")
DATA_DIR = os.path.join(BASE_DIR, "uspto_trademarks_data", "data")
TEMP_DIR = os.path.join(BASE_DIR, ".duckdb_temp")

os.makedirs(TEMP_DIR, exist_ok=True)

con = duckdb.connect(DB_PATH)
con.execute(f"SET temp_directory='{TEMP_DIR}';")
con.execute("SET preserve_insertion_order=false;")
con.execute("SET threads=1;")
con.execute("SET memory_limit='5GB';")

print("正在提取并拼接完整的商品服务声明 (GS)...")

# 1. 提取所有有效 GS 描述并先落盘物理小表
con.execute(f"""
CREATE OR REPLACE TABLE temp_gs_all AS
SELECT 
    serial_no,
    statement_seq,
    TRIM(SUBSTRING(statement_text, 1, 500)) AS txt
FROM read_parquet('{DATA_DIR}/statement/*.parquet')
WHERE statement_text IS NOT NULL 
  AND TRIM(statement_text) != ''
  AND UPPER(TRIM(statement_type_cd)) LIKE 'GS%';
""")
con.execute("CHECKPOINT;")

# 2. 聚合生成带完整拼接的临时表
print("正在聚合文本...")
con.execute("""
CREATE OR REPLACE TABLE temp_gs_agg AS
SELECT 
    serial_no,
    string_agg(txt, ' ; ' ORDER BY statement_seq ASC) AS for_goods_services
FROM temp_gs_all
GROUP BY serial_no;
""")
con.execute("DROP TABLE IF EXISTS temp_gs_all;")
con.execute("CHECKPOINT;")

# 3. 核心改进：用流式 LEFT JOIN 重建表替代原先会导致 OOM 的 UPDATE
print("正在流式合并回主表（零内存开销方式）...")
con.execute("""
CREATE OR REPLACE TABLE trademarks_new AS
SELECT 
    t.mark,
    t.us_serial_number,
    t.us_registration_number,
    t.application_filing_date,
    t.registration_date,
    t.publication_date,
    t.status_code,
    t.status_date,
    -- 填入刚刚聚合好的 GS 描述，没有匹配的保留 NULL
    COALESCE(a.for_goods_services, t.for_goods_services) AS for_goods_services,
    t.international_classes,
    t.owner_name,
    t.owner_address,
    t.legal_entity_type,
    t.state_or_country_where_organized,
    t.attorney_name,
    t.docket_number,
    t.attorney_primary_email_address,
    t.attorney_email_authorized,
    t.correspondent_name_address,
    t.correspondent_phone,
    t.correspondent_email,
    t.correspondent_email_authorized,
    t.earliest_date_can_be_filed,
    t.latest_date_without_additional_fee,
    t.latest_date_with_additional_fee
FROM trademarks t
LEFT JOIN temp_gs_agg a ON t.us_serial_number = a.serial_no;
""")

# 4. 替换原表并清理临时表
con.execute("DROP TABLE IF EXISTS trademarks;")
con.execute("DROP TABLE IF EXISTS temp_gs_agg;")
con.execute("ALTER TABLE trademarks_new RENAME TO trademarks;")

# 5. 重建主键索引并刷盘
con.execute("CREATE INDEX IF NOT EXISTS idx_serial_no ON trademarks(us_serial_number);")
con.execute("CHECKPOINT;")
con.close()

print(" 完整商品服务描述对齐修复完成！")