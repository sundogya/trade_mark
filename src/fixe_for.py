import os
import shutil
import duckdb

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "db", "uspto_trademarks.duckdb")
CLEAN_DB_PATH = os.path.join(BASE_DIR, "db", "uspto_trademarks_clean.duckdb")
DATA_DIR = os.path.join(BASE_DIR, "uspto_trademarks_data", "data")
TEMP_DIR = os.path.join(BASE_DIR, ".duckdb_temp")

os.makedirs(TEMP_DIR, exist_ok=True)
if os.path.exists(CLEAN_DB_PATH):
    os.remove(CLEAN_DB_PATH)

con = duckdb.connect()

# 严格配置安全运行参数
con.execute(f"SET temp_directory='{TEMP_DIR}';")
con.execute("SET preserve_insertion_order=false;")
con.execute("SET threads=2;")
con.execute("SET memory_limit='4500MB';")

print("正在提取有效商品服务声明 (GS)...")

# 1. 过滤有效 GS 文本落入纯内存临时表（单行截断至 300 字符）
con.execute(f"""
CREATE TEMPORARY TABLE temp_gs_all AS
SELECT 
    serial_no,
    statement_seq,
    TRIM(SUBSTRING(statement_text, 1, 300)) AS txt
FROM read_parquet('{DATA_DIR}/statement/*.parquet')
WHERE statement_text IS NOT NULL 
  AND TRIM(statement_text) != ''
  AND UPPER(TRIM(statement_type_cd)) LIKE 'GS%';
""")

# 2. 建立临时聚合表
con.execute("""
CREATE TEMPORARY TABLE temp_gs_agg (
    serial_no VARCHAR,
    for_goods_services VARCHAR
);
""")

# 3. 核心：分 20 批均匀哈希分片聚合，单批内存极低
print("正在执行分桶拼接（共分 20 批）...")
for bucket in range(20):
    print(f" -> 处理分片 [{bucket + 1}/20]...")
    con.execute(f"""
    INSERT INTO temp_gs_agg
    SELECT 
        serial_no,
        string_agg(txt, ' ; ' ORDER BY statement_seq ASC) AS for_goods_services
    FROM temp_gs_all
    WHERE abs(hash(serial_no)) % 20 = {bucket}
    GROUP BY serial_no;
    """)

con.execute("DROP TABLE IF EXISTS temp_gs_all;")

# 4. 挂载原库（只读）与纯净新库（避免原地修改导致的 16G 碎片膨胀）
print("正在流式写入纯净新库（消除 16G 膨胀空洞）...")
con.execute(f"ATTACH '{DB_PATH}' AS src (READ_ONLY);")
con.execute(f"ATTACH '{CLEAN_DB_PATH}' AS target;")

con.execute("""
CREATE TABLE target.trademarks AS
SELECT 
    t.mark,
    t.us_serial_number,
    t.us_registration_number,
    t.application_filing_date,
    t.registration_date,
    t.publication_date,
    t.status_code,
    t.status_date,
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
FROM src.trademarks t
LEFT JOIN temp_gs_agg a ON t.us_serial_number = a.serial_no
ORDER BY TRY_CAST(t.us_serial_number AS BIGINT) ASC;
""")

print("为新库重建轻量索引...")
con.execute("CREATE INDEX idx_serial_no ON target.trademarks(us_serial_number);")

con.execute("DETACH src;")
con.execute("DETACH target;")
con.close()

# 替换旧文件，回收所有磁盘碎片
old_size = os.path.getsize(DB_PATH) / (1024**3)
new_size = os.path.getsize(CLEAN_DB_PATH) / (1024**3)
os.replace(CLEAN_DB_PATH, DB_PATH)

if os.path.exists(TEMP_DIR):
    shutil.rmtree(TEMP_DIR, ignore_errors=True)

print(f"🎉 修复完成！文件物理大小: {old_size:.2f} GB -> 紧凑后: {new_size:.2f} GB（零空洞，内存安全）")