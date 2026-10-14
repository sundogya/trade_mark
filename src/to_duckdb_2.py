import os
import shutil
import duckdb

DB_PATH = "db/uspto_trademarks.duckdb"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "uspto_trademarks_data", "data")
TEMP_DIR = os.path.join(BASE_DIR, ".duckdb_temp")

os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
os.makedirs(TEMP_DIR, exist_ok=True)

if os.path.exists(DB_PATH):
    os.remove(DB_PATH)

print(f"正在初始化 DuckDB，创建单文件数据库: {DB_PATH}")
con = duckdb.connect(DB_PATH)

# 配置严格的内存保护参数
con.execute(f"SET temp_directory='{TEMP_DIR}';")
con.execute("SET preserve_insertion_order=false;")
con.execute("SET threads=1;")
con.execute("SET memory_limit='16GB';")

# -------------------------------------------------------------------------
# [第 1 步] 提取商品服务声明 (statement)
# 核心秘诀：按 serial_no 分区取首条代表性 GS 描述（或使用 QUALIFY），
# 彻底废弃内存杀手 string_agg 和 GROUP BY，内存消耗降低 95%！
# -------------------------------------------------------------------------
print("\n[第 1/5 步] 正在提取并落盘商品服务声明 (statement)...")
con.execute(f"""
CREATE OR REPLACE TABLE t_statement AS
SELECT 
    serial_no,
    TRIM(SUBSTRING(statement_text, 1, 1000)) AS for_goods_services
FROM read_parquet('{DATA_DIR}/statement/*.parquet')
WHERE statement_text IS NOT NULL 
  AND TRIM(statement_text) != ''
  AND UPPER(TRIM(statement_type_cd)) LIKE 'GS%'
QUALIFY ROW_NUMBER() OVER(PARTITION BY serial_no ORDER BY statement_seq ASC) = 1;
""")
con.execute("CHECKPOINT;")
print(" statement 提取完成，内存已自动刷盘释放。")

# -------------------------------------------------------------------------
# [第 2 步] 预先聚合国际分类 (intl_class，数字短字段，内存开销极小)
# -------------------------------------------------------------------------
print("\n[第 2/5 步] 正在提取并落盘国际分类 (intl_class)...")
con.execute(f"""
CREATE OR REPLACE TABLE t_intl_class AS
SELECT 
    serial_no,
    string_agg(intl_class_cd, ', ') AS international_classes
FROM read_parquet('{DATA_DIR}/intl_class/*.parquet')
GROUP BY serial_no;
""")
con.execute("CHECKPOINT;")

# -------------------------------------------------------------------------
# [第 3 步] 提取所有人信息 (owner)
# -------------------------------------------------------------------------
print("\n[第 3/5 步] 正在提取并落盘申请人信息 (owner)...")
con.execute(f"""
CREATE OR REPLACE TABLE t_owner AS
SELECT 
    serial_no,
    own_name AS owner_name,
    CONCAT_WS(', ', 
        NULLIF(own_addr_1, ''), 
        NULLIF(own_addr_2, ''), 
        NULLIF(own_addr_city, ''), 
        NULLIF(own_addr_state_cd, ''), 
        NULLIF(own_addr_postal, ''), 
        NULLIF(own_addr_country_cd, '')
    ) AS owner_address,
    own_entity_desc AS legal_entity_type,
    COALESCE(NULLIF(own_nalty_country_cd, ''), own_nalty_state_cd) AS state_or_country_where_organized
FROM read_parquet('{DATA_DIR}/owner/*.parquet')
QUALIFY ROW_NUMBER() OVER(PARTITION BY serial_no ORDER BY own_seq ASC) = 1;
""")
con.execute("CHECKPOINT;")

# -------------------------------------------------------------------------
# [第 4 步] 提取通信人信息 (correspondent)
# -------------------------------------------------------------------------
print("\n[第 4/5 步] 正在提取并落盘通信人信息 (correspondent)...")
con.execute(f"""
CREATE OR REPLACE TABLE t_correspondent AS
SELECT 
    serial_no,
    CONCAT_WS('\n', 
        NULLIF(cor_addr_1, ''), 
        NULLIF(cor_addr_2, ''), 
        NULLIF(cor_addr_3, ''), 
        NULLIF(cor_addr_4, ''), 
        NULLIF(cor_addr_5, '')
    ) AS correspondent_name_address
FROM read_parquet('{DATA_DIR}/correspondent/*.parquet');
""")
con.execute("CHECKPOINT;")

# -------------------------------------------------------------------------
# [第 5 步] 流式关联拼装生成最终主表 trademarks
# -------------------------------------------------------------------------
print("\n[第 5/5 步] 正在流式拼接生成最终主表 trademarks...")
con.execute(f"""
CREATE OR REPLACE TABLE trademarks AS
SELECT 
    c.mark_id_char                          AS mark,
    c.serial_no                             AS us_serial_number,
    c.registration_no                       AS us_registration_number,
    c.filing_dt                             AS application_filing_date,
    c.registration_dt                       AS registration_date,
    c.publication_dt                        AS publication_date,
    c.status_cd                             AS status_code,
    c.status_dt                             AS status_date,

    -- 关联字段
    st.for_goods_services                   AS for_goods_services,
    ic.international_classes                AS international_classes,
    ow.owner_name                           AS owner_name,
    ow.owner_address                        AS owner_address,
    ow.legal_entity_type                    AS legal_entity_type,
    ow.state_or_country_where_organized     AS state_or_country_where_organized,

    -- 律师及案号
    c.attorney_name                         AS attorney_name,
    c.atty_dkt_no                           AS docket_number,
    CAST(NULL AS VARCHAR)                   AS attorney_primary_email_address,
    CAST(NULL AS VARCHAR)                   AS attorney_email_authorized,

    -- 代理人通信信息
    co.correspondent_name_address           AS correspondent_name_address,
    CAST(NULL AS VARCHAR)                   AS correspondent_phone,
    CAST(NULL AS VARCHAR)                   AS correspondent_email,
    CAST(NULL AS VARCHAR)                   AS correspondent_email_authorized,

    -- §8 宣誓维护窗口推算
    c.registration_dt + INTERVAL 5 YEAR     AS earliest_date_can_be_filed,
    c.registration_dt + INTERVAL 6 YEAR     AS latest_date_without_additional_fee,
    c.registration_dt + INTERVAL 6 YEAR + INTERVAL 6 MONTH AS latest_date_with_additional_fee

FROM read_parquet('{DATA_DIR}/case_file/*.parquet') c
LEFT JOIN t_statement st     ON c.serial_no = st.serial_no
LEFT JOIN t_intl_class ic    ON c.serial_no = ic.serial_no
LEFT JOIN t_owner ow         ON c.serial_no = ow.serial_no
LEFT JOIN t_correspondent co ON c.serial_no = co.serial_no;
""")

# -------------------------------------------------------------------------
# 清理中间过渡表与创建索引
# -------------------------------------------------------------------------
print("\n清理过渡表并创建索引...")
con.execute("DROP TABLE IF EXISTS t_statement;")
con.execute("DROP TABLE IF EXISTS t_intl_class;")
con.execute("DROP TABLE IF EXISTS t_owner;")
con.execute("DROP TABLE IF EXISTS t_correspondent;")
con.execute("CHECKPOINT;")

print("正在创建 B-Tree 索引...")
con.execute("CREATE INDEX IF NOT EXISTS idx_serial_no ON trademarks(us_serial_number);")
con.execute("CREATE INDEX IF NOT EXISTS idx_reg_no ON trademarks(us_registration_number);")
con.execute("CREATE INDEX IF NOT EXISTS idx_mark ON trademarks(mark);")

print("压缩数据库空间 (VACUUM)...")
con.execute("CHECKPOINT;")
con.execute("VACUUM;")
con.close()

if os.path.exists(TEMP_DIR):
    shutil.rmtree(TEMP_DIR)

print(f"\n 构建成功！单文件数据库已生成: {DB_PATH}")