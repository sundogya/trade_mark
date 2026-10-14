import os
import duckdb

# 1. 配置路径
DB_PATH = "uspto_trademarks.duckdb"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "uspto_trademarks_data", "data")
TEMP_DIR = os.path.join(BASE_DIR, ".duckdb_temp")

os.makedirs(TEMP_DIR, exist_ok=True)

print(f"正在初始化 DuckDB，创建/连接文件: {DB_PATH}")
con = duckdb.connect(DB_PATH)

# 2. 关键内存与外存交换配置
con.execute(f"SET temp_directory='{TEMP_DIR}';")       # 允许内存溢出时借用硬盘空间
con.execute("SET preserve_insertion_order=false;")     # 关闭物理插入保序，大幅削减聚合与关联内存
con.execute("SET threads=2;")                          # 降到 2 线程，减少多线程缓冲区的内存翻倍
con.execute("SET memory_limit='6GB';")                 # 根据你的 Mac 内存调整，写 6GB 或 8GB

print("\n[第 1/4 步] 写入 case_file 主表数据及宣誓日期推算...")
con.execute(f"""
CREATE OR REPLACE TABLE trademarks AS
SELECT 
    mark_id_char                          AS mark,
    serial_no                             AS us_serial_number,
    registration_no                       AS us_registration_number,
    filing_dt                             AS application_filing_date,
    registration_dt                       AS registration_date,
    publication_dt                        AS publication_date,
    status_cd                             AS status_code,
    status_dt                             AS status_date,
    
    -- 律师及案号
    attorney_name                         AS attorney_name,
    atty_dkt_no                           AS docket_number,
    CAST(NULL AS VARCHAR)                 AS attorney_primary_email_address,
    CAST(NULL AS VARCHAR)                 AS attorney_email_authorized,

    -- 预留待填充字段
    CAST(NULL AS VARCHAR)                 AS for_goods_services,
    CAST(NULL AS VARCHAR)                 AS international_classes,
    CAST(NULL AS VARCHAR)                 AS owner_name,
    CAST(NULL AS VARCHAR)                 AS owner_address,
    CAST(NULL AS VARCHAR)                 AS legal_entity_type,
    CAST(NULL AS VARCHAR)                 AS state_or_country_where_organized,
    CAST(NULL AS VARCHAR)                 AS correspondent_name_address,
    CAST(NULL AS VARCHAR)                 AS correspondent_phone,
    CAST(NULL AS VARCHAR)                 AS correspondent_email,
    CAST(NULL AS VARCHAR)                 AS correspondent_email_authorized,

    -- §8 维护窗口动态推算
    registration_dt + INTERVAL 5 YEAR     AS earliest_date_can_be_filed,
    registration_dt + INTERVAL 6 YEAR     AS latest_date_without_additional_fee,
    registration_dt + INTERVAL 6 YEAR + INTERVAL 6 MONTH AS latest_date_with_additional_fee
FROM read_parquet('{DATA_DIR}/case_file/*.parquet');
""")

print("[第 2/4 步] 正在关联聚合国际分类 (intl_class)...")
con.execute(f"""
UPDATE trademarks
SET international_classes = sub.classes
FROM (
    SELECT 
        serial_no,
        string_agg(intl_class_cd, ', ') AS classes
    FROM read_parquet('{DATA_DIR}/intl_class/*.parquet')
    GROUP BY serial_no
) sub
WHERE trademarks.us_serial_number = sub.serial_no;
""")

print("[第 3/4 步] 正在关联申请人/所有人信息 (owner)...")
con.execute(f"""
UPDATE trademarks
SET 
    owner_name = sub.own_name,
    owner_address = sub.owner_address,
    legal_entity_type = sub.own_entity_desc,
    state_or_country_where_organized = sub.org_country
FROM (
    SELECT 
        serial_no,
        own_name,
        CONCAT_WS(', ', 
            NULLIF(own_addr_1, ''), 
            NULLIF(own_addr_2, ''), 
            NULLIF(own_addr_city, ''), 
            NULLIF(own_addr_state_cd, ''), 
            NULLIF(own_addr_postal, ''), 
            NULLIF(own_addr_country_cd, '')
        ) AS owner_address,
        own_entity_desc,
        COALESCE(NULLIF(own_nalty_country_cd, ''), own_nalty_state_cd) AS org_country
    FROM (
        SELECT *, ROW_NUMBER() OVER(PARTITION BY serial_no ORDER BY own_seq ASC) as rn
        FROM read_parquet('{DATA_DIR}/owner/*.parquet')
    )
    WHERE rn = 1
) sub
WHERE trademarks.us_serial_number = sub.serial_no;
""")

print("[第 4/4 步] 正在关联代理人通信地址 (correspondent)...")
con.execute(f"""
UPDATE trademarks
SET correspondent_name_address = sub.corr_addr
FROM (
    SELECT 
        serial_no,
        CONCAT_WS('\n', 
            NULLIF(cor_addr_1, ''), 
            NULLIF(cor_addr_2, ''), 
            NULLIF(cor_addr_3, ''), 
            NULLIF(cor_addr_4, ''), 
            NULLIF(cor_addr_5, '')
        ) AS corr_addr
    FROM read_parquet('{DATA_DIR}/correspondent/*.parquet')
) sub
WHERE trademarks.us_serial_number = sub.serial_no;
""")

print("\n数据写入完成！正在创建毫秒级查询索引...")
con.execute("CREATE INDEX IF NOT EXISTS idx_serial_no ON trademarks(us_serial_number);")
con.execute("CREATE INDEX IF NOT EXISTS idx_reg_no ON trademarks(us_registration_number);")
con.execute("CREATE INDEX IF NOT EXISTS idx_mark ON trademarks(mark);")

print("正在落盘清理...")
con.execute("CHECKPOINT;")
con.execute("VACUUM;")

con.close()

# 清理临时缓存文件夹
import shutil
if os.path.exists(TEMP_DIR):
    shutil.rmtree(TEMP_DIR)

print(f"\n 全部完成！已生成轻量高效的单文件数据库: {DB_PATH}")