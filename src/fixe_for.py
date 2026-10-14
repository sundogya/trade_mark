import os
import duckdb

DB_PATH = "db/uspto_trademarks.duckdb"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "uspto_trademarks_data", "data")
TEMP_DIR = os.path.join(BASE_DIR, ".duckdb_temp")

con = duckdb.connect(DB_PATH)
con.execute(f"SET temp_directory='{TEMP_DIR}';")
con.execute("SET preserve_insertion_order=false;")
con.execute("SET threads=1;")
con.execute("SET memory_limit='16GB';")

print("正在提取并拼接完整的商品服务声明 (GS)...")

# 1. 提取所有属于 GS 的有效描述，按 serial_no 和 statement_seq 预排序
# 这里先写入物理临时表，由磁盘接管，避免内存膨胀
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

# 2. 对已落盘的纯净小表进行拼接（保留所有分类/所有条款，与官网对齐）
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

# 3. 流式写回主表
print("正在更新回 trademarks 主表...")
con.execute("""
UPDATE trademarks
SET for_goods_services = a.for_goods_services
FROM temp_gs_agg a
WHERE trademarks.us_serial_number = a.serial_no;
""")

con.execute("DROP TABLE IF EXISTS temp_gs_agg;")
con.execute("CHECKPOINT;")
con.close()
print("完整商品服务描述对齐修复完成！")