import duckdb
import os
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
src_db = os.path.join(BASE_DIR, "db", "uspto_trademarks.duckdb")
compact_db = os.path.join(BASE_DIR, "db", "uspto_trademarks_clean.duckdb")
# src_db = "db/uspto_trademarks.duckdb"
# compact_db = "db/uspto_trademarks_clean.duckdb"

print("正在对 15GB+ 数据库进行物理脱水与空间回收...")
con = duckdb.connect()

# 挂载原库和纯净新库
con.execute(f"ATTACH '{src_db}' AS src (READ_ONLY);")
con.execute(f"ATTACH '{compact_db}' AS target;")

# 物理紧凑复制：按申请号排序重写（这会让 DuckDB 原生字典压缩率发挥到极致）
con.execute("""
CREATE TABLE target.trademarks AS 
SELECT * FROM src.trademarks 
ORDER BY TRY_CAST(us_serial_number AS BIGINT) ASC;
""")

# 重建轻量索引
print("重建索引...")
con.execute("CREATE INDEX idx_serial_no ON target.trademarks(us_serial_number);")
con.execute("CREATE INDEX idx_reg_no ON target.trademarks(us_registration_number);")
con.execute("CREATE INDEX idx_mark ON target.trademarks(mark);")

con.execute("DETACH src;")
con.execute("DETACH target;")
con.close()

# 替换旧文件
old_size = os.path.getsize(src_db) / (1024**3)
new_size = os.path.getsize(compact_db) / (1024**3)
os.replace(compact_db, src_db)

print(f"🎉 瘦身完成！原大小: {old_size:.2f} GB -> 瘦身后大小: {new_size:.2f} GB")