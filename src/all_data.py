import shutil
import duckdb

# 制作副本以防主库被锁或冲突
shutil.copyfile("cumulative_emails.duckdb", "temp_read.duckdb")
# shutil.copyfile("./db/split_files/split_1.duckdb", "temp_read.duckdb")
con = duckdb.connect("temp_read.duckdb", read_only=True)

# 1. 查看当前数据库中的所有表
tables = con.execute("SHOW TABLES").fetchall()
table_names = [row[0] for row in tables]
print(f"包含的表: {table_names}\n")

# 2. 遍历查询所有表的结构和数据
for table in table_names:
    print("=" * 60)
    print(f"📋 表名: {table}")
    print("=" * 60)

    # 打印表结构（字段名、数据类型、是否允许NULL、Key等）
    print("\n[ 字段结构 (Schema) ]")
    schema_df = con.execute(f"DESCRIBE {table}").df()
    print(schema_df.to_string(index=False))

    # 打印数据预览
    print("\n[ 数据预览 (前几行) ]")
    df = con.execute(f"SELECT * FROM {table}").df()
    print(df)
    print()

# 3. 关闭连接
con.close()