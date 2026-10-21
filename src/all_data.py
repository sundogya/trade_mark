import shutil
import duckdb
shutil.copyfile("cumulative_emails.duckdb", "temp_read.duckdb")
con = duckdb.connect("temp_read.duckdb", read_only=True)
# 1. 连接到本地 DuckDB 数据库文件（只读模式 read_only=True 可以防止误修改）
# con = duckdb.connect("cumulative_emails.duckdb", read_only=True)

# 2. 查看当前数据库中的所有表
tables = con.execute("SHOW TABLES").fetchall()
table_names = [row[0] for row in tables]
print(f"包含的表: {table_names}")

# 3. 遍历查询所有表的数据
for table in table_names:
    print(f"\n--- 表名: {table} ---")

    # 方式 A：转为 Pandas DataFrame（适合数据分析和查看）
    df = con.execute(f"SELECT * FROM {table}").df()
    print(df)

    # 方式 B：获取原生元组列表
    # rows = con.execute(f"SELECT * FROM {table}").fetchall()
    # print(rows)

# 4. 关闭连接
con.close()