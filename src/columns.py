import duckdb
import os

data_dir = "uspto_trademarks_data/data"
tables = ["case_file", "owner", "intl_class", "correspondent","statement"]

for table in tables:
    parquet_path = f"{data_dir}/{table}/*.parquet"
    try:
        # DESCRIBE 会直接返回该表所有列的名称 (column_name) 和类型 (column_type)
        schema = duckdb.query(f"DESCRIBE SELECT * FROM '{parquet_path}'").df()
        print(f"\n================【 表名: {table} 】================")
        for _, row in schema.iterrows():
            print(f"  {row['column_name']:<25} ({row['column_type']})")
    except Exception as e:
        print(f"读取表 {table} 失败: {e}")