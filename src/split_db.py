import os
import duckdb

def split_duckdb_table(source_db, table_name, target_dir, chunk_size=2000000):
    """
    将指定的 DuckDB 表按照行数切分导出到多个不超过 1GB 的新 .duckdb 文件中
    :param source_db: 源 .duckdb 文件路径
    :param table_name: 需要切分的表名
    :param target_dir: 存放切分后文件的目标文件夹
    :param chunk_size: 每批次导出的行数（根据单行数据大小调整，200万行约几百MB）
    """
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)

    # 1. 连接源数据库
    src_conn = duckdb.connect(source_db, read_only=True)
    
    # 获取总行数
    total_rows = src_conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    print(f"📡 表 '{table_name}' 总行数: {total_rows}")

    offset = 0
    file_index = 1

    # 2. 分批读取并写入新数据库
    while offset < total_rows:
        target_db_path = os.path.join(target_dir, f"split_{file_index}.duckdb")
        print(f"📦 正在生成子数据库 ({file_index}): {target_db_path} ...")
        
        # 创建新的子数据库连接
        tgt_conn = duckdb.connect(target_db_path)
        
        # 使用 LIMIT 和 OFFSET 分批流式将数据写入新表的创建语句中
        query = f"""
            CREATE TABLE {table_name} AS 
            SELECT * FROM src_conn.{table_name} 
            LIMIT {chunk_size} OFFSET {offset}
        """
        
        # 在新连接中，通过注册源连接的方式跨库读取
        # tgt_conn.execute(f"ATTACH '{source_db}' AS src_db READ_ONLY")
        # tgt_conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM src_db.{table_name} LIMIT {chunk_size} OFFSET {offset}")
        # tgt_conn.execute("DETACH src_db")
        # 🟢 Correct syntax: (READ_ONLY) goes inside parentheses
        tgt_conn.execute(f"ATTACH '{source_db}' AS src_db (READ_ONLY)")
        
        # Copy the specific chunk over
        tgt_conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM src_db.{table_name} LIMIT {chunk_size} OFFSET {offset}")
        
        tgt_conn.execute("DETACH src_db")

        
        # 检查新生成的物理文件大小
        tgt_conn.close()
        file_size_gb = os.path.getsize(target_db_path) / (1024 ** 3)
        print(f"   └─ 完成！文件大小: {file_size_gb:.2f} GB")

        offset += chunk_size
        file_index += 1

    src_conn.close()
    print("🎉 所有切分文件处理完毕！")

# —— 使用示例 ——
if __name__ == "__main__":
    
    # SOURCE_DATABASE = "db/uspto_trademarks.duckdb"  # 你的9GB大文件
    TABLE_TO_SPLIT = "trademarks"         # 里面的核心表名
    # OUTPUT_DIRECTORY = "db/split_files"         # 存放切分文件的目录

    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    SRC_DB = os.path.join(BASE_DIR, "db", "uspto_trademarks.duckdb")
    SPLIT_DIR = os.path.join(BASE_DIR, "db", "split_files")
    
    # 调整 chunk_size。如果单行字段极多，可以缩小到 1000000（100万行）
    split_duckdb_table(SRC_DB, TABLE_TO_SPLIT, SPLIT_DIR, chunk_size=2500000)
