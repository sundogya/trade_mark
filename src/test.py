import duckdb
import pandas as pd

# 1. 配置各数据表的 Parquet 路径
BASE_DIR = "uspto_trademarks_data/data"
case_file_path = f"{BASE_DIR}/case_file/*.parquet"
owner_path = f"{BASE_DIR}/owner/*.parquet"
intl_class_path = f"{BASE_DIR}/intl_class/*.parquet"
correspondent_path = f"{BASE_DIR}/correspondent/*.parquet"

# 2. DuckDB 高性能关联查询
query = f"""
CREATE TABLE IF NOT EXISTS trademark_full AS
WITH 
-- 1. 国际分类聚合
agg_intl_class AS (
    SELECT 
        serial_no,
        string_agg(intl_class_cd, ', ') AS intl_classes
    FROM read_parquet('data/intl_class/*.parquet')
    GROUP BY serial_no
),

-- 2. 所有人信息 (取主所有人)
agg_owner AS (
    SELECT 
        serial_no,
        own_name,
        CONCAT_WS(', ', NULLIF(own_addr_1, ''), NULLIF(own_addr_2, ''), NULLIF(own_addr_city, ''), NULLIF(own_addr_state_cd, ''), NULLIF(own_addr_postal, ''), NULLIF(own_addr_country_cd, '')) AS own_full_address,
        own_entity_desc,
        own_nalty_country_cd
    FROM (
        SELECT *, ROW_NUMBER() OVER(PARTITION BY serial_no ORDER BY own_seq ASC) as rn
        FROM read_parquet('data/owner/*.parquet')
    )
    WHERE rn = 1
),

-- 3. 通信代理人/律师地址信息 (聚合地址行)
agg_correspondent AS (
    SELECT 
        serial_no,
        CONCAT_WS('\n', NULLIF(cor_addr_1, ''), NULLIF(cor_addr_2, ''), NULLIF(cor_addr_3, ''), NULLIF(cor_addr_4, ''), NULLIF(cor_addr_5, '')) AS correspondent_info
    FROM read_parquet('data/correspondent/*.parquet')
)

SELECT 
    -- 核心标识
    c.serial_no,
    c.registration_no,
    c.mark_id_char AS mark_name,
    c.filing_dt,
    c.registration_dt,
    c.status_cd,
    c.status_dt,
    c.publication_dt,
    
    -- 律师与代理人
    c.attorney_name,
    corr.correspondent_info,

    -- 维护期动态推算 (图中圈出的 §8 关键日期)
    c.registration_dt + INTERVAL 5 YEAR AS sec8_earliest_dt,               -- Earliest date §8 can be filed
    c.registration_dt + INTERVAL 6 YEAR AS sec8_latest_no_fee_dt,          -- Latest date §8 without additional fee
    c.registration_dt + INTERVAL 6 YEAR + INTERVAL 6 MONTH AS sec8_grace_dt, -- Latest date with fee (宽限期)

    -- 分类
    cls.intl_classes,

    -- 申请人信息
    o.own_name,
    o.own_full_address,
    o.own_entity_desc,
    o.own_nalty_country_cd

FROM read_parquet('data/case_file/*.parquet') c
LEFT JOIN agg_intl_class cls ON c.serial_no = cls.serial_no
LEFT JOIN agg_owner o ON c.serial_no = o.serial_no
LEFT JOIN agg_correspondent corr ON c.serial_no = corr.serial_no;
"""

print("[+] 正在检索最新 100 条商标的详细信息...")

try:
    df = duckdb.query(query).df()
    
    # 打印前 3 条结果至终端预览
    print(f"\n[✓] 检索完成！成功获取 {len(df)} 条最新数据。预览前 3 条：\n")
    for idx, row in df.head(3).iterrows():
        print(f"--- [No.{idx+1}] {row['Mark']} (申请号: {row['US Serial Number']}) ---")
        for col in df.columns:
            print(f"  {col}: {row[col]}")
        print()

    # 导出为 CSV，方便在 VS Code 或 Numbers/Excel 中直接点开查看
    output_csv = "latest_100_trademarks.csv"
    df.to_csv(output_csv, index=False, encoding="utf-8-sig")
    print(f"[✓] 100 条最新数据已保存至: {output_csv}")

except Exception as e:
    print(f"[-] 查询发生异常: {e}")