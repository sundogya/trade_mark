import os
import time
import random
import queue
import threading
import requests
import duckdb
import yaml

# =====================================================================
# 1. 配置 Serverless 厂商网关
# =====================================================================
with open("env.yaml", "r", encoding="utf-8") as f:
    config = yaml.safe_load(f)

# 只提取开启了 enabled: true 的节点
active_nodes = [item for item in config.get("gateways", []) if item.get("enabled", True)]
GATEWAYS = [item["url"] for item in active_nodes]

# 同时直接生成名字映射字典，连写正则/判断域名都省了
GATEWAY_NAMES = {item["url"]: item["name"] for item in active_nodes}
print(f"🔗 已启用 {len(GATEWAYS)} 个网关节点: {list(GATEWAY_NAMES.values())}")


DB_PATH = "cumulative_emails.duckdb"
BATCH_SIZE = 1000  # 每批处理 1000 条后深度冷却

# 数据库写锁与打印锁
db_lock = threading.Lock()
print_lock = threading.Lock()

def safe_log(msg):
    with print_lock:
        print(f"[{time.strftime('%H:%M:%S')}] {msg}")

def get_gateway_display_name(url):
    return GATEWAY_NAMES.get(url, "CustomGateway")
# =====================================================================
# 2. 单个网关工作线程任务
# =====================================================================
def worker_task(gw_idx, gateway_url, initial_targets, backlog_queue, con):
    gw_name = get_gateway_display_name(gateway_url)
    safe_log(f"🟢 节点 [{gw_name}] 启动，分配初始任务量: {len(initial_targets)} 条")

    consecutive_fail = 0
    MAX_CONSECUTIVE_FAIL = 3  # 连续 3 次 429 或 403 触发熔断保护
    gateway_healthy = True

    # 本线程私有数据缓冲，格式: (sn, correspondent_email, attorney_email)
    local_buffer = []

    def flush_buffer():
        if local_buffer:
            with db_lock:
                con.executemany("""
                INSERT INTO email_patch VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT (serial_no) DO UPDATE 
                SET correspondent_email = EXCLUDED.correspondent_email,
                    attorney_email = EXCLUDED.attorney_email,
                    crawl_time = EXCLUDED.crawl_time;
                """, local_buffer)
                con.execute("CHECKPOINT;")
            local_buffer.clear()

    # 阶段一：处理初始分配的任务组
    for i, sn in enumerate(initial_targets):
        try:
            resp = requests.get(gateway_url, params={"sn": sn}, timeout=15)
            
            if resp.status_code == 200:
                consecutive_fail = 0
                data = resp.json()
                corr_email = data.get("correspondent_email", "") or data.get("email", "")
                attorney_email = data.get("attorney_email", "")
                safe_log(f"[{gw_name}] 申请号: {sn} | 成功: True | 对应人: {corr_email or '无'} | 律师: {attorney_email or '无'}")
                if corr_email or attorney_email:
                    local_buffer.append((sn, corr_email, attorney_email))
            elif resp.status_code in (429, 403):
                consecutive_fail += 1
                safe_log(f"⚠️ [{gw_name}] 申请号: {sn} 遭遇状态码 {resp.status_code} (连续 {consecutive_fail}/{MAX_CONSECUTIVE_FAIL})")
            else:
                safe_log(f"❌ [{gw_name}] 申请号: {sn} 状态异常 ({resp.status_code})")
        except Exception as e:
            safe_log(f"❌ [{gw_name}] 申请号: {sn} 请求失败: {e}")

        # 每 20 条批量刷入数据库
        if len(local_buffer) >= 20:
            flush_buffer()

        # 触发熔断判定：如果该网关持续 429 / 403
        if consecutive_fail >= MAX_CONSECUTIVE_FAIL:
            safe_log(f"🚫 [{gw_name}] 触发连续阻断熔断！放弃当前网关，回收剩余任务至公共队列...")
            gateway_healthy = False
            
            remaining_items = initial_targets[i:]
            for rem_sn in remaining_items:
                backlog_queue.put(rem_sn)
            break

        # 保持健康间隔（2.0 ~ 3.5 秒）
        time.sleep(random.uniform(2.0, 3.5))

    flush_buffer()

    # 阶段二：如果本节点依然健康，继续帮忙消化从坏节点回退出来的任务
    if gateway_healthy:
        safe_log(f"✅ [{gw_name}] 自身初始任务已跑完，检查公共后备队列...")
        while True:
            try:
                sn = backlog_queue.get_nowait()
            except queue.Empty:
                break

            try:
                resp = requests.get(gateway_url, params={"sn": sn}, timeout=15)
                if resp.status_code == 200:
                    consecutive_fail = 0
                    data = resp.json()
                    corr_email = data.get("correspondent_email", "") or data.get("email", "")
                    attorney_email = data.get("attorney_email", "")
                    safe_log(f"[{gw_name} 接管] 申请号: {sn} | 成功: True | 对应人: {corr_email or '无'} | 律师: {attorney_email or '无'}")
                    if corr_email or attorney_email:
                        local_buffer.append((sn, corr_email, attorney_email))
                elif resp.status_code in (429, 403):
                    consecutive_fail += 1
                    backlog_queue.put(sn)
                    safe_log(f"⚠️ [{gw_name} 接管时] 遭遇状态码 {resp.status_code} (连续 {consecutive_fail}/{MAX_CONSECUTIVE_FAIL})")
                    if consecutive_fail >= MAX_CONSECUTIVE_FAIL:
                        safe_log(f"🚫 [{gw_name}] 在接管任务期间触发熔断，退出工作。")
                        break
            except Exception as e:
                backlog_queue.put(sn)
                safe_log(f"❌ [{gw_name} 接管时] 异常: {e}，任务重回队列")

            if len(local_buffer) >= 20:
                flush_buffer()

            time.sleep(random.uniform(2.0, 3.5))

        flush_buffer()

    safe_log(f"🏁 节点 [{gw_name}] 任务生命周期结束。")

# =====================================================================
# 3. 调度总控（含 DuckDB 预检排重 + 表结构平滑迁移）
# =====================================================================
def run_pipeline(all_targets, batch_size=BATCH_SIZE):
    num_gateways = len(GATEWAYS)
    raw_total = len(all_targets)
    print(f"🚀 启动分组调度流水线，原始输入总量: {raw_total} 条，网关数: {num_gateways}")

    con = duckdb.connect(DB_PATH)
    
    # 1. 初始化表（若表已存在，自动添加 attorney_email 字段）
    con.execute("""
    CREATE TABLE IF NOT EXISTS email_patch (
        serial_no VARCHAR PRIMARY KEY,
        correspondent_email VARCHAR,
        attorney_email VARCHAR,
        crawl_time TIMESTAMP
    );
    """)
    con.execute("""
    ALTER TABLE email_patch ADD COLUMN IF NOT EXISTS attorney_email VARCHAR;
    """)

    # 2. 启动前查重过滤
    print("🔍 正在检索 DuckDB 已有记录，执行启动前排重...")
    existing_rows = con.execute("SELECT serial_no FROM email_patch").fetchall()
    existing_set = {str(row[0]).strip() for row in existing_rows}

    filtered_targets = [str(sn).strip() for sn in all_targets if str(sn).strip() not in existing_set]
    skipped_count = raw_total - len(filtered_targets)

    print(f"📊 数据库比对结果: 已存在 {skipped_count} 条 (直接跳过)，实际待抓取新任务: {len(filtered_targets)} 条")

    if not filtered_targets:
        print("✅ 所有目标数据均已成功入库，无新增数据需要抓取，程序安全退出。")
        con.close()
        return

    print(f"⚙️ 批次策略: 每处理 {batch_size} 条进入一次 5~10 分钟深度冷却，防止 IP 触发 403 封锁")

    remaining_targets = list(filtered_targets)
    batch_num = 1

    while remaining_targets:
        current_batch = remaining_targets[:batch_size]
        remaining_targets = remaining_targets[batch_size:]

        start_count = con.execute("SELECT COUNT(*) FROM email_patch;").fetchone()[0]

        print(f"\n" + "=" * 55)
        print(f"▶️ 【第 {batch_num} 批次启动】 本批规模: {len(current_batch)} 条 | 剩余待排队: {len(remaining_targets)} 条")
        print("=" * 55)

        groups = [current_batch[i::num_gateways] for i in range(num_gateways)]
        for i, g in enumerate(groups):
            print(f"  * 组 {i} ({get_gateway_display_name(GATEWAYS[i])}): {len(g)} 条")

        backlog_queue = queue.Queue()

        threads = []
        for i in range(num_gateways):
            t = threading.Thread(
                target=worker_task,
                args=(i, GATEWAYS[i], groups[i], backlog_queue, con),
                name=f"Worker-{i}"
            )
            threads.append(t)
            t.start()
            time.sleep(0.4)

        for t in threads:
            t.join()

        leftovers = []
        while not backlog_queue.empty():
            try:
                leftovers.append(backlog_queue.get_nowait())
            except queue.Empty:
                break

        if leftovers:
            safe_log(f"⚠️ 本批次有 {len(leftovers)} 条任务因节点熔断未完成，已回收至待处理队列。")
            remaining_targets = leftovers + remaining_targets

        end_count = con.execute("SELECT COUNT(*) FROM email_patch;").fetchone()[0]
        batch_new_saved = end_count - start_count
        print(f"🎉 第 {batch_num} 批次执行完成！本批新增入库: {batch_new_saved} 条，当前数据库累计: {end_count} 条。")

        if remaining_targets:
            rest_seconds = random.uniform(5 * 60, 10 * 60)
            rest_minutes = rest_seconds / 60
            print(f"\n😴 【深度保护期】已完成一批数据，脚本全局断流休眠 {rest_minutes:.1f} 分钟 ({int(rest_seconds)} 秒)...")
            print(f"⏳ 正在冷却远端 WAF/Akamai 滑动计数器，请勿中断程序...")
            time.sleep(rest_seconds)
            batch_num += 1

    total_saved = con.execute("SELECT COUNT(*) FROM email_patch;").fetchone()[0]
    con.close()

    print("\n" + "=" * 55)
    print(f"🏁 全部任务列表已处理完毕！")
    print(f"📦 DuckDB 数据库最终有效记录数: {total_saved} 条")
    print("=" * 55)

if __name__ == "__main__":
    if os.path.exists("./csv/target_first_10yr_declaration_sns.csv"):
        import pandas as pd
        df = pd.read_csv("./csv/target_first_10yr_declaration_sns.csv")
        targets = df['us_serial_number'].astype(str).tolist()
    else:
        targets = [f"986893{i:02d}" for i in range(50)]

    run_pipeline(targets)