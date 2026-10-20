import os
import time
import random
import queue
import threading
import requests
import duckdb

# =====================================================================
# 1. 配置 Serverless 厂商网关
# =====================================================================
GATEWAYS = [
    "https://usp-deploy.sundogya1.workers.dev",           # 0. Cloudflare Workers 1
    "https://tight-paper-27cd.ynt7pbgrrz.workers.dev",  # 1. Cloudflare Workers 2
    "https://usedeploy-ruby.vercel.app/api",              # 2. Vercel Functions 1
    "https://usedeploy-222.vercel.app/api",               # 3. Vercel Functions 2
    "https://usp-deploy-333.vercel.app/api",               # 4. Vercel Functions 3
    "https://eloquent-starburst-2a52a5.netlify.app/api",  # 5. Netlify Functions
    "https://fumypwsylxatcmhjsimu.supabase.co/functions/v1/usp-deploy", # 6. Supabase Functions
    "https://hgxevimeaevdjuqonteu.supabase.co/functions/v1/use-deploy", # 7. Supabase Functions
    "https://v7yxtgs03z4dz1r6.qt4.lambda-web.us-west-1.on.aws", # 8. AWS Lambda Web
    # "https://txntbf9gfbdx50qb.qt4.lambda-web.eu-west-2.on.aws", # 9. AWS Lambda Web
    # "https://pqz1799rg7vpqhj5.qt4.lambda-web.ca-central-1.on.aws", # 10. AWS Lambda Web
    "https://7cnkjc6bc0n6rtrl.qt4.lambda-web.us-east-1.on.aws",  # 11. AWS Lambda Web
]

DB_PATH = "cumulative_emails.duckdb"

# 数据库写锁与打印锁
db_lock = threading.Lock()
print_lock = threading.Lock()

def safe_log(msg):
    with print_lock:
        print(f"[{time.strftime('%H:%M:%S')}] {msg}")

def get_gateway_display_name(url):
    if "usp-deploy.sundogya1" in url:
        return "Cloudflare-1"
    elif "tight-paper-27cd.ynt7pbgrrz.workers.dev" in url:
        return "Cloudflare-2"
    elif "usp-deploy-333" in url:
        return "Vercel-3"
    elif "usedeploy-222" in url:
        return "Vercel-2"
    elif "usedeploy-ruby" in url:
        return "Vercel-1"
    elif "netlify.app" in url:
        return "Netlify"
    elif "hgxevimeaevdjuqonteu" in url:   
        return "Supabase-2"
    elif "supabase.co" in url:
        return "Supabase-1"
    elif "lambda-web.us-west-1.on.aws" in url:
        return "AWS-Lambda-1"
    elif "lambda-web.eu-west-2.on.aws" in url:
        return "AWS-Lambda-2"
    elif "lambda-web.ca-central-1.on.aws" in url:
        return "AWS-Lambda-3"
    elif "lambda-web.us-east-1.on.aws" in url:
        return "AWS-Lambda-4"
    return "CustomGateway"

# =====================================================================
# 2. 单个网关工作线程任务
# =====================================================================
def worker_task(gw_idx, gateway_url, initial_targets, backlog_queue, con):
    gw_name = get_gateway_display_name(gateway_url)
    safe_log(f"🟢 节点 [{gw_name}] 启动，分配初始任务量: {len(initial_targets)} 条")

    consecutive_429 = 0
    MAX_CONSECUTIVE_429 = 3  # 连续 3 次 429 即判定该网关不可用
    gateway_healthy = True

    # 本线程私有数据缓冲
    local_buffer = []

    def flush_buffer():
        if local_buffer:
            with db_lock:
                con.executemany("""
                INSERT INTO email_patch VALUES (?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT (serial_no) DO UPDATE 
                SET correspondent_email = EXCLUDED.correspondent_email,
                    crawl_time = EXCLUDED.crawl_time;
                """, local_buffer)
                con.execute("CHECKPOINT;")
            local_buffer.clear()

    # 阶段一：处理初始分配的任务组
    for i, sn in enumerate(initial_targets):
        try:
            resp = requests.get(gateway_url, params={"sn": sn}, timeout=15)
            
            if resp.status_code == 200:
                consecutive_429 = 0
                data = resp.json()
                email = data.get("correspondent_email", "") or data.get("email", "")
                safe_log(f"[{gw_name}] 申请号: {sn} | 成功: True | 邮箱: {email or '无'}")
                if email:
                    local_buffer.append((sn, email))
            elif resp.status_code == 429:
                consecutive_429 += 1
                safe_log(f"⚠️ [{gw_name}] 申请号: {sn} 遭遇 429 (连续 {consecutive_429}/{MAX_CONSECUTIVE_429})")
            else:
                safe_log(f"❌ [{gw_name}] 申请号: {sn} 状态异常 ({resp.status_code})")
        except Exception as e:
            safe_log(f"❌ [{gw_name}] 申请号: {sn} 请求失败: {e}")

        # 每 20 条批量刷入数据库
        if len(local_buffer) >= 20:
            flush_buffer()

        # 触发熔断判定：如果该网关持续 429
        if consecutive_429 >= MAX_CONSECUTIVE_429:
            safe_log(f"🚫 [{gw_name}] 触发连续 429 熔断！放弃当前网关，回收剩余任务至公共队列...")
            gateway_healthy = False
            
            # 将当前未成功的这一条以及后面所有未执行的条目推入公共后备队列
            remaining_items = initial_targets[i:]
            for rem_sn in remaining_items:
                backlog_queue.put(rem_sn)
            break

        # 每次请求保持严格的 3~5 秒安全间隔
        time.sleep(random.uniform(2.0, 3.5))

    flush_buffer()

    # 阶段二：如果本节点依然健康，继续帮忙消化从其他坏节点回退出来的后备任务
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
                    consecutive_429 = 0
                    data = resp.json()
                    email = data.get("correspondent_email", "") or data.get("email", "")
                    safe_log(f"[{gw_name} 接管] 申请号: {sn} | 成功: True | 邮箱: {email or '无'}")
                    if email:
                        local_buffer.append((sn, email))
                elif resp.status_code == 429:
                    consecutive_429 += 1
                    # 重新将任务推回队列让其他健康节点处理
                    backlog_queue.put(sn)
                    safe_log(f"⚠️ [{gw_name} 接管时] 遭遇 429 (连续 {consecutive_429}/{MAX_CONSECUTIVE_429})")
                    if consecutive_429 >= MAX_CONSECUTIVE_429:
                        safe_log(f"🚫 [{gw_name}] 在接管任务期间也触发熔断，退出工作。")
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
# 3. 调度总控
# =====================================================================
def run_pipeline(all_targets):
    num_gateways = len(GATEWAYS)
    print(f"🚀 启动分组调度流水线，总量: {len(all_targets)} 条，网关数: {num_gateways}")

    con = duckdb.connect(DB_PATH)
    con.execute("""
    CREATE TABLE IF NOT EXISTS email_patch (
        serial_no VARCHAR PRIMARY KEY,
        correspondent_email VARCHAR,
        crawl_time TIMESTAMP
    );
    """)

    # 1. 均匀拆分为 N 个子组（交错切片取模，避免号段聚集）
    groups = [all_targets[i::num_gateways] for i in range(num_gateways)]
    for i, g in enumerate(groups):
        print(f"  * 组 {i} ({get_gateway_display_name(GATEWAYS[i])}): {len(g)} 条")

    # 2. 公共任务回退队列
    backlog_queue = queue.Queue()

    # 3. 并行启动 N 个独立工作线程
    threads = []
    for i in range(num_gateways):
        t = threading.Thread(
            target=worker_task,
            args=(i, GATEWAYS[i], groups[i], backlog_queue, con),
            name=f"Worker-{i}"
        )
        threads.append(t)
        t.start()
        # 启动时微错峰 0.5s，消除初始并发冲击
        time.sleep(0.5)

    # 等待全部工作线程完成
    for t in threads:
        t.join()

    # 4. 汇总统计
    leftover = backlog_queue.qsize()
    total_saved = con.execute("SELECT COUNT(*) FROM email_patch;").fetchone()[0]
    con.close()

    print("\n" + "=" * 50)
    print(f"🎉 全部批次执行完毕！")
    print(f"📦 数据库累计有效记录总数: {total_saved} 条")
    if leftover > 0:
        print(f"⚠️ 注意: 因所有可用节点均被限流，仍有 {leftover} 条任务滞留于后备队列中，可等待 30 分钟冷却后重跑。")
    print("=" * 50)

if __name__ == "__main__":
    if os.path.exists("./csv/target_first_10yr_declaration_sns.csv"):
        import pandas as pd
        df = pd.read_csv("./csv/target_first_10yr_declaration_sns.csv")
        targets = df['us_serial_number'].astype(str).tolist()
    else:
        targets = [f"986893{i:02d}" for i in range(50)]

    run_pipeline(targets)