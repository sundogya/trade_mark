import os
import glob
import time
import random
import queue
import threading
import zoneinfo
from datetime import datetime
import requests
import duckdb
import yaml
import pandas as pd

# =====================================================================
# 1. 配置 Serverless 厂商网关与时区控制
# =====================================================================
with open("env.yaml", "r", encoding="utf-8") as f:
    config = yaml.safe_load(f)

# 读取 keychain 密钥配置
KEYCHAIN = config.get("keychain", "")
DEFAULT_HEADERS = {
    "x-auth-kimball": str(KEYCHAIN)
} if KEYCHAIN else {}

if KEYCHAIN:
    print(f"🔑 已加载 Keychain 鉴权密钥，将在请求中附带 x-auth-kimball 标头")

# 只提取开启了 enabled: true 的节点
active_nodes = [item for item in config.get("gateways", []) if item.get("enabled", True)]
GATEWAYS = [item["url"] for item in active_nodes]

# 同时直接生成名字映射字典
GATEWAY_NAMES = {item["url"]: item["name"] for item in active_nodes}
print(f"🔗 已启用 {len(GATEWAYS)} 个网关节点: {list(GATEWAY_NAMES.values())}")

DB_PATH = "cumulative_emails.duckdb"
BATCH_SIZE = 1000  # 每批处理 1000 条后深度冷却
CSV_DIR = "./csv"  # CSV 存放目录

# 数据库写锁与打印锁
db_lock = threading.Lock()
print_lock = threading.Lock()

# 美东时区 (自动兼容夏令时/冬令时)
EASTERN_TZ = zoneinfo.ZoneInfo("America/New_York")

def safe_log(msg):
    with print_lock:
        print(f"[{time.strftime('%H:%M:%S')}] {msg}")

def get_gateway_display_name(url):
    return GATEWAY_NAMES.get(url, "CustomGateway")

def wait_if_in_us_business_hours():
    """
    检查美东时间：如果在早上 07:30 到下午 16:30 之间，则阻塞休眠直到 16:30
    """
    while True:
        now_et = datetime.now(EASTERN_TZ)
        current_hour_float = now_et.hour + now_et.minute / 60.0
        # 7.5点到16.5点（包含 07:30:00 到 16:29:59）
        if 7.5 <= current_hour_float < 16.5:
            with print_lock:
                print(f"\n⏸️ [时段避让] 当前美东时间为 {now_et.strftime('%Y-%m-%d %H:%M:%S')} (处于高峰期 07:30 ~ 16:30)")
                print(f"⏳ 任务暂停中，所有 Gateway 已就地挂起，避免高峰期拥堵与 500 报错...")
            
            # 每隔 5 分钟检查一次，避免空转
            time.sleep(300)
        else:
            break

# =====================================================================
# 2. 单个网关工作线程任务
# =====================================================================
def worker_task(gw_idx, gateway_url, initial_targets, backlog_queue, con):
    gw_name = get_gateway_display_name(gateway_url)
    safe_log(f"🟢 节点 [{gw_name}] 启动，分配初始任务量: {len(initial_targets)} 条")

    consecutive_fail = 0
    MAX_CONSECUTIVE_FAIL = 3  # 单节点连续 3 次 429 或 403 触发自身下线保护
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
        # 每次发包前检查美东时间，如果在 09:00~17:00 则暂停等待
        wait_if_in_us_business_hours()

        try:
            # 在请求中带上 x-auth-kimball 鉴权 Header
            resp = requests.get(gateway_url, params={"sn": sn}, headers=DEFAULT_HEADERS, timeout=15)
            
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
            elif resp.status_code == 500:
                safe_log(f"❌ [{gw_name}] 申请号: {sn} 遭遇 500 服务器错误，任务回收待重试")
                backlog_queue.put(sn)  # 回收防止数据丢失
            else:
                safe_log(f"❌ [{gw_name}] 申请号: {sn} 状态异常 ({resp.status_code})")
        except Exception as e:
            safe_log(f"❌ [{gw_name}] 申请号: {sn} 请求失败: {e}")

        # 每 20 条批量刷入数据库
        if len(local_buffer) >= 20:
            flush_buffer()

        # 触发自身 429/403 熔断判定
        if consecutive_fail >= MAX_CONSECUTIVE_FAIL:
            safe_log(f"🚫 [{gw_name}] 触发频控/阻断熔断！放弃当前网关，回收剩余任务至公共队列...")
            gateway_healthy = False
            
            remaining_items = initial_targets[i:]
            for rem_sn in remaining_items:
                backlog_queue.put(rem_sn)
            break

        time.sleep(random.uniform(3, 5))

    flush_buffer()

    # 阶段二：若本节点健康，接管公共后备队列任务
    if gateway_healthy:
        safe_log(f"✅ [{gw_name}] 自身初始任务已跑完，检查公共后备队列...")
        while True:
            # 同样在接管取任务前做时段检查
            wait_if_in_us_business_hours()

            try:
                sn = backlog_queue.get_nowait()
            except queue.Empty:
                break

            try:
                # 在接管请求中同样带上 x-auth-kimball 鉴权 Header
                resp = requests.get(gateway_url, params={"sn": sn}, headers=DEFAULT_HEADERS, timeout=15)
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
                elif resp.status_code == 500:
                    safe_log(f"❌ [{gw_name} 接管时] 申请号: {sn} 遭遇 500 错误，任务重回队列")
                    backlog_queue.put(sn)
                else:
                    safe_log(f"❌ [{gw_name} 接管时] 申请号: {sn} 状态异常 ({resp.status_code})，任务重回队列")
                    backlog_queue.put(sn)
            except Exception as e:
                backlog_queue.put(sn)
                safe_log(f"❌ [{gw_name} 接管时] 异常: {e}，任务重回队列")

            if len(local_buffer) >= 20:
                flush_buffer()

            time.sleep(random.uniform(3, 5))

        flush_buffer()

    safe_log(f"🏁 节点 [{gw_name}] 任务生命周期结束。")

# =====================================================================
# 3. 调度总控（含 DuckDB 预检排重 + 批次冷却）
# =====================================================================
def run_pipeline(all_targets, batch_size=BATCH_SIZE):
    num_gateways = len(GATEWAYS)
    raw_total = len(all_targets)
    print(f"🚀 启动分组调度流水线，原始输入总量: {raw_total} 条，网关数: {num_gateways}")

    con = duckdb.connect(DB_PATH)
    
    # 1. 初始化表（若表已存在，自动补齐字段）
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
        print("✅ 当前文件所有目标数据均已存在于数据库中，无需抓取。")
        con.close()
        return True

    print(f"⚙️ 批次策略: 每处理 {batch_size} 条进入一次 5~8 分钟深度冷却，防止 IP 触发阻断")

    remaining_targets = list(filtered_targets)
    batch_num = 1

    while remaining_targets:
        # 启动批次前先检查时区，若在避让窗口内则提前等待
        wait_if_in_us_business_hours()

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

        # 回收因各种熔断未完成的任务
        leftovers = []
        while not backlog_queue.empty():
            try:
                leftovers.append(backlog_queue.get_nowait())
            except queue.Empty:
                break

        if leftovers:
            safe_log(f"⚠️ 本批次有 {len(leftovers)} 条任务因节点熔断/异常未完成，已回收至待处理队列。")
            remaining_targets = leftovers + remaining_targets

        end_count = con.execute("SELECT COUNT(*) FROM email_patch;").fetchone()[0]
        batch_new_saved = end_count - start_count
        print(f"🎉 第 {batch_num} 批次执行完成！本批新增入库: {batch_new_saved} 条，当前数据库累计: {end_count} 条。")

        if remaining_targets:
            rest_seconds = random.uniform(5 * 60, 8 * 60)
            rest_minutes = rest_seconds / 60
            print(f"\n😴 【批次休眠期】已完成一批数据，脚本全局断流休眠 {rest_minutes:.1f} 分钟 ({int(rest_seconds)} 秒)...")
            print(f"⏳ 正在冷却远端 WAF 滑动计数器，请勿中断程序...")
            time.sleep(rest_seconds)
            batch_num += 1

    total_saved = con.execute("SELECT COUNT(*) FROM email_patch;").fetchone()[0]
    con.close()

    print("\n" + "=" * 55)
    print(f"🏁 本次文件列表处理完毕！")
    print(f"📦 DuckDB 数据库最终有效记录数: {total_saved} 条")
    print("=" * 55)
    return True

# =====================================================================
# 4. 主入口：顺序扫描 ./csv/ 目录下所有 CSV，处理完后自动删除
# =====================================================================
if __name__ == "__main__":
    if not os.path.exists(CSV_DIR):
        os.makedirs(CSV_DIR, exist_ok=True)
        print(f"📁 未检测到 {CSV_DIR} 目录，已自动创建。请放入待抓取的 CSV 文件后重新运行。")
        exit(0)

    csv_files = sorted(glob.glob(os.path.join(CSV_DIR, "*.csv")))

    if not csv_files:
        print(f"ℹ️ 目录 {CSV_DIR} 下未发现任何待处理的 CSV 文件。")
        exit(0)

    print(f"📂 共检测到 {len(csv_files)} 个待处理 CSV 文件:")
    for idx, fpath in enumerate(csv_files, 1):
        print(f"   {idx}. {os.path.basename(fpath)}")

    for idx, fpath in enumerate(csv_files, 1):
        file_name = os.path.basename(fpath)
        print("\n" + "#" * 60)
        print(f"📑 >>> 正在处理文件 [{idx}/{len(csv_files)}]: {file_name}")
        print("#" * 60)

        try:
            df = pd.read_csv(fpath)
            if 'us_serial_number' in df.columns:
                targets = df['us_serial_number'].dropna().astype(str).tolist()
            elif 'serial_no' in df.columns:
                targets = df['serial_no'].dropna().astype(str).tolist()
            else:
                first_col = df.columns[0]
                print(f"⚠️ 未找到 'us_serial_number' 或 'serial_no' 列，自动使用首列 [{first_col}]")
                targets = df[first_col].dropna().astype(str).tolist()

            if not targets:
                print(f"⚠️ 文件 {file_name} 中无有效申请号数据，跳过并删除...")
                os.remove(fpath)
                continue

            success = run_pipeline(targets)

            if success:
                os.remove(fpath)
                time.sleep(60*2)  # 删除后等待 2 分钟，确保文件系统稳定
                print(f"🗑️ 文件 {file_name} 已处理入库完毕，源 CSV 文件已成功删除。")

        except Exception as e:
            print(f"❌ 处理文件 {file_name} 时发生异常: {e}")
            print(f"⚠️ 为防止数据丢失，保留原文件 {file_name}，继续处理后续任务。")

    print("\n" + "🎉" * 20)
    print("✨ 所有队列中的 CSV 文件已全部处理完毕！")