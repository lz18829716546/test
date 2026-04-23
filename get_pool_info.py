#!/usr/bin/env python3
import json
import subprocess
import requests

PROM_URL = "http://172.16.3.107:9095"

def run_cmd(cmd):
    return subprocess.check_output(cmd, shell=True).decode()

# ---------------------------
# 获取 pool 基础信息 + percent_used
# ---------------------------
def get_pools():
    df = json.loads(run_cmd("ceph df detail -f json"))
    pools = {}

    for p in df["pools"]:
        pools[p["id"]] = {
            "pool_id": p["id"],
            "pool_name": p["name"],
            "objects": p["stats"]["objects"],
            "%USED": round(p["stats"]["percent_used"] * 100, 4)
        }

    return pools

# ---------------------------
# 获取 pool -> crush_rule
# ---------------------------
def get_pool_rule_map():
    data = json.loads(run_cmd("ceph osd pool ls detail -f json"))
    m = {}
    for p in data:
        m[p["pool"]] = p["crush_rule"]
    return m

# ---------------------------
# 获取 crush_rule -> device_class
# ---------------------------
def get_rule_class_map():
    rules = json.loads(run_cmd("ceph osd crush rule dump -f json"))
    m = {}

    for r in rules:
        rule_id = r["rule_id"]
        device_class = "unknown"

        for step in r["steps"]:
            if step["op"] == "take":
                device_class = step.get("item_name", "unknown")

        m[rule_id] = device_class

    return m

# ---------------------------
# Prometheus 查询
# ---------------------------
def prom_query(query):
    resp = requests.get(
        f"{PROM_URL}/api/v1/query",
        params={"query": query}
    ).json()

    result = {}

    if resp["status"] != "success":
        return result

    for item in resp["data"]["result"]:
        pool_id = int(item["metric"].get("pool_id", -1))
        value = float(item["value"][1])
        result[pool_id] = value

    return result

# ---------------------------
# 获取性能
# ---------------------------
def get_perf():
    iops_q = '''
avg_over_time((sum by (pool_id)
(rate(ceph_pool_rd[5m]) + rate(ceph_pool_wr[5m])))[12h:])
'''

    bw_q = '''
avg_over_time((sum by (pool_id)
(rate(ceph_pool_rd_bytes[5m]) + rate(ceph_pool_wr_bytes[5m])))[12h:])
'''

    latency_q = '''
sum(rate(ceph_osd_op_latency_sum[5m])) /
sum(rate(ceph_osd_op_latency_count[5m]))
'''

    iops = prom_query(iops_q)
    bw = prom_query(bw_q)

    latency_resp = requests.get(
        f"{PROM_URL}/api/v1/query",
        params={"query": latency_q}
    ).json()

    latency = None
    if latency_resp["data"]["result"]:
        latency = float(latency_resp["data"]["result"][0]["value"][1]) * 1000

    return iops, bw, latency

# ---------------------------
# 主逻辑
# ---------------------------
def main():
    pools = get_pools()
    pool_rule = get_pool_rule_map()
    rule_class = get_rule_class_map()
    iops, bw, latency = get_perf()

    result = {
        "cluster": "test-cluster",
        "pools": []
    }

    for pid, p in pools.items():
        rule_id = pool_rule.get(pid)
        device_class = rule_class.get(rule_id, "unknown")

        p["device_class"] = device_class

        p["performance"] = {
            "iops_12h": iops.get(pid, 0),
            "bw_bytes_12h": bw.get(pid, 0),
            "latency_ms": latency
        }

        result["pools"].append(p)

    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
