#!/usr/bin/env python3
import json
import subprocess
import requests

PROM_URL = "http://172.16.3.107:9095"

# ---------------------------
# 执行命令
# ---------------------------
def run_cmd(cmd):
    return subprocess.check_output(cmd, shell=True).decode()

# ---------------------------
# 获取 pool 基础信息
# ---------------------------
def get_pools():
    out = run_cmd("ceph df detail -f json")
    data = json.loads(out)

    pools = {}
    for p in data["pools"]:
        pools[p["id"]] = {
            "pool_id": p["id"],
            "pool_name": p["name"],
            "bytes_used": p["stats"]["bytes_used"],
            "objects": p["stats"]["objects"],
        }
    return pools

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
# 获取性能数据
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

    # latency 是全局
    latency_resp = requests.get(
        f"{PROM_URL}/api/v1/query",
        params={"query": latency_q}
    ).json()

    latency = None
    if latency_resp["data"]["result"]:
        latency = float(latency_resp["data"]["result"][0]["value"][1]) * 1000  # 转 ms

    return iops, bw, latency

# ---------------------------
# 主逻辑
# ---------------------------
def main():
    pools = get_pools()
    iops, bw, latency = get_perf()

    result = {
        "cluster": "test-cluster",
        "pools": []
    }

    for pid, p in pools.items():
        p["performance"] = {
            "iops_12h": iops.get(pid, 0),
            "bw_bytes_12h": bw.get(pid, 0),
            "latency_ms": latency
        }

        result["pools"].append(p)

    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
