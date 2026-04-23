#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import subprocess
import requests

# =========================
# Prometheus 配置（按你环境改）
# =========================
PROM_URL = "http://172.16.3.107:9095"

# =========================
# 工具函数
# =========================
def run_cmd(cmd):
    try:
        out = subprocess.check_output(cmd, shell=True)
        return json.loads(out)
    except Exception as e:
        print(f"[ERROR] cmd failed: {cmd}, {e}")
        return {}

def prom_query(query):
    try:
        r = requests.get(f"{PROM_URL}/api/v1/query", params={"query": query}, timeout=10)
        data = r.json()
        return data.get("data", {}).get("result", [])
    except Exception as e:
        print(f"[ERROR] prom query failed: {query}, {e}")
        return []

# =========================
# 获取 pool -> crush_rule 映射（兼容不同版本）
# =========================
def get_pool_rule_map():
    data = run_cmd("ceph osd pool ls detail -f json")
    m = {}
    for p in data:
        pool_id = p.get("pool") or p.get("pool_id")
        rule_id = p.get("crush_rule")
        if pool_id is not None and rule_id is not None:
            m[pool_id] = rule_id
    return m

# =========================
# 获取 crush rule -> device_class
# =========================
def get_rule_device_class_map():
    data = run_cmd("ceph osd crush rule dump -f json")
    m = {}

    for rule in data:
        rule_id = rule["rule_id"]
        device_class = "unknown"

        for step in rule.get("steps", []):
            if "item_name" in step:
                name = step["item_name"]
                if "~" in name:
                    device_class = name.split("~")[-1]
                else:
                    device_class = name

        m[rule_id] = device_class

    return m

# =========================
# 获取性能数据
# =========================
def get_perf():
    perf = {
        "iops": {},
        "bw": {},
        "latency": 0
    }

    # IOPS（12h平均）
    q_iops = 'avg_over_time((sum by (pool_id) (rate(ceph_pool_rd[5m]) + rate(ceph_pool_wr[5m])))[12h:])'
    for r in prom_query(q_iops):
        pid = int(r["metric"]["pool_id"])
        val = float(r["value"][1])
        perf["iops"][pid] = val

    # 带宽（12h平均）
    q_bw = 'avg_over_time((sum by (pool_id) (rate(ceph_pool_rd_bytes[5m]) + rate(ceph_pool_wr_bytes[5m])))[12h:])'
    for r in prom_query(q_bw):
        pid = int(r["metric"]["pool_id"])
        val = float(r["value"][1])
        perf["bw"][pid] = val

    # 延迟（全局）
    q_lat = 'sum(rate(ceph_osd_op_latency_sum[5m])) / sum(rate(ceph_osd_op_latency_count[5m]))'
    res = prom_query(q_lat)
    if res:
        perf["latency"] = float(res[0]["value"][1]) * 1000  # 转 ms

    return perf

# =========================
# 主函数
# =========================
def main():
    df = run_cmd("ceph df detail -f json")

    pool_rule = get_pool_rule_map()
    rule_class = get_rule_device_class_map()
    perf = get_perf()

    result = {
        "cluster": run_cmd("ceph fsid"),
        "pools": []
    }

    for p in df.get("pools", []):
        pool_id = p.get("id")
        stats = p.get("stats", {})

        rule_id = pool_rule.get(pool_id)
        device_class = rule_class.get(rule_id, "unknown")

        percent_used = round(stats.get("percent_used", 0) * 100, 4)

        result["pools"].append({
            "pool_id": pool_id,
            "pool_name": p.get("name"),
            "percent_used": percent_used,
            "objects": stats.get("objects", 0),
            "device_class": device_class,
            "performance": {
                "iops_12h": perf["iops"].get(pool_id, 0),
                "bw_bytes_12h": perf["bw"].get(pool_id, 0),
                "latency_ms": perf["latency"]
            }
        })

    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
