#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import subprocess
import requests

PROM_URL = "http://172.16.3.107:9095"

# =========================
# 基础工具
# =========================
def run_cmd(cmd):
    try:
        out = subprocess.check_output(cmd, shell=True)
        return out.decode().strip()
    except Exception as e:
        print(f"[ERROR] {cmd} failed: {e}")
        return ""

def run_json(cmd):
    out = run_cmd(cmd)
    try:
        return json.loads(out)
    except:
        return {}

def prom_query(query):
    try:
        r = requests.get(f"{PROM_URL}/api/v1/query", params={"query": query}, timeout=10)
        return r.json().get("data", {}).get("result", [])
    except:
        return []

# =========================
# Step1: pool_name -> crush_rule_name
# =========================
def get_pool_rule_name(pool_name):
    out = run_cmd(f"ceph osd pool get {pool_name} crush_rule")
    # crush_rule: replicated_rule_ssd
    return out.split(":")[-1].strip() if out else ""

# =========================
# Step2: crush_rule_name -> root(item_name)
# =========================
def get_rule_root(rule_name):
    data = run_json(f"ceph osd crush rule dump {rule_name} -f json")
    if not data:
        return ""

    for step in data.get("steps", []):
        if step.get("op") == "take":
            return step.get("item_name", "")
    return ""

# =========================
# Step3: root -> device_class（核心）
# =========================
def get_device_class(root_name):
    if not root_name:
        return "unknown"

    data = run_json("ceph osd crush tree -f json")
    nodes = data.get("nodes", [])

    # 找 root
    root = next((n for n in nodes if n.get("type") == "root" and n.get("name") == root_name), None)
    if not root:
        return "unknown"

    classes = set()

    # root -> host -> osd
    for host_id in root.get("children", []):
        host = next((n for n in nodes if n.get("id") == host_id), None)
        if not host:
            continue

        for osd_id in host.get("children", []):
            osd = next((n for n in nodes if n.get("id") == osd_id), None)
            if osd and osd.get("device_class"):
                classes.add(osd.get("device_class"))

    if not classes:
        return "unknown"

    return ",".join(sorted(classes))

# =========================
# 性能数据
# =========================
def get_perf():
    perf = {"iops": {}, "bw": {}, "latency": 0}

    q_iops = 'avg_over_time((sum by (pool_id) (rate(ceph_pool_rd[5m]) + rate(ceph_pool_wr[5m])))[12h:])'
    for r in prom_query(q_iops):
        pid = int(r["metric"]["pool_id"])
        perf["iops"][pid] = float(r["value"][1])

    q_bw = 'avg_over_time((sum by (pool_id) (rate(ceph_pool_rd_bytes[5m]) + rate(ceph_pool_wr_bytes[5m])))[12h:])'
    for r in prom_query(q_bw):
        pid = int(r["metric"]["pool_id"])
        perf["bw"][pid] = float(r["value"][1])

    q_lat = 'sum(rate(ceph_osd_op_latency_sum[5m])) / sum(rate(ceph_osd_op_latency_count[5m]))'
    res = prom_query(q_lat)
    if res:
        perf["latency"] = float(res[0]["value"][1]) * 1000

    return perf

# =========================
# 主逻辑
# =========================
def main():
    df = run_json("ceph df detail -f json")
    perf = get_perf()

    result = {
        "cluster": run_cmd("ceph fsid"),
        "pools": []
    }

    for p in df.get("pools", []):
        pool_name = p.get("name")
        pool_id = p.get("id")
        stats = p.get("stats", {})

        # ⭐ 核心链路
        rule_name = get_pool_rule_name(pool_name)
        root_name = get_rule_root(rule_name)
        device_class = get_device_class(root_name)

        result["pools"].append({
            "pool_id": pool_id,
            "pool_name": pool_name,
            "percent_used": round(stats.get("percent_used", 0) * 100, 4),
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
