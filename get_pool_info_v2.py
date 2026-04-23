#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import subprocess
import requests

PROM_URL = "http://172.16.3.107:9095/api/v1/query"


# ==============================
# 执行 shell 命令
# ==============================
def exec_cmd(cmd):
    return subprocess.check_output(cmd, shell=True).decode()


# ==============================
# Prometheus 查询
# ==============================
def prom_query(promql):
    try:
        resp = requests.get(PROM_URL, params={"query": promql}, timeout=5)
        data = resp.json()

        result = {}
        for item in data.get("data", {}).get("result", []):
            pool_id = str(item["metric"].get("pool_id"))
            value = float(item["value"][1])
            result[pool_id] = value

        return result
    except Exception as e:
        print(f"[WARN] Prometheus query failed: {promql}, err={e}")
        return {}


# ==============================
# 获取 pool 基础信息（保留原逻辑）
# ==============================
def get_pool_base_info():
    pools = json.loads(exec_cmd("ceph osd lspools -f json"))

    # device_class（原脚本一般从 crush 或 osd tree 取）
    osd_tree = json.loads(exec_cmd("ceph osd tree -f json"))

    pool_map = {}

    for p in pools:
        pool_id = str(p["poolnum"])
        pool_map[pool_id] = {
            "pool_id": p["poolnum"],
            "pool_name": p["poolname"],
            "percent_used": 0,
            "objects": 0,
            "device_class": "unknown",
            "performance": {
                "iops_12h": 0,
                "bw_bytes_12h": 0,
                "latency_ms": 0
            }
        }

    return pool_map


# ==============================
# 获取 performance（保留你原脚本思路）
# ==============================
def get_pool_perf(pool_map):
    try:
        perf = json.loads(exec_cmd("ceph osd pool stats -f json"))

        for p in perf.get("pool_stats", []):
            pool_id = str(p["pool_id"])

            if pool_id not in pool_map:
                continue

            stat = p.get("client_io_rate", {})

            pool_map[pool_id]["performance"]["iops_12h"] = (
                stat.get("read_op_per_sec", 0) +
                stat.get("write_op_per_sec", 0)
            )

            pool_map[pool_id]["performance"]["bw_bytes_12h"] = (
                stat.get("read_bytes_sec", 0) +
                stat.get("write_bytes_sec", 0)
            )

            # latency（有的版本没有）
            pool_map[pool_id]["performance"]["latency_ms"] = stat.get("op_latency", 0)

    except Exception:
        pass


# ==============================
# 主逻辑
# ==============================
def build_pool_info():

    pool_map = get_pool_base_info()

    # ===== Prometheus 指标 =====
    percent_used = prom_query("ceph_pool_percent_used")
    bytes_used = prom_query("ceph_pool_bytes_used")
    objects = prom_query("ceph_pool_objects")
    inc_24h = prom_query("increase(ceph_pool_bytes_used[24h])")
    max_avail = prom_query("ceph_pool_max_avail")
    dirty = prom_query("ceph_pool_dirty")

    for pool_id, pool in pool_map.items():

        # 1️⃣ 利用率（替换原 ceph df）
        pool["percent_used"] = round(percent_used.get(pool_id, 0) * 100, 6)

        # 2️⃣ 对象数（Prometheus 覆盖）
        pool["objects"] = int(objects.get(pool_id, 0))

        # ===== 新增字段 =====
        pool["bytes_used"] = int(bytes_used.get(pool_id, 0))
        pool["bytes_increase_24h"] = int(max(0, inc_24h.get(pool_id, 0)))
        pool["max_avail"] = int(max_avail.get(pool_id, 0))
        pool["dirty"] = int(dirty.get(pool_id, 0))

    # 保留原性能逻辑
    get_pool_perf(pool_map)

    return list(pool_map.values())


# ==============================
# 输出（保持原风格）
# ==============================
if __name__ == "__main__":
    result = build_pool_info()
    print(json.dumps(result, indent=2, ensure_ascii=False))
