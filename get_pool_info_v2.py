#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import subprocess
import requests
from typing import Dict

PROMETHEUS_URL = "http://172.16.3.107:9095/api/v1/query"


# =============================
# Prometheus 查询封装
# =============================
def query_prometheus(promql: str) -> Dict[str, float]:
    """
    返回: {pool_id: value}
    """
    try:
        resp = requests.get(PROMETHEUS_URL, params={"query": promql}, timeout=5)
        data = resp.json()

        result = {}
        for item in data.get("data", {}).get("result", []):
            pool_id = item["metric"].get("pool_id")
            value = float(item["value"][1])
            result[pool_id] = value

        return result

    except Exception as e:
        print(f"[WARN] Prometheus query failed: {promql}, err={e}")
        return {}


# =============================
# 获取 Ceph Pool 基础信息
# =============================
def get_pools():
    cmd = "ceph osd lspools -f json"
    output = subprocess.check_output(cmd, shell=True)
    pools = json.loads(output)

    pool_map = {}
    for p in pools:
        pool_map[str(p["poolnum"])] = {
            "pool_id": p["poolnum"],
            "pool_name": p["poolname"],
        }

    return pool_map


# =============================
# 主逻辑
# =============================
def build_pool_info():
    pool_map = get_pools()

    # ===== Prometheus 指标 =====
    percent_used = query_prometheus("ceph_pool_percent_used")
    bytes_used = query_prometheus("ceph_pool_bytes_used")
    objects = query_prometheus("ceph_pool_objects")
    increase_24h = query_prometheus("increase(ceph_pool_bytes_used[24h])")
    max_avail = query_prometheus("ceph_pool_max_avail")
    dirty = query_prometheus("ceph_pool_dirty")

    # ===== 合并数据 =====
    for pool_id, pool in pool_map.items():

        # 利用率（Prometheus返回的是比例，需要转百分比）
        pool["percent_used"] = round(percent_used.get(pool_id, 0) * 100, 6)

        # 使用量（字节）
        pool["bytes_used"] = int(bytes_used.get(pool_id, 0))

        # 对象数
        pool["objects"] = int(objects.get(pool_id, 0))

        # 24小时增长（字节）
        pool["bytes_increase_24h"] = int(increase_24h.get(pool_id, 0))

        # 最大可用空间（字节）
        pool["max_avail"] = int(max_avail.get(pool_id, 0))

        # dirty
        pool["dirty"] = int(dirty.get(pool_id, 0))

    return list(pool_map.values())


# =============================
# 输出
# =============================
if __name__ == "__main__":
    pool_info = build_pool_info()
    print(json.dumps(pool_info, indent=2, ensure_ascii=False))
