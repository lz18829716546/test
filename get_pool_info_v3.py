#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import subprocess
import requests

PROMETHEUS_URL = "http://127.0.0.1:9095/api/v1/query"

# ===============================
# PromQL 查询（24h增长）
# ===============================
PROMQL = 'ceph_pool_bytes_used - ceph_pool_bytes_used offset 24h'


def get_prometheus_data():
    try:
        resp = requests.get(PROMETHEUS_URL, params={"query": PROMQL}, timeout=10)
        data = resp.json()
        result = data.get("data", {}).get("result", [])

        pool_usage = {}
        for item in result:
            pool_id = item["metric"].get("pool_id")
            value = float(item["value"][1])
            pool_usage[pool_id] = value

        return pool_usage
    except Exception as e:
        print(f"[ERROR] Prometheus query failed: {e}")
        return {}


def get_ceph_pool_detail():
    try:
        cmd = ["ceph", "osd", "pool", "ls", "detail", "-f", "json"]
        output = subprocess.check_output(cmd)
        return json.loads(output)
    except Exception as e:
        print(f"[ERROR] Ceph command failed: {e}")
        return []


def build_pool_model(pool_list, prom_data):
    pool_models = []

    for p in pool_list:
        pool_id = str(p.get("pool_id"))
        pool_name = p.get("pool_name")

        # ===============================
        # 1️⃣ 副本/EC配置
        # ===============================
        replica_ec_conf = {
            "type": p.get("type"),  # 1=replicated, 3=ec
            "size": p.get("size"),
            "min_size": p.get("min_size"),
            "erasure_code_profile": p.get("erasure_code_profile") or None
        }

        # ===============================
        # 2️⃣ PG配置
        # ===============================
        pg_conf = {
            "pg_num": p.get("pg_num"),
            "pg_autoscale_mode": p.get("pg_autoscale_mode"),
            "pg_num_target": p.get("pg_num_target"),
            "pg_num_pending": p.get("pg_num_pending")
        }

        # ===============================
        # 3️⃣ CRUSH规则
        # ===============================
        crush_rule = p.get("crush_rule")

        # ===============================
        # 4️⃣ 应用类型
        # ===============================
        app_meta = p.get("application_metadata", {})

        # 简单提取类型标签（增强）
        if "rbd" in app_meta:
            app_type = "rbd"
        elif "cephfs" in app_meta:
            if "metadata" in app_meta.get("cephfs", {}):
                app_type = "cephfs_metadata"
            elif "data" in app_meta.get("cephfs", {}):
                app_type = "cephfs_data"
            else:
                app_type = "cephfs"
        elif "rgw" in app_meta:
            app_type = "rgw"
        else:
            app_type = "unknown"

        # ===============================
        # Prometheus数据
        # ===============================
        used_delta = prom_data.get(pool_id, 0)

        # ===============================
        # 构建最终模型
        # ===============================
        pool_model = {
            "pool_id": pool_id,
            "pool_name": pool_name,

            # 你的4个维度
            "replica_ec_conf": replica_ec_conf,
            "pg_conf": pg_conf,
            "crush_rule": crush_rule,
            "application_metadata": app_meta,
            "app_type": app_type,   # 🔥增强字段（强烈建议保留）

            # Prometheus扩展
            "used_delta_24h_bytes": used_delta,
            "used_delta_24h_gb": round(used_delta / 1024 / 1024 / 1024, 2)
        }

        pool_models.append(pool_model)

    return pool_models


def main():
    ceph_pools = get_ceph_pool_detail()
    prom_data = get_prometheus_data()

    pool_models = build_pool_model(ceph_pools, prom_data)

    # 按增长排序（方便巡检）
    pool_models = sorted(pool_models, key=lambda x: x["used_delta_24h_bytes"], reverse=True)

    print(json.dumps(pool_models, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
