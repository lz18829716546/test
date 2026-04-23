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
    except Exception as e:
        print(f"[WARN] Prometheus query failed: {query} -> {e}")
        return []
 
def bytes_to_human(b):
    """将字节转换为人类可读格式"""
    try:
        b = float(b)
    except:
        return "0 B"
    for unit in ["B", "KB", "MB", "GB", "TB", "PB"]:
        if abs(b) < 1024.0:
            return f"{b:.2f} {unit}"
        b /= 1024.0
    return f"{b:.2f} EB"
 
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
# Prometheus 存储池指标批量采集
# 需求1~6：通过 Prometheus API 获取各项存储池指标
# =========================
 
def get_prom_pool_metrics():
    """
    从 Prometheus 批量获取以下指标（均以 pool_id 为 key）：
      - percent_used     : 需求1 - ceph_pool_percent_used（0~1 小数，显示时 *100 转百分比）
      - bytes_used       : 需求2 - ceph_pool_bytes_used（字节）
      - objects          : 需求3 - ceph_pool_objects（个数）
      - growth_24h_bytes : 需求4 - ceph_pool_bytes_used - ceph_pool_bytes_used offset 24h
                           用当前值减去24h前的值，结果更精确；正值=增长，负值=释放
      - max_avail_bytes  : 需求5 - ceph_pool_max_avail（字节）
      - dirty            : 需求6 - ceph_pool_dirty（个数）
    """
    metrics = {}
 
    def collect(query, field, converter=float):
        for r in prom_query(query):
            pid = int(r["metric"]["pool_id"])
            if pid not in metrics:
                metrics[pid] = {}
            try:
                metrics[pid][field] = converter(r["value"][1])
            except:
                metrics[pid][field] = 0
 
    # 需求1: 存储池利用率（Prometheus 返回 0~1 的小数）
    collect("ceph_pool_percent_used", "percent_used")
 
    # 需求2: 存储池实际使用量（字节）
    collect("ceph_pool_bytes_used", "bytes_used")
 
    # 需求3: 存储池对象数
    collect("ceph_pool_objects", "objects", lambda v: int(float(v)))
 
    # 需求4: 存储池 24 小时空间增长量（字节）
    # 用 offset 差值法：当前值 - 24h 前的值，比 increase() 更精确
    # 返回值可为负数（表示该池在过去 24h 内净释放了空间）
    collect("ceph_pool_bytes_used - ceph_pool_bytes_used offset 24h", "growth_24h_bytes")
 
    # 需求5: 存储池最大可用空间（字节）
    collect("ceph_pool_max_avail", "max_avail_bytes")
 
    # 需求6: 存储池 dirty 数据
    collect("ceph_pool_dirty", "dirty", lambda v: int(float(v)))
 
    return metrics
 
# =========================
# 主逻辑
# =========================
 
def main():
    # 使用 ceph df detail 获取 pool 列表（pool_id / pool_name），
    # 存储池各统计指标均改由 Prometheus API 提供
    df = run_json("ceph df detail -f json")
    perf = get_perf()
 
    # 批量从 Prometheus 获取所有存储池指标（需求 1~6）
    prom_metrics = get_prom_pool_metrics()
 
    result = {
        "cluster": run_cmd("ceph fsid"),
        "pools": []
    }
 
    for p in df.get("pools", []):
        pool_name = p.get("name")
        pool_id   = p.get("id")
 
        # ⭐ 核心链路：device_class 解析保持不变
        rule_name    = get_pool_rule_name(pool_name)
        root_name    = get_rule_root(rule_name)
        device_class = get_device_class(root_name)
 
        # 从 Prometheus 获取该 pool 的指标，若未采集到则置默认值
        pm = prom_metrics.get(pool_id, {})
 
        # 需求1: percent_used —— Prometheus 返回 0~1 小数，*100 转为百分比保留4位小数
        percent_used = round(pm.get("percent_used", 0) * 100, 4)
 
        # 需求2: 实际使用量（字节 + 人类可读）
        bytes_used_raw = pm.get("bytes_used", 0)
 
        # 需求3: 对象数（来自 Prometheus）
        objects = pm.get("objects", 0)
 
        # 需求4: 24h 空间净变化量（字节 + 人类可读），正值=增长，负值=净释放
        growth_24h_raw = pm.get("growth_24h_bytes", 0)
 
        # 需求5: 最大可用空间（字节 + 人类可读）
        max_avail_raw = pm.get("max_avail_bytes", 0)
 
        # 需求6: dirty 数据量（个数）
        dirty = pm.get("dirty", 0)
 
        result["pools"].append({
            "pool_id":      pool_id,
            "pool_name":    pool_name,
            "percent_used": percent_used,           # 单位: %
            "bytes_used":   bytes_to_human(bytes_used_raw),   # 需求2
            "bytes_used_raw": int(bytes_used_raw),            # 需求2（原始字节）
            "objects":      objects,                          # 需求3
            "max_avail":    bytes_to_human(max_avail_raw),    # 需求5
            "max_avail_raw": int(max_avail_raw),              # 需求5（原始字节）
            "growth_24h":   bytes_to_human(growth_24h_raw),   # 需求4
            "growth_24h_raw": int(growth_24h_raw),            # 需求4（原始字节）
            "dirty":        dirty,                            # 需求6
            "device_class": device_class,
            "performance": {
                "iops_12h":    perf["iops"].get(pool_id, 0),
                "bw_bytes_12h": perf["bw"].get(pool_id, 0),
                "latency_ms":  perf["latency"]
            }
        })
 
    print(json.dumps(result, indent=2, ensure_ascii=False))
 
if __name__ == "__main__":
    main()
 
