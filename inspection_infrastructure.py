#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Ceph 基础架构巡检脚本 - 2.1 基础架构的巡检
覆盖 2.1.1 ~ 2.1.13 共 13 个检查项
"""
 
import json
import subprocess
import urllib.request
import urllib.parse
from datetime import datetime
 
# ──────────────────────────────────────────────
# 配置
# ──────────────────────────────────────────────
PROMETHEUS_URL = "http://localhost:9095"
 
 
# ──────────────────────────────────────────────
# 工具函数
# ──────────────────────────────────────────────
 
def make_result(check_id: str, check_name: str, status: str,
                value=None, detail=None) -> dict:
    """
    统一数据模型：
    {
        "check_id":   "2.1.1",
        "check_name": "告警状态",
        "status":     "pass" | "fail" | "error",
        "value":      <原始返回值，可为任意类型>,
        "detail":     <不通过或出错时的详细说明，pass 时为 None>
    }
    """
    return {
        "check_id":   check_id,
        "check_name": check_name,
        "status":     status,
        "value":      value,
        "detail":     detail,
    }
 
 
def run_cmd(args: list) -> str:
    """执行 shell 命令，返回 stdout 字符串；失败抛出异常。"""
    result = subprocess.run(args, capture_output=True, text=True, check=True)
    return result.stdout.strip()
 
 
def prom_query(query: str) -> list:
    """
    向 Prometheus 发起 instant query，返回 result 列表。
    result 元素格式：{"metric": {...}, "value": [timestamp, "value_str"]}
    """
    encoded = urllib.parse.urlencode({"query": query})
    url = f"{PROMETHEUS_URL}/api/v1/query?{encoded}"
    with urllib.request.urlopen(url, timeout=15) as resp:
        data = json.loads(resp.read().decode())
    if data.get("status") != "success":
        raise RuntimeError(f"Prometheus 查询失败: {data}")
    return data["data"]["result"]
 
 
# ──────────────────────────────────────────────
# 2.1.1  告警状态
# ──────────────────────────────────────────────
 
def check_2_1_1() -> dict:
    check_id, check_name = "2.1.1", "告警状态"
    try:
        output = run_cmd(["ceph", "health", "detail"])
        first_line = output.splitlines()[0].strip() if output else ""
        if first_line.startswith("HEALTH_OK"):
            return make_result(check_id, check_name, "pass", value=first_line)
        else:
            return make_result(check_id, check_name, "fail",
                               value=first_line, detail=output)
    except Exception as e:
        return make_result(check_id, check_name, "error", detail=str(e))
 
 
# ──────────────────────────────────────────────
# 2.1.2  集群慢 IO 状态
# ──────────────────────────────────────────────
 
def check_2_1_2() -> dict:
    check_id, check_name = "2.1.2", "集群慢IO状态"
    try:
        results = prom_query("max_over_time(ceph_healthcheck_slow_ops[24h])")
        if not results:
            # 无数据视为 0（集群从未上报慢 IO）
            return make_result(check_id, check_name, "pass", value=0)
        val = float(results[0]["value"][1])
        if val == 0:
            return make_result(check_id, check_name, "pass", value=val)
        else:
            return make_result(check_id, check_name, "fail", value=val,
                               detail=f"过去24小时最大慢IO数量: {val}")
    except Exception as e:
        return make_result(check_id, check_name, "error", detail=str(e))
 
 
# ──────────────────────────────────────────────
# 2.1.3  系统盘使用容量
# ──────────────────────────────────────────────
 
def check_2_1_3() -> dict:
    check_id, check_name = "2.1.3", "系统盘使用容量"
    query = ('100 - (node_filesystem_avail_bytes{mountpoint="/",fstype!~"tmpfs|overlay"}'
             ' / node_filesystem_size_bytes{mountpoint="/",fstype!~"tmpfs|overlay"} * 100)')
    try:
        results = prom_query(query)
        node_values = {}
        failed_nodes = {}
        for r in results:
            instance = r["metric"].get("instance", "unknown")
            val = float(r["value"][1])
            node_values[instance] = round(val, 2)
            if val >= 70:
                failed_nodes[instance] = round(val, 2)
        if not failed_nodes:
            return make_result(check_id, check_name, "pass", value=node_values)
        else:
            return make_result(check_id, check_name, "fail", value=node_values,
                               detail={"超阈值节点(≥70%)": failed_nodes})
    except Exception as e:
        return make_result(check_id, check_name, "error", detail=str(e))
 
 
# ──────────────────────────────────────────────
# 2.1.4  节点 inode 使用率
# ──────────────────────────────────────────────
 
def check_2_1_4() -> dict:
    check_id, check_name = "2.1.4", "节点inode使用率"
    query = ('100 - (node_filesystem_files_free{mountpoint=~"/|/var|/opt|/tmp",'
             'fstype!~"tmpfs|overlay"} / node_filesystem_files{mountpoint=~"/|/var|/opt|/tmp",'
             'fstype!~"tmpfs|overlay"} * 100)')
    try:
        results = prom_query(query)
        node_values = {}
        failed_nodes = {}
        for r in results:
            instance  = r["metric"].get("instance", "unknown")
            mountpoint = r["metric"].get("mountpoint", "?")
            key = f"{instance}:{mountpoint}"
            val = float(r["value"][1])
            node_values[key] = round(val, 4)
            if val >= 70:
                failed_nodes[key] = round(val, 4)
        if not failed_nodes:
            return make_result(check_id, check_name, "pass", value=node_values)
        else:
            return make_result(check_id, check_name, "fail", value=node_values,
                               detail={"超阈值节点(≥70%)": failed_nodes})
    except Exception as e:
        return make_result(check_id, check_name, "error", detail=str(e))
 
 
# ──────────────────────────────────────────────
# 2.1.5  节点文件系统状态检查
# ──────────────────────────────────────────────
 
def check_2_1_5() -> dict:
    check_id, check_name = "2.1.5", "节点文件系统状态检查"
    query = 'node_filesystem_device_error{fstype!~"tmpfs|overlay"} == 1'
    try:
        results = prom_query(query)
        if not results:
            return make_result(check_id, check_name, "pass", value=[])
        else:
            errors = [
                {"instance": r["metric"].get("instance"),
                 "device":   r["metric"].get("device"),
                 "mountpoint": r["metric"].get("mountpoint")}
                for r in results
            ]
            return make_result(check_id, check_name, "fail", value=errors,
                               detail={"文件系统故障设备": errors})
    except Exception as e:
        return make_result(check_id, check_name, "error", detail=str(e))
 
 
# ──────────────────────────────────────────────
# 2.1.6  节点 CPU 负载
# ──────────────────────────────────────────────
 
def check_2_1_6() -> dict:
    check_id, check_name = "2.1.6", "节点CPU负载"
    query = ('100 - (min_over_time(avg by(instance)'
             '(irate(node_cpu_seconds_total{mode="idle"}[5m]))[24h:]) * 100)')
    try:
        results = prom_query(query)
        node_values = {}
        failed_nodes = {}
        for r in results:
            instance = r["metric"].get("instance", "unknown")
            val = float(r["value"][1])
            node_values[instance] = round(val, 4)
            if val >= 90:
                failed_nodes[instance] = round(val, 4)
        if not failed_nodes:
            return make_result(check_id, check_name, "pass", value=node_values)
        else:
            return make_result(check_id, check_name, "fail", value=node_values,
                               detail={"超阈值节点(≥90%)": failed_nodes})
    except Exception as e:
        return make_result(check_id, check_name, "error", detail=str(e))
 
 
# ──────────────────────────────────────────────
# 2.1.7  内存使用率
# ──────────────────────────────────────────────
 
def check_2_1_7() -> dict:
    check_id, check_name = "2.1.7", "内存使用率"
    query = '100 - (node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes * 100)'
    try:
        results = prom_query(query)
        node_values = {}
        failed_nodes = {}
        for r in results:
            instance = r["metric"].get("instance", "unknown")
            val = float(r["value"][1])
            node_values[instance] = round(val, 4)
            if val >= 70:
                failed_nodes[instance] = round(val, 4)
        if not failed_nodes:
            return make_result(check_id, check_name, "pass", value=node_values)
        else:
            return make_result(check_id, check_name, "fail", value=node_values,
                               detail={"超阈值节点(≥70%)": failed_nodes})
    except Exception as e:
        return make_result(check_id, check_name, "error", detail=str(e))
 
 
# ──────────────────────────────────────────────
# 2.1.8  节点网卡丢包
# ──────────────────────────────────────────────
 
def check_2_1_8() -> dict:
    check_id, check_name = "2.1.8", "节点网卡丢包"
    query = 'rate(node_network_receive_drop_total{device=~"^(ens|bond).*"}[1m])'
    try:
        results = prom_query(query)
        iface_values = {}
        failed_ifaces = {}
        for r in results:
            instance = r["metric"].get("instance", "unknown")
            device   = r["metric"].get("device", "unknown")
            key      = f"{instance}/{device}"
            val      = float(r["value"][1])
            iface_values[key] = round(val, 4)
            if val >= 1:
                failed_ifaces[key] = round(val, 4)
        if not failed_ifaces:
            return make_result(check_id, check_name, "pass", value=iface_values)
        else:
            return make_result(check_id, check_name, "fail", value=iface_values,
                               detail={"超阈值网卡(≥1 pkt/s)": failed_ifaces})
    except Exception as e:
        return make_result(check_id, check_name, "error", detail=str(e))
 
 
# ──────────────────────────────────────────────
# 2.1.9  节点网卡错误包
# ──────────────────────────────────────────────
 
def check_2_1_9() -> dict:
    check_id, check_name = "2.1.9", "节点网卡错误包"
    query = 'rate(node_network_receive_errs_total{device!~"lo"}[1m])'
    try:
        results = prom_query(query)
        iface_values = {}
        failed_ifaces = {}
        for r in results:
            instance = r["metric"].get("instance", "unknown")
            device   = r["metric"].get("device", "unknown")
            key      = f"{instance}/{device}"
            val      = float(r["value"][1])
            iface_values[key] = round(val, 4)
            if val >= 1:
                failed_ifaces[key] = round(val, 4)
        if not failed_ifaces:
            return make_result(check_id, check_name, "pass", value=iface_values)
        else:
            return make_result(check_id, check_name, "fail", value=iface_values,
                               detail={"超阈值网卡(≥1 err/s)": failed_ifaces})
    except Exception as e:
        return make_result(check_id, check_name, "error", detail=str(e))
 
 
# ──────────────────────────────────────────────
# 2.1.10 节点网卡状态
# ──────────────────────────────────────────────
 
def check_2_1_10() -> dict:
    check_id, check_name = "2.1.10", "节点网卡状态"
    query = 'node_network_up{device=~"^(ens|bond).*"}'
    try:
        results = prom_query(query)
        iface_values = {}
        failed_ifaces = {}
        for r in results:
            instance = r["metric"].get("instance", "unknown")
            device   = r["metric"].get("device", "unknown")
            key      = f"{instance}/{device}"
            val      = int(float(r["value"][1]))
            iface_values[key] = val
            if val != 1:
                failed_ifaces[key] = val
        if not failed_ifaces:
            return make_result(check_id, check_name, "pass", value=iface_values)
        else:
            return make_result(check_id, check_name, "fail", value=iface_values,
                               detail={"状态异常网卡(≠1)": failed_ifaces})
    except Exception as e:
        return make_result(check_id, check_name, "error", detail=str(e))
 
 
# ──────────────────────────────────────────────
# 2.1.11 CRUSH_RULE 物理设备使用率
# ──────────────────────────────────────────────
 
def _build_node_map(nodes):
    return {node["id"]: node for node in nodes}
 
 
def _get_osd_utilizations(node_id, node_map):
    node = node_map.get(node_id)
    if node is None:
        return []
    if node["type"] == "osd":
        return [{"osd_id":       node["id"],
                 "osd_name":     node["name"],
                 "utilization":  node["utilization"]}]
    osds = []
    for child_id in node.get("children", []):
        osds.extend(_get_osd_utilizations(child_id, node_map))
    return osds
 
 
def check_2_1_11() -> dict:
    check_id, check_name = "2.1.11", "CRUSH_RULE物理设备使用率"
    try:
        raw      = run_cmd(["ceph", "osd", "df", "tree", "-f", "json"])
        data     = json.loads(raw)
        nodes    = data["nodes"]
        node_map = _build_node_map(nodes)
        roots    = [n for n in nodes if n["type"] == "root"]
 
        crush_rule_stats = {}
        failed_rules     = {}
 
        for root in roots:
            osds = _get_osd_utilizations(root["id"], node_map)
            if not osds:
                continue
            top = max(osds, key=lambda x: x["utilization"])
            crush_rule_stats[root["name"]] = {
                "max_utilization": round(top["utilization"], 4),
                "osd_id":         top["osd_id"],
                "osd_name":       top["osd_name"],
            }
            if top["utilization"] >= 70:
                failed_rules[root["name"]] = crush_rule_stats[root["name"]]
 
        if not failed_rules:
            return make_result(check_id, check_name, "pass",
                               value=crush_rule_stats)
        else:
            return make_result(check_id, check_name, "fail",
                               value=crush_rule_stats,
                               detail={"超阈值CRUSH_RULE(≥70%)": failed_rules})
    except Exception as e:
        return make_result(check_id, check_name, "error", detail=str(e))
 
 
# ──────────────────────────────────────────────
# 2.1.12 节点硬件温度
# ──────────────────────────────────────────────
 
def check_2_1_12() -> dict:
    check_id, check_name = "2.1.12", "节点硬件温度"
    query = "max by(instance)(node_thermal_zone_temp)"
    try:
        results = prom_query(query)
        node_values = {}
        failed_nodes = {}
        for r in results:
            instance = r["metric"].get("instance", "unknown")
            val      = float(r["value"][1])
            node_values[instance] = round(val, 1)
            if val > 85:
                failed_nodes[instance] = round(val, 1)
        if not failed_nodes:
            return make_result(check_id, check_name, "pass", value=node_values)
        else:
            return make_result(check_id, check_name, "fail", value=node_values,
                               detail={"超温节点(>85℃)": failed_nodes})
    except Exception as e:
        return make_result(check_id, check_name, "error", detail=str(e))
 
 
# ──────────────────────────────────────────────
# 2.1.13 获取节点信息
# ──────────────────────────────────────────────
 
def check_2_1_13() -> dict:
    check_id, check_name = "2.1.13", "获取节点信息"
    query = "node_os_info or node_uname_info"
    try:
        results = prom_query(query)
        # 按 instance 聚合 os 和 uname 信息
        node_info: dict = {}
        for r in results:
            m        = r["metric"]
            instance = m.get("instance", "unknown")
            metric_name = m.get("__name__", "")
            if instance not in node_info:
                node_info[instance] = {}
            if metric_name == "node_os_info":
                node_info[instance].update({
                    "os_name":       m.get("name", ""),
                    "os_version":    m.get("pretty_name", ""),
                    "os_version_id": m.get("version_id", ""),
                })
            elif metric_name == "node_uname_info":
                node_info[instance].update({
                    "hostname":       m.get("nodename", ""),
                    "kernel_release": m.get("release", ""),
                    "kernel_version": m.get("version", ""),
                    "machine":        m.get("machine", ""),
                })
        # 该项仅采集，不做通过/失败判断
        return make_result(check_id, check_name, "pass", value=node_info)
    except Exception as e:
        return make_result(check_id, check_name, "error", detail=str(e))
 
 
# ──────────────────────────────────────────────
# 主入口
# ──────────────────────────────────────────────
 
def run_all_checks() -> dict:
    checkers = [
        check_2_1_1,
        check_2_1_2,
        check_2_1_3,
        check_2_1_4,
        check_2_1_5,
        check_2_1_6,
        check_2_1_7,
        check_2_1_8,
        check_2_1_9,
        check_2_1_10,
        check_2_1_11,
        check_2_1_12,
        check_2_1_13,
    ]
 
    results = []
    for fn in checkers:
        results.append(fn())
 
    total  = len(results)
    passed = sum(1 for r in results if r["status"] == "pass")
    failed = sum(1 for r in results if r["status"] == "fail")
    errors = sum(1 for r in results if r["status"] == "error")
 
    return {
        "inspect_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "summary": {
            "total":  total,
            "pass":   passed,
            "fail":   failed,
            "error":  errors,
        },
        "checks": results,
    }
 
 
if __name__ == "__main__":
    output = run_all_checks()
    print(json.dumps(output, indent=2, ensure_ascii=False))
