#!/usr/bin/env python3
import json
import subprocess

def get_osd_df_tree():
    result = subprocess.run(
        ["ceph", "osd", "df", "tree", "-f", "json"],
        capture_output=True, text=True, check=True
    )
    return json.loads(result.stdout)

def build_node_map(nodes):
    return {node["id"]: node for node in nodes}

def get_osd_utilizations(node_id, node_map):
    """递归收集某节点下所有 OSD 的利用率"""
    node = node_map.get(node_id)
    if node is None:
        return []
    if node["type"] == "osd":
        return [{
            "osd_id":   node["id"],
            "osd_name": node["name"],
            "utilization": node["utilization"]
        }]
    osds = []
    for child_id in node.get("children", []):
        osds.extend(get_osd_utilizations(child_id, node_map))
    return osds

def analyze():
    data     = get_osd_df_tree()
    nodes    = data["nodes"]
    node_map = build_node_map(nodes)

    roots  = [n for n in nodes if n["type"] == "root"]
    result = {}

    for root in roots:
        osds = get_osd_utilizations(root["id"], node_map)
        if not osds:
            continue
        top = max(osds, key=lambda x: x["utilization"])
        result[root["name"]] = {
            "max_utilization": round(top["utilization"], 4),
            "osd_id":          top["osd_id"],
            "osd_name":        top["osd_name"]
        }

    return result

if __name__ == "__main__":
    output = analyze()
    print(json.dumps(output, indent=2, ensure_ascii=False))
