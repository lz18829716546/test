#!/usr/bin/env python3
import json
import subprocess


def run_cmd(cmd):
    result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if result.returncode != 0:
        raise Exception(f"Command failed: {cmd}\n{result.stderr.decode()}")
    return json.loads(result.stdout.decode())


def main():
    # 获取 pool 配置
    pool_detail = run_cmd("ceph osd pool ls detail --format json")

    # 获取 pool 使用情况
    df_detail = run_cmd("ceph df detail --format json")

    # 构建 df map（按 pool_name）
    df_map = {}
    for p in df_detail.get("pools", []):
        df_map[p["name"]] = p

    result = {
        "cluster": "ceph-cluster",
        "pools": []
    }

    for p in pool_detail:
        name = p.get("pool_name")

        df = df_map.get(name, {})

        pool_info = {
            "pool_id": p.get("pool_id"),
            "pool_name": name,
            "type": "replicated" if p.get("type") == 1 else "erasure",
            "size": p.get("size"),
            "min_size": p.get("min_size"),
            "pg_num": p.get("pg_num"),
            "pgp_num": p.get("pg_placement_num"),
            "crush_rule": p.get("crush_rule"),
            "autoscale_mode": p.get("pg_autoscale_mode"),
            "application": list(p.get("application_metadata", {}).keys()),

            "stored_bytes": df.get("stats", {}).get("stored"),
            "objects": df.get("stats", {}).get("objects"),
            "used_bytes": df.get("stats", {}).get("bytes_used"),
            "percent_used": df.get("stats", {}).get("percent_used"),
            "max_avail_bytes": df.get("stats", {}).get("max_avail"),
        }

        result["pools"].append(pool_info)

    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
