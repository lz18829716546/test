root@gm1-pub-ceph-172-16-3-107:/tmp# python3 get_pool_info_v3.py 
{
  "cluster": "4d5ef994-b2a4-11ef-9745-11886f06c997",
  "pools": [
    {
      "pool_id": 1,
      "pool_name": "device_health_metrics",
      "percent_used": 0.0012,
      "bytes_used": "303.07 MB",
      "bytes_used_raw": 317792685,
      "objects": 28,
      "max_avail": "7.80 TB",
      "max_avail_raw": 8577691942912,
      "growth_24h": "1.62 MB",
      "growth_24h_raw": 1701930,
      "dirty": 0,
      "device_class": "hdd",
      "replica_ec_conf": {
        "type": "replicated",
        "size": 3,
        "min_size": 2,
        "erasure_code_profile": ""
      },
      "pg_conf": {
        "pg_num": 1,
        "pg_autoscale_mode": "on",
        "pg_num_target": 1,
        "pg_num_pending": 1
      },
      "crush_rule": "replicated_rule",
      "application_metadata": {
        "mgr_devicehealth": {}
      },
      "performance": {
        "iops_12h": 0.0019444444444444559,
        "bw_bytes_12h": 4483.579259259211,
        "latency_ms": 21.689253921051538
      }
    },
    {
      "pool_id": 2,
      "pool_name": "cloud",
      "percent_used": 0.9923,
      "bytes_used": "240.19 GB",
      "bytes_used_raw": 257898956663,
      "objects": 35263,
      "max_avail": "7.80 TB",
      "max_avail_raw": 8577691942912,
      "growth_24h": "-728.62 MB",
      "growth_24h_raw": -764009062,
      "dirty": 0,
      "device_class": "hdd",
      "replica_ec_conf": {
        "type": "replicated",
        "size": 3,
        "min_size": 2,
        "erasure_code_profile": ""
      },
      "pg_conf": {
        "pg_num": 128,
        "pg_autoscale_mode": "off",
        "pg_num_target": 128,
        "pg_num_pending": 128
      },
      "crush_rule": "replicated_rule",
      "application_metadata": {
        "rbd": {}
      },
      "performance": {
        "iops_12h": 128.11546934865896,
        "bw_bytes_12h": 7379165.144112387,
        "latency_ms": 21.689253921051538
      }
    },
    {
      "pool_id": 3,
      "pool_name": "cephfs-metadata",
      "percent_used": 0.0307,
      "bytes_used": "797.19 MB",
      "bytes_used_raw": 835915375,
      "objects": 698,
      "max_avail": "845.53 GB",
      "max_avail_raw": 907878072320,
      "growth_24h": "-81.46 MB",
      "growth_24h_raw": -85416125,
      "dirty": 0,
      "device_class": "ssd",
      "replica_ec_conf": {
        "type": "replicated",
        "size": 3,
        "min_size": 2,
        "erasure_code_profile": ""
      },
      "pg_conf": {
        "pg_num": 64,
        "pg_autoscale_mode": "off",
        "pg_num_target": 64,
        "pg_num_pending": 64
      },
      "crush_rule": "replicated_rule_ssd",
      "application_metadata": {
        "cephfs": {
          "metadata": "cephfs"
        }
      },
      "performance": {
        "iops_12h": 1.6204054916985953,
        "bw_bytes_12h": 5645.824980842904,
        "latency_ms": 21.689253921051538
      }
    },
    {
      "pool_id": 4,
      "pool_name": "cephfs-data",
      "percent_used": 0.1185,
      "bytes_used": "28.42 GB",
      "bytes_used_raw": 30519787520,
      "objects": 6654,
      "max_avail": "7.80 TB",
      "max_avail_raw": 8577691942912,
      "growth_24h": "130.93 MB",
      "growth_24h_raw": 137289728,
      "dirty": 0,
      "device_class": "hdd",
      "replica_ec_conf": {
        "type": "replicated",
        "size": 3,
        "min_size": 2,
        "erasure_code_profile": ""
      },
      "pg_conf": {
        "pg_num": 128,
        "pg_autoscale_mode": "off",
        "pg_num_target": 128,
        "pg_num_pending": 128
      },
      "crush_rule": "replicated_rule",
      "application_metadata": {
        "cephfs": {
          "data": "cephfs"
        }
      },
      "performance": {
        "iops_12h": 0.5368582375478929,
        "bw_bytes_12h": 3036.526181353769,
        "latency_ms": 21.689253921051538
      }
    },
    {
      "pool_id": 5,
      "pool_name": ".rgw.root",
      "percent_used": 0.0,
      "bytes_used": "72.00 KB",
      "bytes_used_raw": 73728,
      "objects": 6,
      "max_avail": "7.80 TB",
      "max_avail_raw": 8577691942912,
      "growth_24h": "0.00 B",
      "growth_24h_raw": 0,
      "dirty": 0,
      "device_class": "hdd",
      "replica_ec_conf": {
        "type": "replicated",
        "size": 3,
        "min_size": 2,
        "erasure_code_profile": ""
      },
      "pg_conf": {
        "pg_num": 32,
        "pg_autoscale_mode": "on",
        "pg_num_target": 32,
        "pg_num_pending": 32
      },
      "crush_rule": "replicated_rule",
      "application_metadata": {
        "rgw": {}
      },
      "performance": {
        "iops_12h": 0.0,
        "bw_bytes_12h": 0.0,
        "latency_ms": 21.689253921051538
      }
    },
    {
      "pool_id": 6,
      "pool_name": "default.rgw.log",
      "percent_used": 0.0,
      "bytes_used": "7.75 MB",
      "bytes_used_raw": 8122368,
      "objects": 209,
      "max_avail": "7.80 TB",
      "max_avail_raw": 8577691942912,
      "growth_24h": "-2.95 MB",
      "growth_24h_raw": -3096576,
      "dirty": 0,
      "device_class": "hdd",
      "replica_ec_conf": {
        "type": "replicated",
        "size": 3,
        "min_size": 2,
        "erasure_code_profile": ""
      },
      "pg_conf": {
        "pg_num": 32,
        "pg_autoscale_mode": "on",
        "pg_num_target": 32,
        "pg_num_pending": 32
      },
      "crush_rule": "replicated_rule",
      "application_metadata": {
        "rgw": {}
      },
      "performance": {
        "iops_12h": 4.04062340357599,
        "bw_bytes_12h": 5402.6584929757355,
        "latency_ms": 21.689253921051538
      }
    },
    {
      "pool_id": 7,
      "pool_name": "default.rgw.control",
      "percent_used": 0.0,
      "bytes_used": "0.00 B",
      "bytes_used_raw": 0,
      "objects": 8,
      "max_avail": "7.80 TB",
      "max_avail_raw": 8577691942912,
      "growth_24h": "0.00 B",
      "growth_24h_raw": 0,
      "dirty": 0,
      "device_class": "hdd",
      "replica_ec_conf": {
        "type": "replicated",
        "size": 3,
        "min_size": 2,
        "erasure_code_profile": ""
      },
      "pg_conf": {
        "pg_num": 32,
        "pg_autoscale_mode": "on",
        "pg_num_target": 32,
        "pg_num_pending": 32
      },
      "crush_rule": "replicated_rule",
      "application_metadata": {
        "rgw": {}
      },
      "performance": {
        "iops_12h": 0.0,
        "bw_bytes_12h": 0.0,
        "latency_ms": 21.689253921051538
      }
    },
    {
      "pool_id": 8,
      "pool_name": "default.rgw.meta",
      "percent_used": 0.0,
      "bytes_used": "3.67 MB",
      "bytes_used_raw": 3850915,
      "objects": 372,
      "max_avail": "7.80 TB",
      "max_avail_raw": 8577691942912,
      "growth_24h": "3.98 KB",
      "growth_24h_raw": 4077,
      "dirty": 0,
      "device_class": "hdd",
      "replica_ec_conf": {
        "type": "replicated",
        "size": 3,
        "min_size": 2,
        "erasure_code_profile": ""
      },
      "pg_conf": {
        "pg_num": 32,
        "pg_autoscale_mode": "on",
        "pg_num_target": 32,
        "pg_num_pending": 32
      },
      "crush_rule": "replicated_rule",
      "application_metadata": {
        "rgw": {}
      },
      "performance": {
        "iops_12h": 0.4102737867177525,
        "bw_bytes_12h": 328.94038314176277,
        "latency_ms": 21.689253921051538
      }
    },
    {
      "pool_id": 9,
      "pool_name": "default.rgw.buckets.data",
      "percent_used": 40.8494,
      "bytes_used": "16.16 TB",
      "bytes_used_raw": 17771233763328,
      "objects": 4671323,
      "max_avail": "15.60 TB",
      "max_avail_raw": 17155383885824,
      "growth_24h": "99.14 GB",
      "growth_24h_raw": 106448265216,
      "dirty": 0,
      "device_class": "hdd",
      "replica_ec_conf": {
        "type": "erasure",
        "size": 6,
        "min_size": 4,
        "erasure_code_profile": "k4m2-osd"
      },
      "pg_conf": {
        "pg_num": 64,
        "pg_autoscale_mode": "off",
        "pg_num_target": 64,
        "pg_num_pending": 64
      },
      "crush_rule": "erasure_rule_k4m2_osd",
      "application_metadata": {
        "rgw": {}
      },
      "performance": {
        "iops_12h": 27.395426245210746,
        "bw_bytes_12h": 5454362.929042148,
        "latency_ms": 21.689253921051538
      }
    },
    {
      "pool_id": 10,
      "pool_name": "default.rgw.buckets.index",
      "percent_used": 0.1007,
      "bytes_used": "2.56 GB",
      "bytes_used_raw": 2744998057,
      "objects": 960,
      "max_avail": "845.53 GB",
      "max_avail_raw": 907878072320,
      "growth_24h": "14.75 MB",
      "growth_24h_raw": 15470281,
      "dirty": 0,
      "device_class": "ssd",
      "replica_ec_conf": {
        "type": "replicated",
        "size": 3,
        "min_size": 2,
        "erasure_code_profile": ""
      },
      "pg_conf": {
        "pg_num": 64,
        "pg_autoscale_mode": "off",
        "pg_num_target": 64,
        "pg_num_pending": 64
      },
      "crush_rule": "replicated_rule_ssd",
      "application_metadata": {
        "rgw": {}
      },
      "performance": {
        "iops_12h": 25.023165708812233,
        "bw_bytes_12h": 45825.024163473834,
        "latency_ms": 21.689253921051538
      }
    },
    {
      "pool_id": 11,
      "pool_name": "default.rgw.buckets.non-ec",
      "percent_used": 0.0,
      "bytes_used": "9.74 MB",
      "bytes_used_raw": 10208773,
      "objects": 244,
      "max_avail": "7.80 TB",
      "max_avail_raw": 8577691942912,
      "growth_24h": "-42.69 KB",
      "growth_24h_raw": -43713,
      "dirty": 0,
      "device_class": "hdd",
      "replica_ec_conf": {
        "type": "replicated",
        "size": 3,
        "min_size": 2,
        "erasure_code_profile": ""
      },
      "pg_conf": {
        "pg_num": 32,
        "pg_autoscale_mode": "on",
        "pg_num_target": 32,
        "pg_num_pending": 32
      },
      "crush_rule": "replicated_rule",
      "application_metadata": {
        "rgw": {}
      },
      "performance": {
        "iops_12h": 2.088413952745849,
        "bw_bytes_12h": 1118.2982375478928,
        "latency_ms": 21.689253921051538
      }
    }
  ]
}
root@gm1-pub-ceph-172-16-3-107:/tmp# 
