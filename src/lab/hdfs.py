import json
import subprocess

import requests

from lab.config import NAMENODE_HTTP, RESOURCEMANAGER_HTTP


# La CLI de Hadoop (hdfs, yarn, mapred) lee la configuración del clúster de HADOOP_CONF_DIR=/hadoop/conf
def run(*command, check=True):
    result = subprocess.run(command, capture_output=True, text=True)
    if check and result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip())
    return result.stdout.strip()


def hdfs(*args, check=True):
    return run("hdfs", *args, check=check)


# JMX: cada demonio de Hadoop publica sus métricas internas como JSON en su puerto web
def namenode_jmx(name):
    return requests.get(f"{NAMENODE_HTTP}/jmx",
                        params={
                            "qry": f"Hadoop:service=NameNode,name={name}"
                        },
                        timeout=5).json()["beans"][0]


# WebHDFS: la API REST de HDFS, la misma que usa la interfaz web (puerto 9870)
def webhdfs(path, op, **params):
    response = requests.get(f"{NAMENODE_HTTP}/webhdfs/v1{path}", params={"op": op, **params}, timeout=10)
    response.raise_for_status()
    return response.json()


# API REST del ResourceManager de YARN
def yarn_api(path, **params):
    return requests.get(f"{RESOURCEMANAGER_HTTP}/ws/v1/cluster/{path}", params=params, timeout=5).json()


def datanodes():
    rows = []
    info = namenode_jmx("NameNodeInfo")
    for state, field in (("vivo", "LiveNodes"), ("muerto", "DeadNodes")):
        for name, node in json.loads(info[field]).items():
            rows.append({
                "datanode": name.split(":")[0],
                "state": state,
                "address": node.get("xferaddr", "-"),
                "blocks": node.get("numBlocks", "-"),
                "used_mb": round(node["used"] / 2**20, 1) if "used" in node else "-",
                "last_contact_s": node.get("lastContact", "-"),
            })
    return rows


def yarn_nodes():
    return [{
        "node": n["nodeHostName"],
        "state": n["state"],
        "containers": n["numContainers"],
        "used_mb": n["usedMemoryMB"],
        "available_mb": n["availMemoryMB"],
        "used_vcores": n["usedVirtualCores"],
        "available_vcores": n["availableVirtualCores"],
    } for n in yarn_api("nodes")["nodes"]["node"]]
