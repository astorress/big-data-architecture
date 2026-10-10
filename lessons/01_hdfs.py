# Lección 01 - HDFS por dentro: NameNode, DataNodes, bloques, réplicas y snapshots.
#
# HDFS guarda cada archivo partido en bloques grandes (128 MB por defecto) y copia cada bloque en varios DataNodes.
# El NameNode no guarda datos: solo sabe qué bloques forman cada archivo y en qué DataNodes vive cada uno, y lo tiene
# todo en memoria. Los clientes le preguntan dónde están los bloques y luego leen o escriben directo en los DataNodes.
import re
from urllib.parse import urlparse

import requests

from lab.config import BRONZE, NAMENODE_HTTP
from lab.hdfs import datanodes, hdfs, namenode_jmx, webhdfs
from lab.show import show, step, table, title

DAY_FILE = f"{BRONZE}/readings/readings_2026-09-01.csv"
DEMO = "/tmp/hdfs-demo"

title("01 - HDFS")

step("El clúster según el NameNode (JMX): capacidad, archivos, bloques y DataNodes vivos")
state = namenode_jmx("FSNamesystemState")
system = namenode_jmx("FSNamesystem")
table([{
    "capacity_gb": round(state["CapacityTotal"] / 2**30, 1),
    "used_mb": round(state["CapacityUsed"] / 2**20, 1),
    "files": system["FilesTotal"],
    "blocks": state["BlocksTotal"],
    "live_datanodes": state["NumLiveDataNodes"],
    "dead_datanodes": state["NumDeadDataNodes"],
    "under_replicated": state["UnderReplicatedBlocks"],
}])
nodes = datanodes()
table(nodes)
names = {n["address"].split(":")[0]: n["datanode"] for n in nodes}


def fsck(path):
    """Bloques de un archivo y los DataNodes donde vive cada réplica."""
    output = hdfs("fsck", path, "-files", "-blocks", "-locations")
    rows = []
    for line in output.splitlines():
        match = re.match(r"^(\d+)\. .*:(blk_\d+)_\d+ len=(\d+) Live_repl=(\d+)\s+\[(.*)\]", line)
        if match:
            hosts = re.findall(r"\[?DatanodeInfoWithStorage\[([\d.]+):", match.group(5))
            rows.append({
                "block": int(match.group(1)),
                "id": match.group(2),
                "bytes": int(match.group(3)),
                "replicas": int(match.group(4)),
                "datanodes": ", ".join(names.get(h, h) for h in hosts),
            })
    return rows, output


step(f"hdfs dfs -stat: tamaño, tamaño de bloque y replicación de {DAY_FILE}")
print(hdfs("dfs", "-stat", "size=%b  blocksize=%o  replication=%r  modified=%y", DAY_FILE))
print(
    "El archivo (3,7 MB) cabe en un bloque de 128 MB: el bloque solo ocupa en disco lo que mide el archivo.")

step("hdfs fsck: el bloque del archivo y los dos DataNodes que guardan una copia")
blocks, output = fsck(DAY_FILE)
table(blocks)
print(re.search(r"Status: \w+", output).group(0))

step("El mismo archivo copiado con bloques de 1 MB (-D dfs.blocksize): ahora son 4 bloques")
hdfs("dfs", "-rm", "-r", "-f", "-skipTrash", DEMO)
hdfs("dfs", "-mkdir", "-p", DEMO)
hdfs("dfs", "-D", "dfs.blocksize=1m", "-cp", DAY_FILE, f"{DEMO}/readings_1mb.csv")
blocks, _ = fsck(f"{DEMO}/readings_1mb.csv")
table(blocks)
print("Cada bloque tiene 2 réplicas, una en cada DataNode (dfs.replication=2).")
print(
    "Spark y MapReduce asignan una tarea por bloque y la ejecutan, si pueden, en un nodo que tiene ese bloque."
)

step("WebHDFS: la API REST de HDFS. GETFILESTATUS devuelve los metadatos que guarda el NameNode")
show(webhdfs(f"{DEMO}/readings_1mb.csv", "GETFILESTATUS")["FileStatus"])

step("Replicación: bajar a 1 copia libera la mitad del espacio; volver a 2 hace que el NameNode la recree")
hdfs("dfs", "-setrep", "-w", "1", f"{DEMO}/readings_1mb.csv")
print(hdfs("dfs", "-du", "-h", "-v", DEMO))
hdfs("dfs", "-setrep", "-w", "2", f"{DEMO}/readings_1mb.csv")
print(hdfs("dfs", "-du", "-h", "-v", DEMO))
print(
    "-w espera a que termine: el NameNode ordena a un DataNode copiar cada bloque al otro (re-replicación).")

step(
    "El problema de los archivos pequeños: cada archivo y cada bloque es un objeto en la memoria del NameNode"
)
system = namenode_jmx("FSNamesystem")
objects = system["FilesTotal"] + system["BlocksTotal"]
print(f"objetos en el NameNode: {objects} (archivos + carpetas + bloques), ~150 bytes de heap cada uno")
print(
    "Los 30 CSV diarios (107 MB) son 30 archivos y 30 bloques. En un solo archivo serían 1 archivo y 1 bloque."
)
print(
    "Con miles de millones de archivos pequeños el NameNode se queda sin memoria y cada trabajo lanza una tarea"
)
print(
    "por archivo. Por eso la zona silver (lección 04) compacta los datos en pocos archivos Parquet grandes.")

step("Snapshots: una foto de solo lectura de una carpeta, sin copiar datos")
hdfs("dfsadmin", "-allowSnapshot", DEMO)
hdfs("dfs", "-createSnapshot", DEMO, "antes")
hdfs("dfs", "-rm", "-skipTrash", f"{DEMO}/readings_1mb.csv")
print("después de borrar:", hdfs("dfs", "-ls", DEMO) or "(carpeta vacía)")
print("en el snapshot:   ", hdfs("dfs", "-ls", "-C", f"{DEMO}/.snapshot/antes"))
hdfs("dfs", "-cp", f"{DEMO}/.snapshot/antes/readings_1mb.csv", DEMO)
print("restaurado:       ", hdfs("dfs", "-ls", "-C", DEMO))
hdfs("dfs", "-deleteSnapshot", DEMO, "antes")
hdfs("dfsadmin", "-disallowSnapshot", DEMO)

step(
    "Leer un rango de bytes con WebHDFS: el NameNode responde con una redirección al DataNode que tiene el bloque"
)
response = requests.get(f"{NAMENODE_HTTP}/webhdfs/v1{DEMO}/readings_1mb.csv",
                        params={
                            "op": "OPEN",
                            "offset": 2_500_000,
                            "length": 120
                        },
                        timeout=10)
first, datanode = response.history[0], urlparse(response.url)
print(f"1. NameNode -> {first.status_code} {first.reason}, Location: {datanode.hostname}:{datanode.port}")
print(f"2. DataNode -> {response.status_code}, {len(response.content)} bytes del bloque 2:")
print("  ", response.text.splitlines()[1])
print("Los datos nunca pasan por el NameNode: por eso un solo NameNode atiende a miles de clientes.")

hdfs("dfs", "-rm", "-r", "-skipTrash", DEMO)
print("\nSiguiente: python lessons/02_mapreduce.py")
