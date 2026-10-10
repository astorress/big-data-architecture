# Lección 02 - MapReduce sobre YARN: energía diaria y lecturas inválidas de todo el mes.
#
# MapReduce divide un cálculo en dos funciones: map (cada línea -> pares clave/valor) y reduce (todos los valores de una
# clave -> un resultado). Entre las dos, el framework ordena y agrupa por clave (shuffle). YARN reparte las tareas en los
# NodeManagers, cerca de los bloques de HDFS. Con Hadoop Streaming, mapper y reducer son scripts de Python.
import os
import re
import subprocess
import time

from lab.config import BRONZE
from lab.hdfs import hdfs, yarn_api, yarn_nodes
from lab.show import step, table, title

MAPPER, REDUCER = "lessons/mapreduce/mapper.py", "lessons/mapreduce/reducer.py"
OUTPUT = "/tmp/mapreduce"
COUNTERS = {
    "Launched map tasks": "map_tasks",
    "Data-local map tasks": "data_local_maps",
    "Map input records": "map_input",
    "Map output records": "map_output",
    "Combine output records": "combine_output",
    "Reduce input records": "reduce_input",
    "Reduce shuffle bytes": "shuffle_bytes",
}

title("02 - MapReduce")


def pipe(command, text):
    return subprocess.run(command, input=text, capture_output=True, text=True, check=True).stdout


step("MapReduce en miniatura: cat | mapper | sort | reducer sobre 6 líneas de dos días")
sample = "".join(
    f"{line}\n" for d in (1, 2)
    for line in hdfs("dfs", "-head", f"{BRONZE}/readings/readings_2026-09-0{d}.csv").splitlines()[1:4])
mapped = pipe(["python", MAPPER], sample)
shuffled = "".join(sorted(mapped.splitlines(keepends=True)))
print("map     ->", mapped.splitlines())
print("shuffle ->", shuffled.splitlines(), "(ordenado: cada clave queda junta)")
print("reduce  ->", pipe(["python", REDUCER], shuffled).splitlines())

step("Los recursos de YARN: cada NodeManager ofrece 2 GB y 2 núcleos para contenedores")
table(yarn_nodes())


def mapreduce(name, generic=(), streaming=()):
    """Lanza un trabajo de Hadoop Streaming en YARN y devuelve sus contadores."""
    output = f"{OUTPUT}/{name}"
    hdfs("dfs", "-rm", "-r", "-f", "-skipTrash", output)
    started = time.perf_counter()
    # Con nivel INFO, el cliente imprime el progreso y, al final, los contadores del trabajo (por stderr)
    job = subprocess.run(
        [
            "mapred",
            "streaming",
            "-D",
            f"mapreduce.job.name=daily-energy-{name}",
            "-D",
            "mapreduce.job.reduces=2",
            *generic,  # opciones genéricas de Hadoop (-D, -files): van antes de las de streaming
            "-files",
            f"{MAPPER},{REDUCER}",
            *streaming,
            "-mapper",
            "python3 mapper.py",
            "-reducer",
            "python3 reducer.py",
            "-input",
            f"{BRONZE}/readings",
            "-output",
            output,
        ],
        capture_output=True,
        text=True,
        env={
            **os.environ, "HADOOP_ROOT_LOGGER": "INFO,console"
        })
    if job.returncode != 0:
        raise RuntimeError(job.stderr[-3000:])
    counters = {"job": name, "seconds": round(time.perf_counter() - started, 1)}
    for label, key in COUNTERS.items():
        match = re.search(rf"\s{label}=(\d+)", job.stderr)
        counters[key] = int(match.group(1)) if match else 0
    return counters


step("Trabajo 1: un map por archivo (30 archivos pequeños = 30 maps) y sin combiner")
print("Mientras corre: http://localhost:8088 muestra la aplicación en YARN y sus contenedores.")
results = [mapreduce("plain")]
table(results)

step("Trabajo 2: CombineTextInputFormat (varios archivos por map) y el reducer como combiner")
results.append(
    mapreduce(
        "combined",
        generic=[
            "-D", "mapreduce.input.fileinputformat.split.maxsize=67108864", "-D",
            "stream.map.input.ignoreKey=true"
        ],  # que el mapper reciba solo la línea, sin su offset
        streaming=[
            "-inputformat", "org.apache.hadoop.mapred.lib.CombineTextInputFormat", "-combiner",
            "python3 reducer.py"
        ]))
table(results)
print("Con combiner, cada map suma sus propias lecturas antes de enviarlas: el shuffle pasa de millones de")
print("registros a unas pocas decenas. Con menos maps, YARN arranca menos JVM, que es lo que más tarda aquí.")
print("data_local_maps: maps que corrieron en un NodeManager con una copia local de su bloque.")

step("El resultado en HDFS: un archivo part-0000N por cada reducer (2), cada uno con sus claves")
print(hdfs("dfs", "-ls", "-C", f"{OUTPUT}/combined"))
rows = []
for line in hdfs("dfs", "-cat", f"{OUTPUT}/combined/part-*").splitlines():
    day, kwh, readings, invalid = line.split("\t")
    rows.append({"day": day, "kwh": float(kwh), "readings": int(readings), "invalid": int(invalid)})
table(sorted(rows, key=lambda r: r["day"])[:10])
print(
    "... 96.000 lecturas esperadas por día: las diferencias son faltantes y duplicados (los limpia la lección 04)."
)

step("Las aplicaciones que corrieron en YARN")
apps = yarn_api("apps", states="FINISHED")["apps"]["app"]
table([{
    "id": a["id"],
    "name": a["name"],
    "type": a["applicationType"],
    "status": a["finalStatus"],
    "seconds": round(a["elapsedTime"] / 1000, 1),
} for a in sorted(apps, key=lambda a: a["startedTime"])[-4:]])
print("\nSiguiente: python lessons/03_spark_core.py")
