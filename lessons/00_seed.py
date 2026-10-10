# Lección 00 - Cargar el conjunto de datos en la zona bronze del data lake (HDFS).
#
# Un sistema de medición (AMI) entrega un archivo CSV por día con las lecturas de los medidores. Aquí se generan los 30
# días de septiembre de 2026 y se suben a HDFS tal como llegan, con sus errores: así empieza un data lake. El catálogo de
# medidores, el clima y las inspecciones de campo llegan como archivos aparte.
import csv
import os
import shutil
import tempfile
import time

from lab import dataset
from lab.config import BRONZE, CHECKPOINTS, LAKE, MODELS
from lab.hdfs import hdfs
from lab.show import step, table, title

title("00 - Seed del data lake")

step("Borrando el data lake, los modelos y los checkpoints anteriores (si existen)")
hdfs("dfs", "-rm", "-r", "-f", "-skipTrash", LAKE, MODELS, CHECKPOINTS)
hdfs("dfs", "-mkdir", "-p",
     *(f"{BRONZE}/{name}" for name in ("readings", "meters", "weather", "inspections")))

local = tempfile.mkdtemp(prefix="seed-")


def write_csv(name, header, rows):
    path = os.path.join(local, name)
    with open(path, "w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(header)
        writer.writerows(rows)
    return path


step(
    f"Catálogo: {dataset.METERS} medidores, clima horario de {len(dataset.CITIES)} ciudades e inspecciones de campo"
)
meters = dataset.meters()
files = {
    "meters":
    write_csv("meters.csv", dataset.METER_COLUMNS, ([m[c] for c in dataset.METER_COLUMNS] for m in meters)),
    "weather":
    write_csv("weather.csv", ["city", "ts", "temperature_c"], dataset.weather()),
    "inspections":
    write_csv("inspections.csv", ["meter_id", "inspected_at", "result"], dataset.inspections()),
}
for name, path in files.items():
    hdfs("dfs", "-put", "-f", path, f"{BRONZE}/{name}/")
table(meters[:5])

step(f"Lecturas: un CSV por día, {dataset.METERS} medidores x {dataset.SLOTS} lecturas de 15 minutos")
started = time.perf_counter()
readings = os.path.join(local, "readings")
os.makedirs(readings)
for day in dataset.days():
    with open(os.path.join(readings, f"readings_{day}.csv"), "w", encoding="utf-8") as file:
        file.write(dataset.readings_csv(day))
print(f"generados {len(dataset.days())} archivos en {time.perf_counter() - started:.1f} s")
with open(os.path.join(readings, f"readings_{dataset.START}.csv"), encoding="utf-8") as file:
    print("".join(next(file) for _ in range(4)), end="")

step("hdfs dfs -put: el cliente parte cada archivo en bloques y los escribe en los DataNodes")
started = time.perf_counter()
hdfs("dfs", "-put", "-f", *sorted(os.path.join(readings, f) for f in os.listdir(readings)),
     f"{BRONZE}/readings/")
print(f"subidos en {time.perf_counter() - started:.1f} s")
shutil.rmtree(local)

step("hdfs dfs -count: carpetas, archivos y bytes de cada zona")
print(hdfs("dfs", "-count", "-v", "-h", f"{BRONZE}/*"))

step("hdfs dfs -du: tamaño lógico y espacio real ocupado (x2 por la replicación)")
print(hdfs("dfs", "-du", "-s", "-h", "-v", BRONZE))

step("hdfs dfs -head: el primer kilobyte de un archivo, leído desde los DataNodes")
print("\n".join(hdfs("dfs", "-head", f"{BRONZE}/readings/readings_{dataset.START}.csv").splitlines()[:5]))
print("\nSiguiente: python lessons/01_hdfs.py")
