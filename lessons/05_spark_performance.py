# Lección 05 - Rendimiento en Spark: formatos, poda de particiones y columnas, shuffle, joins, skew, AQE y caché.
#
# En big data lo que cuesta es mover bytes: leerlos del disco, enviarlos por la red en un shuffle y escribirlos. Cada
# paso de esta lección mide cuántos bytes leyó y movió una consulta (API REST de la Spark UI) y cuánto tardó.
import time

from pyspark.sql import functions as F

from lab.config import BRONZE, SILVER
from lab.hdfs import hdfs
from lab.show import step, table, title
from lab.spark import describe, rest, session

title("05 - Rendimiento en Spark")
spark = session("05-spark-performance")
describe(spark)
results = []


def measure(label, action):
    """Ejecuta una acción y suma lo que leyeron y movieron los stages que lanzó."""
    before = max((s["stageId"] for s in rest(spark, "stages")), default=-1)
    started = time.perf_counter()
    value = action()
    seconds = time.perf_counter() - started
    stages = [s for s in rest(spark, "stages") if s["stageId"] > before and s["status"] == "COMPLETE"]
    row = {
        "query": label,
        "seconds": round(seconds, 2),
        "stages": len(stages),
        "tasks": sum(s["numTasks"] for s in stages),
        "input_mb": round(sum(s["inputBytes"] for s in stages) / 2**20, 2),
        "shuffle_mb": round(sum(s["shuffleWriteBytes"] for s in stages) / 2**20, 2),
    }
    results.append(row)
    table([row])
    return value


csv = spark.read.csv(f"{BRONZE}/readings",
                     header=True,
                     schema="meter_id string, ts timestamp, kwh double, "
                     "voltage double")
parquet = spark.read.parquet(f"{SILVER}/readings")

step("Formato: el consumo de un día leído de CSV (bronze) y de Parquet particionado (silver)")
measure("csv: un día", lambda: csv.where(F.to_date("ts") == "2026-09-15").agg(F.sum("kwh")).first())
measure("parquet: un día", lambda: parquet.where("date = '2026-09-15'").agg(F.sum("kwh")).first())
print("CSV: hay que leer y parsear los 30 archivos completos para saber qué filas son de ese día.")
print(
    "Parquet particionado: Spark abre solo la carpeta date=2026-09-15 (poda de particiones) y solo la columna kwh."
)
parquet.where("date = '2026-09-15'").agg(F.sum("kwh")).explain()

step("Poda de columnas: Parquet guarda cada columna por separado; leer una sola cuesta mucho menos")
measure("parquet: 1 columna", lambda: parquet.agg(F.max("voltage")).first())
measure("parquet: 4 columnas",
        lambda: parquet.agg(F.max("voltage"), F.max("kwh"), F.max("ts"), F.max("meter_id")).first())

step("Shuffle: spark.sql.shuffle.partitions decide en cuántas tareas se reparte después de un groupBy")
# Un DataFrame nuevo en cada ejecución: el plan físico se fija la primera vez que se ejecuta un DataFrame
by_meter = lambda: parquet.groupBy("meter_id").agg(F.sum("kwh").alias("kwh")).collect()
spark.conf.set("spark.sql.adaptive.enabled", "false")
for partitions in (200, 8):
    spark.conf.set("spark.sql.shuffle.partitions", partitions)
    measure(f"groupBy, {partitions} particiones, sin AQE", by_meter)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", 200)
measure("groupBy, 200 particiones, con AQE", by_meter)
print("200 tareas para 1.000 medidores: casi todas vacías, y cada una cuesta lo mismo de arrancar.")
print("AQE (Adaptive Query Execution) mira el tamaño real tras el shuffle y une las particiones pequeñas.")
spark.conf.set("spark.sql.shuffle.partitions", 8)

step(
    "Joins: broadcast (la tabla pequeña viaja a cada ejecutor) contra sort-merge (las dos tablas se barajan)")
meters = spark.read.parquet(f"{SILVER}/meters")
by_type = lambda frame: frame.groupBy("customer_type").agg(F.sum("kwh")).collect()
measure("join broadcast", lambda: by_type(parquet.join(F.broadcast(meters), "meter_id")))
measure("join sort-merge", lambda: by_type(parquet.join(meters.hint("merge"), "meter_id")))
print(
    "Con broadcast no hay shuffle de las 2,8 millones de lecturas. Spark lo elige solo cuando la tabla pequeña"
)
print("mide menos de spark.sql.autoBroadcastJoinThreshold (10 MB por defecto).")
parquet.join(meters, "meter_id").explain()

step("Skew: una clave con muchas más filas que las demás deja una tarea haciendo casi todo el trabajo")
spark.conf.set("spark.sql.adaptive.enabled", "false")  # para ver el reparto tal cual


def records_per_task(frame):
    frame.collect()
    stage = max(rest(spark, "stages"), key=lambda s: s["stageId"])
    tasks = rest(spark, f"stages/{stage['stageId']}/0/taskList?length=50")
    return sorted((t["taskMetrics"]["shuffleReadMetrics"]["recordsRead"] for t in tasks), reverse=True)


# Sin agregación parcial: cada lectura viaja en el shuffle hasta la tarea de su tipo de cliente
joined = parquet.join(F.broadcast(meters), "meter_id")
skewed = joined.repartition(8, "customer_type")
print("filas por tarea, repartiendo por customer_type:",
      records_per_task(skewed.groupBy("customer_type").count()))
salted = joined.withColumn("salt", (F.rand(seed=1) * 8).cast("int")).repartition(8, "customer_type", "salt")
print("filas por tarea, con salt (clave + número 0-7):",
      records_per_task(salted.groupBy("customer_type", "salt").count()))
print("El 81 % de los medidores son residenciales: sin salt, una tarea recibe casi todas las filas.")
print("Con salt, cada tipo se reparte en 8 subgrupos; luego se suman los subtotales de cada tipo.")
spark.conf.set("spark.sql.adaptive.enabled", "true")

step(
    "cache(): consultas repetidas sobre el mismo DataFrame, la primera vez desde HDFS y después desde memoria"
)
week = parquet.where("date BETWEEN '2026-09-07' AND '2026-09-13'").join(F.broadcast(meters), "meter_id")
week.cache()
measure("semana, 1ª consulta (llena la caché)", lambda: week.groupBy("city").agg(F.sum("kwh")).collect())
measure("semana, 2ª consulta (desde caché)", lambda: week.groupBy("feeder").agg(F.avg("voltage")).collect())
print("input_mb de la 2ª consulta son bytes leídos de la caché en memoria de los ejecutores, no de HDFS.")
print(
    f"en memoria: {sum(r['memoryUsed'] for r in rest(spark, 'executors')) / 2**20:.1f} MB entre driver y ejecutores"
)
week.unpersist()

step("Escritura: cada partición de Spark escribe su propio archivo")
for label, frame in (("repartition(16)", parquet.repartition(16)), ("coalesce(2)", parquet.coalesce(2))):
    frame.write.mode("overwrite").parquet("/tmp/performance/readings")
    files = [
        f for f in hdfs("dfs", "-ls", "-C", "/tmp/performance/readings").splitlines()
        if f.endswith(".parquet")
    ]
    print(f"{label:16} -> {len(files)} archivos")
hdfs("dfs", "-rm", "-r", "-skipTrash", "/tmp/performance")
print(
    "Muchos archivos pequeños castigan al NameNode y a la siguiente lectura; pocos muy grandes limitan el paralelismo."
)
print("Una regla práctica: archivos de 128 MB a 1 GB.")

step("Resumen")
table(results)
spark.stop()
print("\nSiguiente: python lessons/06_kafka.py")
