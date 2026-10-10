# Lección 03 - Spark por dentro: driver, ejecutores, RDD, evaluación perezosa, stages, shuffle y localidad de datos.
#
# Spark hace lo mismo que MapReduce (map, shuffle, reduce) pero encadena muchos pasos en un solo trabajo y guarda los
# resultados intermedios en memoria en lugar de escribirlos en HDFS entre paso y paso. El driver (este programa, en el
# contenedor lab) arma el plan y lo reparte en tareas; los ejecutores (contenedores de YARN en worker1 y worker2) las
# ejecutan sobre las particiones de los datos.
import time
from collections import Counter

from pyspark.sql import functions as F

from lab import dataset
from lab.config import BRONZE
from lab.show import step, table, title
from lab.spark import describe, rest, session

title("03 - Spark core")

step("SparkSession en YARN: el ResourceManager le da a la aplicación un ApplicationMaster y 2 ejecutores")
spark = session("03-spark-core")
sc = spark.sparkContext
describe(spark)
table([{
    "id": e["id"],
    "host": e["hostPort"].split(":")[0],
    "cores": e["totalCores"],
    "storage_memory_mb": round(e["maxMemory"] / 2**20),
} for e in rest(spark, "executors")])
print(
    "driver: arma el plan y coordina; no procesa datos. Cada ejecutor es una JVM con 2 núcleos = 2 tareas a la vez."
)

step("RDD: una colección repartida en particiones. textFile crea una partición por bloque de HDFS")
lines = sc.textFile(f"{BRONZE}/readings")
print("particiones:", lines.getNumPartitions(), "(30 archivos de un bloque cada uno)")

step("Transformaciones perezosas: definir map, filter y reduceByKey no ejecuta nada")
started = time.perf_counter()
rows = lines.filter(lambda line: not line.startswith("meter_id")).map(lambda line: line.split(","))
valid = rows.filter(lambda r: r[3] != "" and float(r[2]) >= 0)
daily = valid.map(lambda r: (r[1][:10], float(r[2]))).reduceByKey(lambda a, b: a + b)
print(f"plan definido en {(time.perf_counter() - started) * 1000:.1f} ms; todavía no se ha leído ningún byte")

step("Acción: collect() lanza el trabajo. Lo mismo que el MapReduce de la lección 02, en un solo programa")
started = time.perf_counter()
result = sorted(daily.collect())
print(f"{len(result)} días en {time.perf_counter() - started:.1f} s ->",
      [(d, round(k, 1)) for d, k in result[:3]], "...")

step(
    "El linaje (DAG) del RDD: cada transformación recuerda de dónde viene; el shuffle parte el trabajo en 2 stages"
)
print(daily.toDebugString().decode())
print("Las líneas con la misma sangría van en un stage (transformaciones estrechas, sin mover datos).")
print(
    "reduceByKey es amplia: necesita todos los valores de cada día juntos, así que hay un shuffle entre stages."
)
print("Si un ejecutor muere, Spark recalcula solo las particiones perdidas siguiendo este linaje.")

step("Stages y tareas del último trabajo (API REST de la Spark UI): una tarea por partición")
stages = sorted(rest(spark, "stages"), key=lambda s: s["stageId"])[-2:]
table([{
    "stage": s["stageId"],
    "tasks": s["numTasks"],
    "input_mb": round(s["inputBytes"] / 2**20, 1),
    "shuffle_write_kb": round(s["shuffleWriteBytes"] / 2**10, 1),
    "shuffle_read_kb": round(s["shuffleReadBytes"] / 2**10, 1),
    "name": s["name"][:40],
} for s in stages])

step("Localidad de datos: en qué nodo corrió cada tarea del stage que leyó HDFS")
tasks = rest(spark, f"stages/{stages[0]['stageId']}/0/taskList?length=100")
table([{
    "host": h,
    "locality": l,
    "tasks": n
} for (h, l), n in sorted(Counter((t["host"], t["taskLocality"]) for t in tasks).items())])
print("NODE_LOCAL: la tarea leyó un bloque guardado en el disco de su propio nodo, sin pasar por la red.")

step("Particiones: glom() muestra cuántos elementos tiene cada una; repartition mueve datos, coalesce no")
numbers = sc.parallelize(range(20), 4)
print("parallelize(range(20), 4):", numbers.glom().collect())
print("coalesce(2):              ", numbers.coalesce(2).glom().collect())
print("repartition(3):           ", numbers.repartition(3).glom().collect())

step(
    "Variables compartidas: broadcast envía el catálogo una vez por ejecutor; un acumulador cuenta inválidas")
cities = sc.broadcast({m["meter_id"]: m["city"] for m in dataset.meters()})
invalid = sc.accumulator(0)


def city_energy(row):
    if row[3] == "" or float(row[2]) < 0:
        invalid.add(1)
        return []
    return [(cities.value[row[0]], float(row[2]))]


by_city = rows.flatMap(city_energy).reduceByKey(lambda a, b: a + b).collect()
table([{"city": c, "mwh": round(k / 1000, 1)} for c, k in sorted(by_city, key=lambda x: -x[1])])
print("lecturas inválidas contadas por el acumulador:", invalid.value)

step("cache(): la segunda acción sobre un RDD en memoria no vuelve a leer HDFS")
valid.cache()
for attempt in ("1ª (lee HDFS y guarda en memoria)", "2ª (desde memoria)"):
    started = time.perf_counter()
    count = valid.count()
    print(f"{attempt:34} count={count:,} en {time.perf_counter() - started:.2f} s")
valid.unpersist()

step("El mismo cálculo con DataFrames: Spark conoce el esquema y optimiza el plan (Catalyst), todo en la JVM")
started = time.perf_counter()
schema = "meter_id string, ts timestamp, kwh double, voltage double"
frame = (spark.read.csv(f"{BRONZE}/readings", header=True,
                        schema=schema).where("voltage IS NOT NULL AND kwh >= 0").groupBy(
                            F.to_date("ts").alias("day")).agg(F.sum("kwh").alias("kwh")).orderBy("day"))
first = frame.first()
print(f"primer día: {first['day']} = {first['kwh']:,.1f} kWh, en {time.perf_counter() - started:.1f} s")
frame.explain()
print("Se lee de abajo hacia arriba. Lo que el optimizador (Catalyst) hizo solo:")
print("- ReadSchema: lee 3 de las 4 columnas (meter_id no se usa).")
print("- PushedFilters: el filtro se aplica mientras se lee el archivo.")
print(
    "- partial_sum antes del Exchange (shuffle): cada ejecutor suma lo suyo, como el combiner de MapReduce.")
print(
    "Con RDD, cada línea pasa por un proceso de Python; con DataFrames, Python solo describe la consulta y la"
)
print("JVM la ejecuta. Por eso las lecciones siguientes usan DataFrames y SQL.")

spark.stop()
print("\nSiguiente: python lessons/04_spark_sql.py")
