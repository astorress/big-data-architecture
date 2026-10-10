# Lección 07 - Spark Structured Streaming: Kafka -> ventanas de tiempo con watermark -> HDFS, con checkpoints.
#
# Un stream se trata como una tabla que no deja de crecer: se escribe la misma consulta que en batch y Spark la
# ejecuta en micro-lotes, procesando solo lo nuevo desde el lote anterior. Los offsets de Kafka y el estado de las
# agregaciones se guardan en un checkpoint de HDFS: si la consulta se detiene, retoma donde quedó sin perder ni
# duplicar datos.
import time
from collections import Counter
from datetime import date, datetime

from pyspark.sql import functions as F

from lab import dataset
from lab.config import BRONZE, CHECKPOINTS, KAFKA_BOOTSTRAP_SERVERS, READINGS_TOPIC
from lab.hdfs import hdfs
from lab.show import step, table, title
from lab.simulator import LATE_FEEDER, Replay
from lab.spark import describe, session

DAY = date(2026, 9, 30)
ARCHIVE = f"{BRONZE}/readings_stream"
CHECKPOINT = f"{CHECKPOINTS}/07"

title("07 - Spark Structured Streaming")
hdfs("dfs", "-rm", "-r", "-f", "-skipTrash", ARCHIVE, CHECKPOINT)

step("SparkSession: el conector de Kafka (spark-sql-kafka) está instalado junto a los jars de Spark")
spark = session("07-spark-streaming")
describe(spark)

step("Fuente: el tema de Kafka como un DataFrame sin fin (readStream)")
stream = (
    spark.readStream.format("kafka").option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS).option(
        "subscribe", READINGS_TOPIC).option("startingOffsets",
                                            "latest")  # solo lo que llegue desde ahora (la primera vez)
    .load())
stream.printSchema()
readings = (stream.select(
    F.from_json(F.col("value").cast("string"),
                "meter_id string, ts string, kwh double, voltage double").alias("r"),
    F.col("timestamp").alias("kafka_time")).select("r.*",
                                                   "kafka_time").withColumn("ts", F.to_timestamp("ts")))
print("isStreaming:", readings.isStreaming)

meters = spark.read.parquet("/datalake/silver/meters").select("meter_id", "feeder")

step("Consulta 1: archivar cada lectura en HDFS (Parquet por fecha), exactamente una vez")
archive = (readings.withColumn("date", F.to_date("ts")).writeStream.format("parquet").option(
    "path",
    ARCHIVE).option("checkpointLocation",
                    f"{CHECKPOINT}/archive").partitionBy("date").trigger(processingTime="5 seconds").start())

step("Consulta 2: energía por circuito en ventanas de 1 hora (tiempo del evento) con watermark de 15 minutos")
closed_windows = []


def print_closed_windows(batch, batch_id):
    rows = batch.orderBy("window", F.desc("kwh")).collect()
    if rows:
        closed_windows.extend(rows)
        start = rows[0]["window"]["start"].strftime("%H:%M")
        top = ", ".join(f"{r['feeder']}={r['kwh']:.0f}" for r in rows[:3])
        print(f"  lote {batch_id}: se cerró la ventana {start} ({len(rows)} circuitos) -> {top} kWh")


feeder_load = (
    readings.join(F.broadcast(meters), "meter_id")  # join stream-estático con el catálogo
    .withWatermark("ts", "15 minutes").groupBy(F.window("ts", "1 hour"), "feeder").agg(
        F.round(F.sum("kwh"), 1).alias("kwh"),
        F.count("*").alias("readings")).writeStream.outputMode(
            "append")  # una ventana se emite una sola vez, cuando el watermark la cierra
    .foreachBatch(print_closed_windows).option(
        "checkpointLocation", f"{CHECKPOINT}/feeder_load").trigger(processingTime="2 seconds").start())

for query in (archive, feeder_load):
    while query.lastProgress is None:  # el primer micro-lote fija los offsets de inicio
        time.sleep(0.5)

step(
    f"Simulador: el {DAY} desde las 08:00, 2 s reales = 15 min. La mitad de {LATE_FEEDER} llega 2 horas tarde"
)
replay = Replay(DAY, start_slot=32, slots=32, seconds_per_slot=2.0, late_slots=8)
replay.start()
print(
    "Mientras corre, la pestaña Structured Streaming de la Spark UI grafica las tasas de entrada y de proceso."
)

step("Progreso de los micro-lotes durante 40 s")
progress = []
started = time.monotonic()
while time.monotonic() - started < 40:
    time.sleep(6)
    p = feeder_load.lastProgress
    state = p["stateOperators"][0] if p["stateOperators"] else {}
    progress.append({
        "batch":
        p["batchId"],
        "input_rows":
        p["numInputRows"],
        "rows_per_s":
        round(p["processedRowsPerSecond"]),
        # El progreso trae las horas en UTC; astimezone() las pasa a la hora local (TZ=America/Bogota)
        "watermark":
        datetime.fromisoformat(p["eventTime"]["watermark"]).astimezone().strftime("%H:%M"),
        "state_rows":
        state.get("numRowsTotal", 0),
        "late_dropped":
        state.get("numRowsDroppedByWatermark", 0),
        "slot":
        f"{replay.current_slot // 4:02d}:{replay.current_slot % 4 * 15:02d}",
    })
    print(progress[-1])
print(
    "watermark = hora del evento más reciente - 15 min. Las ventanas que terminan antes se cierran y se emiten."
)
print("late_dropped: datos que llegaron con su ventana ya cerrada y se descartan. Se cuentan después de la")
print("agregación parcial de cada ejecutor: son grupos (ventana, circuito), no lecturas sueltas.")
print("state_rows: ventanas abiertas que Spark mantiene en memoria (y en el checkpoint) esperando más datos.")

step("Detener las consultas (como si el clúster se cayera) mientras el simulador sigue enviando")
print(
    "Los avisos TaskKilled que pueden aparecer son esperables: se cancela el micro-lote que estaba en curso.")
archive.stop()
feeder_load.stop()
replay.join()
print(f"el simulador envió {replay.sent:,} lecturas en total")

step("Reanudar el archivo desde su checkpoint: availableNow procesa lo pendiente y se detiene")
print(hdfs("dfs", "-ls", "-C", f"{CHECKPOINT}/archive"))
resumed = (readings.withColumn("date", F.to_date("ts")).writeStream.format("parquet").option(
    "path", ARCHIVE).option("checkpointLocation",
                            f"{CHECKPOINT}/archive").partitionBy("date").trigger(availableNow=True).start())
resumed.awaitTermination()
archived = spark.read.parquet(ARCHIVE)
total, distinct = archived.count(), archived.select("meter_id", "ts").distinct().count()
table([{"sent": replay.sent, "archived": total, "distinct": distinct, "duplicates": total - distinct}])
print(
    "offsets/ dice qué rango de Kafka iba a procesar cada lote; commits/ cuáles terminaron. Al reanudar, Spark"
)
print(
    "rehace el lote que no alcanzó a confirmar y el sink de archivos ignora lo ya escrito: exactamente una vez."
)

step("Ventanas cerradas por circuito: el circuito con lecturas tardías quedó incompleto")
dropped = sum(p["stateOperators"][0].get("numRowsDroppedByWatermark", 0) for p in feeder_load.recentProgress
              if p["stateOperators"])
print(f"grupos parciales descartados por el watermark en toda la consulta: {dropped:,}")
expected = Counter(m["feeder"] for m in dataset.meters())  # medidores por circuito
table([{
    "window": r["window"]["start"].strftime("%H:%M"),
    "feeder": r["feeder"],
    "kwh": r["kwh"],
    "readings": r["readings"],
    "expected": expected[r["feeder"]] * 4,
} for r in closed_windows if r["feeder"] in (LATE_FEEDER, "BOG-02")][:8])
print(
    f"Cada medidor reporta 4 lecturas por hora. A {LATE_FEEDER} le faltan las de la mitad de sus medidores: llegaron"
)
print(
    "cuando el watermark ya había cerrado su ventana. Un watermark de 3 horas las habría contado, a cambio de emitir"
)
print("cada ventana más tarde y guardar más estado: es el equilibrio entre latencia y completitud.")

spark.stop()
print("\nSiguiente: python lessons/08_mllib_regression.py")
