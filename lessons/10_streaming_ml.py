# Lección 10 - ML en tiempo real: Kafka -> Spark Structured Streaming + modelo de la lección 08 -> alertas en Kafka.
#
# Cada hora cerrada de cada medidor se compara con lo que el modelo esperaba para esa hora (según el tipo de cliente,
# el día, la temperatura y la escala del medidor). Las desviaciones grandes, los cortes de servicio y las caídas de
# tensión se publican como alertas en otro tema de Kafka, donde cualquier sistema (un tablero, el despacho de
# cuadrillas) las puede consumir. Es la arquitectura Kappa: un solo flujo de eventos para todo.
import json
import threading
import time
from collections import Counter
from datetime import date

from pyspark.ml import PipelineModel
from pyspark.sql import functions as F

from lab import dataset
from lab.config import ALERTS_TOPIC, CHECKPOINTS, GOLD, KAFKA_BOOTSTRAP_SERVERS, MODELS, READINGS_TOPIC, SILVER
from lab.hdfs import hdfs
from lab.kafka import consumer
from lab.show import step, table, title
from lab.simulator import Replay
from lab.spark import describe, session

DAY = date(2026, 9, 30)
CHECKPOINT = f"{CHECKPOINTS}/10"
OUTAGE_FEEDER = "MED-02"
SPIKE_METER = "M00005"  # residencial de Barranquilla
BYPASS_METER = "M00003"  # residencial de Medellín

title("10 - ML en tiempo real")
hdfs("dfs", "-rm", "-r", "-f", "-skipTrash", CHECKPOINT)
spark = session("10-streaming-ml")
describe(spark)

step("Lo estático: el modelo entrenado, el catálogo con la escala de cada medidor y el clima")
model = PipelineModel.load(f"{MODELS}/hourly_kwh")
catalog = (spark.read.parquet(f"{SILVER}/meters").join(
    spark.read.parquet(f"{GOLD}/meter_profile"),
    "meter_id").withColumn("stratum", F.coalesce("stratum", F.lit(0))).withColumn(
        "nominal_voltage",
        F.expr(
            "CASE customer_type WHEN 'residencial' THEN 120 WHEN 'comercial' THEN 208 ELSE 440 END")).cache())
weather = spark.read.parquet(f"{SILVER}/weather").withColumnRenamed("ts", "hour").cache()
print(
    f"modelo: {[type(s).__name__ for s in model.stages]} · {catalog.count()} medidores · {weather.count()} horas de clima"
)

step("El stream: lecturas de Kafka -> consumo por medidor en ventanas de 1 hora (watermark de 15 minutos)")
readings = (spark.readStream.format("kafka").option(
    "kafka.bootstrap.servers",
    KAFKA_BOOTSTRAP_SERVERS).option("subscribe",
                                    READINGS_TOPIC).option("startingOffsets", "latest").load().select(
                                        F.from_json(
                                            F.col("value").cast("string"),
                                            "meter_id string, ts string, kwh double, voltage double").alias(
                                                "r")).select("r.*").withColumn("ts", F.to_timestamp("ts")))
hourly = (readings.withWatermark("ts", "15 minutes").groupBy(F.window("ts", "1 hour"), "meter_id").agg(
    F.sum("kwh").alias("kwh"),
    F.min("voltage").alias("min_voltage"),
    F.count("*").alias("readings")).select("meter_id",
                                           F.col("window.start").alias("hour"), "kwh", "min_voltage",
                                           "readings"))

step("Características (join stream-estático) y predicción: el mismo PipelineModel que en batch")
features = (hourly.join(catalog, "meter_id").join(weather, ["city", "hour"]).withColumn(
    "hour_of_day",
    F.hour("hour")).withColumn("day_of_week",
                               F.dayofweek("hour")).withColumn("is_weekend",
                                                               F.col("day_of_week").isin(1, 7).cast("int")))
scored = model.transform(features)

step("Reglas sobre la predicción: qué se considera una alerta")
alerts = (scored.withColumn("voltage_pu",
                            F.col("min_voltage") / F.col("nominal_voltage")).withColumn(
                                "alert",
                                F.expr("""
              CASE WHEN kwh = 0 AND min_voltage = 0                         THEN 'sin servicio'
                   WHEN voltage_pu < 0.90                                   THEN 'subtensión'
                   WHEN kwh > 3 * prediction AND kwh - prediction > 0.5     THEN 'consumo muy alto'
                   WHEN kwh < 0.25 * prediction                             THEN 'consumo muy bajo'
              END""")).where("alert IS NOT NULL"))
table([
    {
        "alert": "sin servicio",
        "rule": "kWh = 0 y voltaje mínimo = 0 en toda la hora"
    },
    {
        "alert": "subtensión",
        "rule": "voltaje mínimo < 90 % del nominal"
    },
    {
        "alert": "consumo muy alto",
        "rule": "kWh > 3 x lo esperado (y al menos 0,5 kWh más)"
    },
    {
        "alert": "consumo muy bajo",
        "rule": "kWh < 25 % de lo esperado (posible fraude o daño)"
    },
])

step(f"Sink: cada alerta como un mensaje JSON en el tema {ALERTS_TOPIC} (clave = meter_id)")
query = (alerts.select(
    F.col("meter_id").alias("key"),
    F.to_json(
        F.struct(
            "meter_id", "feeder", "city",
            F.date_format("hour", "yyyy-MM-dd HH:mm").alias("hour"), "alert",
            F.round("kwh", 3).alias("kwh"),
            F.round("prediction", 3).alias("expected_kwh"),
            F.round("voltage_pu", 3).alias("voltage_pu"))).alias("value")).writeStream.format("kafka").option(
                "kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS).option("topic", ALERTS_TOPIC).option(
                    "checkpointLocation",
                    f"{CHECKPOINT}/alerts").outputMode("append").trigger(processingTime="2 seconds").start())
while query.lastProgress is None:
    time.sleep(0.5)

step("Un consumidor de las alertas (como lo sería un tablero de operación), en otro hilo")
received = []
stop_consumer = threading.Event()


def watch_alerts():
    reader = consumer(f"operations-{int(time.time())}", **{"auto.offset.reset": "latest"})
    reader.subscribe([ALERTS_TOPIC])
    outages = Counter()  # un corte afecta a todo un circuito: se agrupa por hora y circuito

    def print_outages(before=None):
        for hour, feeder in sorted(k for k in outages if before is None or k[0] < before):
            print(
                f"  [{hour[11:]}] sin servicio     {outages.pop((hour, feeder))} medidores del circuito {feeder}"
            )

    while not stop_consumer.is_set():
        for message in reader.consume(num_messages=200, timeout=0.5):
            alert = json.loads(message.value())
            received.append(alert)
            print_outages(before=alert["hour"])
            if alert["alert"] == "sin servicio":
                outages[(alert["hour"], alert["feeder"])] += 1
            else:
                print(f"  [{alert['hour'][11:]}] {alert['alert']:16} {alert['meter_id']} ({alert['feeder']}) "
                      f"kWh={alert['kwh']} esperado={alert['expected_kwh']} V={alert['voltage_pu']} p.u.")
    print_outages()
    reader.close()


watcher = threading.Thread(target=watch_alerts, daemon=True)
watcher.start()


def incidents(slot, events):
    """Incidentes en vivo sobre las lecturas reales del día."""
    hour = slot // 4
    for event in events:
        meter = dataset.METER_LIST[int(event["meter_id"][1:]) - 1]
        if meter["feeder"] == OUTAGE_FEEDER and 10 <= hour < 12:  # corte de 2 horas en el circuito
            event["kwh"], event["voltage"] = 0.0, 0.0
        if event["meter_id"] == SPIKE_METER and hour >= 11:  # consumo 5 veces mayor (falla o conexión ilegal)
            event["kwh"] = round(event["kwh"] * 5, 3)
        if event["meter_id"] == BYPASS_METER and hour >= 9:  # el medidor deja de registrar casi todo
            event["kwh"] = round(event["kwh"] * 0.1, 3)
    return events


step(
    f"Simulador: el {DAY} de 08:00 a 14:00 con incidentes (2 s reales = 15 min). Alertas a medida que cierra cada hora"
)
print(f"Incidentes: corte en {OUTAGE_FEEDER} de 10:00 a 12:00, {SPIKE_METER} x5 desde las 11:00, "
      f"{BYPASS_METER} x0,1 desde las 09:00")
replay = Replay(DAY, start_slot=32, slots=24, seconds_per_slot=2.0, inject=incidents)
replay.start()
replay.join()
time.sleep(12)  # que el watermark cierre la última hora y el consumidor reciba sus alertas
query.stop()
stop_consumer.set()
watcher.join()

step("Resumen de alertas por tipo y hora")
summary = Counter((a["hour"][11:], a["alert"]) for a in received)
table([{"hour": h, "alert": a, "meters": n} for (h, a), n in sorted(summary.items())])
print(
    "La última hora (13:00) no aparece: el watermark solo la cerraría con lecturas posteriores a las 14:15.")
print("Las alertas quedaron en Kafka: otro consumidor las puede releer desde el principio cuando quiera.")

catalog.unpersist()
weather.unpersist()
spark.stop()
print("\nFin de las lecciones. El sandbox (notebooks/sandbox.ipynb) junta todo para experimentar.")
