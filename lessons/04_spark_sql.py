# Lección 04 - Spark SQL y la arquitectura medallion: bronze (crudo) -> silver (limpio) -> gold (agregado).
#
# Los CSV de bronze se leen con un esquema explícito, se miden sus problemas de calidad, se limpian y se guardan en
# silver como Parquet particionado por fecha. Con silver se construyen las tablas gold que usan el negocio y los
# modelos de las lecciones 08-10. Todo se puede escribir con la API de DataFrames o con SQL: es el mismo motor.
from pyspark.sql import Window
from pyspark.sql import functions as F

from lab import dataset
from lab.config import BRONZE, GOLD, SILVER
from lab.hdfs import hdfs
from lab.show import step, title
from lab.spark import describe, session

title("04 - Spark SQL y medallion")
spark = session("04-spark-sql")
describe(spark)

step("Bronze: leer los CSV con un esquema explícito (inferSchema obligaría a leer todo dos veces)")
raw = spark.read.csv(f"{BRONZE}/readings",
                     header=True,
                     schema="meter_id string, ts timestamp, kwh double, voltage double")
raw.printSchema()
raw.show(3)

step("Calidad de datos: lo que llegó mal del sistema de medición")
expected = dataset.METERS * dataset.SLOTS * dataset.DAYS
quality = raw.agg(
    F.count("*").alias("rows"),
    F.count_distinct("meter_id", "ts").alias("distinct"),
    F.sum(F.when(F.col("kwh") < 0, 1).otherwise(0)).alias("negative_kwh"),
    F.sum(F.when(F.col("voltage").isNull(), 1).otherwise(0)).alias("null_voltage"),
).first()
print(f"filas={quality['rows']:,}  únicas={quality['distinct']:,}  "
      f"duplicadas={quality['rows'] - quality['distinct']:,}  faltantes={expected - quality['distinct']:,}  "
      f"kWh negativos={quality['negative_kwh']:,}  voltaje vacío={quality['null_voltage']:,}")

step("Silver: sin duplicados, kWh negativos corregidos (error de signo) y una columna date para particionar")
readings = (raw.dropDuplicates(["meter_id", "ts"]).withColumn("sign_fixed",
                                                              F.col("kwh")
                                                              < 0).withColumn("kwh", F.abs("kwh")).withColumn(
                                                                  "date", F.to_date("ts")))
(readings.repartition("date")  # una partición de Spark por día -> un archivo por carpeta date=...
 .write.mode("overwrite").partitionBy("date").parquet(f"{SILVER}/readings"))
print("\n".join(hdfs("dfs", "-ls", "-C", f"{SILVER}/readings").splitlines()[:4]), "\n...")
print(hdfs("dfs", "-du", "-s", "-h", f"{BRONZE}/readings", f"{SILVER}/readings"))
print(
    "Parquet es columnar y comprimido: ocupa una fracción del CSV y guarda el esquema y estadísticas por bloque."
)

step("Silver: el catálogo de medidores, el clima y las inspecciones, tipados y en Parquet")
meters = spark.read.csv(f"{BRONZE}/meters", header=True, inferSchema=True)
weather = spark.read.csv(f"{BRONZE}/weather",
                         header=True,
                         schema="city string, ts timestamp, temperature_c double")
inspections = spark.read.csv(f"{BRONZE}/inspections", header=True, inferSchema=True)
for name, frame in (("meters", meters), ("weather", weather), ("inspections", inspections)):
    frame.coalesce(1).write.mode("overwrite").parquet(f"{SILVER}/{name}")
meters.printSchema()

step("SQL: con vistas temporales, las tablas de silver se consultan con SQL estándar")
spark.read.parquet(f"{SILVER}/readings").createOrReplaceTempView("readings")
spark.read.parquet(f"{SILVER}/meters").createOrReplaceTempView("meters")
spark.read.parquet(f"{SILVER}/weather").createOrReplaceTempView("weather")
spark.sql("""
    SELECT m.customer_type,
           count(DISTINCT m.meter_id)                                  AS meters,
           round(sum(r.kwh) / 1000, 1)                                 AS mwh,
           round(sum(r.kwh) / count(DISTINCT m.meter_id) / 30, 2)     AS kwh_day_per_meter
    FROM readings r JOIN meters m USING (meter_id)
    GROUP BY m.customer_type ORDER BY mwh DESC
""").show()

step("SQL: consumo residencial promedio por estrato (kWh/mes)")
spark.sql("""
    SELECT stratum, round(avg(month_kwh), 1) AS avg_kwh_month, count(*) AS meters
    FROM (SELECT meter_id, sum(kwh) AS month_kwh FROM readings GROUP BY meter_id) r
    JOIN meters m USING (meter_id)
    WHERE customer_type = 'residencial'
    GROUP BY stratum ORDER BY stratum
""").show()

step("Gold: consumo diario por medidor")
daily_meter = spark.sql("""
    SELECT meter_id, date,
           round(sum(kwh), 3)                        AS kwh,
           count(*)                                  AS readings,
           min(voltage)                              AS min_voltage,
           sum(CASE WHEN kwh = 0 THEN 1 ELSE 0 END)  AS zero_readings,
           count(DISTINCT kwh)                       AS distinct_values
    FROM readings GROUP BY meter_id, date
""")
daily_meter.write.mode("overwrite").parquet(f"{GOLD}/daily_meter")
spark.read.parquet(f"{GOLD}/daily_meter").orderBy("meter_id", "date").show(3)

step("Funciones de ventana: cada medidor contra su propia historia (semana actual vs. semana anterior)")
# Ventanas de 7 filas (7 días) que se deslizan día a día. Comparar semanas completas evita confundir un fin de
# semana (los comercios cierran) con una caída real del consumo.
by_meter = Window.partitionBy("meter_id").orderBy("date")
trend = (spark.read.parquet(f"{GOLD}/daily_meter").withColumn(
    "kwh_7d",
    F.round(F.avg("kwh").over(by_meter.rowsBetween(-6, 0)),
            2)).withColumn("prev_7d",
                           F.round(F.avg("kwh").over(by_meter.rowsBetween(-13, -7)),
                                   2)).withColumn("ratio",
                                                  F.round(F.col("kwh_7d") / F.col("prev_7d"),
                                                          2)).withColumn("day",
                                                                         F.row_number().over(by_meter)))
example = sorted(dataset.FRAUD)[0]  # uno de los medidores con fraude simulado
(trend.where((F.col("meter_id") == example) & (F.col("day") % 3 == 0)).select(
    "meter_id", "date", "kwh", "kwh_7d", "prev_7d", "ratio").show(10))
print("Medidores cuya semana cayó por debajo del 70 % de la anterior: candidatos a fraude (lección 09)")
(trend.where("ratio < 0.7").groupBy("meter_id").agg(
    F.min("date").alias("first_drop"),
    F.min("ratio").alias("min_ratio")).join(spark.table("meters"),
                                            "meter_id").select("meter_id", "customer_type", "city",
                                                               "first_drop",
                                                               "min_ratio").orderBy("min_ratio").show(5))

step("Gold: carga horaria por circuito (feeder) con la temperatura de su ciudad")
feeder_hourly = spark.sql("""
    SELECT m.feeder, m.city, date_trunc('HOUR', r.ts) AS hour,
           round(sum(r.kwh), 3)        AS kwh,
           -- voltaje en por unidad (p.u.): medido / nominal (120 V, 208 V o 440 V según el tipo de cliente)
           round(avg(r.voltage / CASE m.customer_type WHEN 'residencial' THEN 120
                                                      WHEN 'comercial' THEN 208 ELSE 440 END), 3) AS voltage_pu,
           count(DISTINCT r.meter_id)  AS meters
    FROM readings r JOIN meters m USING (meter_id)
    GROUP BY m.feeder, m.city, date_trunc('HOUR', r.ts)
""").join(spark.table("weather").withColumnRenamed("ts", "hour"), ["city", "hour"], "left")
feeder_hourly.write.mode("overwrite").parquet(f"{GOLD}/feeder_hourly")
spark.read.parquet(f"{GOLD}/feeder_hourly").createOrReplaceTempView("feeder_hourly")
spark.table("feeder_hourly").orderBy("feeder", "hour").show(3)

step("Eventos de la red en gold: horas sin servicio (voltaje 0) y subtensión (< 95 % del nominal)")
spark.sql("""
    SELECT feeder, min(hour) AS start, max(hour) AS last_hour, count(*) AS hours
    FROM feeder_hourly WHERE voltage_pu = 0
    GROUP BY feeder
""").show()
spark.sql("""
    SELECT feeder, hour(hour) AS hour_of_day, round(avg(voltage_pu), 3) AS voltage_pu, count(*) AS days
    FROM feeder_hourly
    GROUP BY feeder, hour(hour) HAVING avg(voltage_pu) < 0.95
    ORDER BY feeder, hour_of_day
""").show()

step("Las zonas del data lake")
for zone in (BRONZE, SILVER, GOLD):
    print(hdfs("dfs", "-du", "-h", zone))

spark.stop()
print("\nSiguiente: python lessons/05_spark_performance.py")
