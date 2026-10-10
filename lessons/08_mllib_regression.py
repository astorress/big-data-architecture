# Lección 08 - Spark MLlib: un modelo que predice el consumo horario de cada medidor.
#
# MLlib entrena en paralelo sobre DataFrames repartidos en el clúster. Un modelo se arma como un Pipeline: una cadena
# de transformadores (preparan columnas) y estimadores (aprenden de los datos con fit()). El Pipeline entrenado se
# guarda en HDFS y se aplica igual a un DataFrame batch o a un stream (lección 10).
import time

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import SQLTransformer, StringIndexer, VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit
from pyspark.sql import functions as F

from lab.config import GOLD, MODELS, SILVER
from lab.show import step, table, title
from lab.spark import describe, session

SPLIT_DATE = "2026-09-24"  # entrenar con 1-23 de septiembre, evaluar con 24-30
MODEL = f"{MODELS}/hourly_kwh"

title("08 - MLlib: regresión del consumo horario")
spark = session("08-mllib-regression")
describe(spark)

step("Datos: consumo por medidor y hora, con el catálogo del medidor y la temperatura de su ciudad")
hourly = (spark.read.parquet(f"{SILVER}/readings").groupBy("meter_id",
                                                           F.date_trunc("HOUR", "ts").alias("hour")).agg(
                                                               F.sum("kwh").alias("kwh")))
dataset = (
    hourly.join(spark.read.parquet(f"{SILVER}/meters"), "meter_id").join(
        spark.read.parquet(f"{SILVER}/weather").withColumnRenamed("ts", "hour"), ["city", "hour"]).withColumn(
            "hour_of_day", F.hour("hour")).withColumn("day_of_week",
                                                      F.dayofweek("hour"))  # 1 = domingo ... 7 = sábado
    .withColumn("is_weekend",
                F.col("day_of_week").isin(1, 7).cast("int")).withColumn("stratum",
                                                                        F.coalesce("stratum", F.lit(0))))
train = dataset.where(F.col("hour") < SPLIT_DATE)
test = dataset.where(F.col("hour") >= SPLIT_DATE)

step("Característica del medidor: su consumo horario promedio, calculado SOLO con los días de entrenamiento")
profile = train.groupBy("meter_id").agg(F.avg("kwh").alias("meter_avg_kwh"))
profile.write.mode("overwrite").parquet(f"{GOLD}/meter_profile")
profile = spark.read.parquet(f"{GOLD}/meter_profile")
# Etiqueta: el consumo relativo al promedio del medidor (1,0 = una hora típica). Así el modelo aprende la forma de
# la curva, que comparten medidores de tamaños muy distintos, en lugar de memorizar la escala de cada uno.
train = train.join(profile, "meter_id").withColumn("load_ratio",
                                                   F.col("kwh") / F.col("meter_avg_kwh")).cache()
test = test.join(profile, "meter_id").cache()
print(f"entrenamiento: {train.count():,} filas · prueba: {test.count():,} filas")
print(
    "Si el promedio incluyera los días de prueba, el modelo vería el futuro (fuga de datos) y la evaluación mentiría."
)
print(
    "Por la misma razón la división es por fecha y no aleatoria: se entrena con el pasado y se evalúa con el futuro."
)
train.select("meter_id", "hour", "customer_type", "temperature_c", "meter_avg_kwh", "kwh",
             "load_ratio").show(3)

step("Pipeline: StringIndexer -> VectorAssembler -> GBTRegressor -> SQLTransformer")
FEATURES = [
    "hour_of_day", "day_of_week", "is_weekend", "temperature_c", "stratum", "customer_type_idx", "city_idx"
]
pipeline = Pipeline(stages=[
    # Texto -> número: los árboles usan cada categoría como una división posible
    StringIndexer(inputCols=["customer_type", "city"], outputCols=["customer_type_idx", "city_idx"]),
    # Las columnas de entrada en un solo vector, el formato que esperan los modelos de MLlib
    VectorAssembler(inputCols=FEATURES, outputCol="features"),
    GBTRegressor(
        featuresCol="features", labelCol="load_ratio", predictionCol="ratio_prediction", maxIter=30, seed=42),
    # De vuelta a kWh: la forma predicha por la escala del medidor
    SQLTransformer(statement="SELECT *, ratio_prediction * meter_avg_kwh AS prediction FROM __THIS__"),
])
print(
    "GBT (gradient-boosted trees): cada árbol corrige el error de los anteriores. Spark reparte el cálculo de"
)
print(
    "las divisiones de cada árbol entre los ejecutores: cada uno resume sus filas y el driver elige el mejor corte."
)

step(
    "Ajuste de hiperparámetros con TrainValidationSplit: 75 % para entrenar cada candidato, 25 % para compararlos"
)
evaluator = RegressionEvaluator(labelCol="kwh", predictionCol="prediction", metricName="rmse")
gbt = pipeline.getStages()[2]
search = TrainValidationSplit(
    estimator=pipeline,
    estimatorParamMaps=ParamGridBuilder().addGrid(gbt.maxDepth, [5, 8]).build(),
    evaluator=evaluator,
    trainRatio=0.75,
    parallelism=1,  # un candidato a la vez: entrenar dos en paralelo duplica la memoria
    seed=42)
started = time.perf_counter()
tuned = search.fit(train)
print(f"2 candidatos entrenados en {time.perf_counter() - started:.0f} s")
table([{
    "maxDepth": params[gbt.maxDepth],
    "rmse_validation": round(metric, 4)
} for params, metric in zip(search.getEstimatorParamMaps(), tuned.validationMetrics)])
model = tuned.bestModel

step(
    "Evaluación con la última semana (que el modelo nunca vio) contra un baseline: la misma hora 7 días antes"
)
predictions = model.transform(test)
last_week = (dataset.select("meter_id", (F.col("hour") + F.expr("INTERVAL 7 DAYS")).alias("hour"),
                            F.col("kwh").alias("baseline")))
scored = predictions.join(last_week, ["meter_id", "hour"])


def metrics(frame, prediction):
    row = frame.agg(
        F.sqrt(F.avg((F.col(prediction) - F.col("kwh"))**2)).alias("rmse"),
        (F.sum(F.abs(F.col(prediction) - F.col("kwh"))) / F.sum("kwh")).alias("wape"),
        F.corr(prediction, "kwh").alias("r")).first()
    return {
        "rmse": round(row["rmse"], 3),
        "wape_pct": round(row["wape"] * 100, 1),
        "r2": round(row["r"]**2, 3)
    }


rows = []
for kind in ("residencial", "comercial", "industrial"):
    subset = scored.where(F.col("customer_type") == kind)
    rows.append({"customer_type": kind, "model": "baseline (hace 7 días)", **metrics(subset, "baseline")})
    rows.append({"customer_type": kind, "model": "GBT", **metrics(subset, "prediction")})
table(rows)
print(
    "WAPE: error absoluto total / consumo total, comparable entre tipos de cliente de tamaños muy distintos.")

step("Importancia de cada característica en los árboles")
importances = model.stages[2].featureImportances.toArray()
table(
    sorted(({
        "feature": f,
        "importance": round(float(i), 3)
    } for f, i in zip(FEATURES, importances)),
           key=lambda r: -r["importance"]))

step(f"Guardar el Pipeline entrenado en HDFS ({MODEL}) y volver a cargarlo")
model.write().overwrite().save(MODEL)
loaded = PipelineModel.load(MODEL)
print("etapas:", [type(s).__name__ for s in loaded.stages])
(loaded.transform(
    test.where("meter_id = 'M00005' AND hour >= '2026-09-30 17:00' AND hour < '2026-09-30 22:00'")).select(
        "meter_id", "hour", "temperature_c",
        F.round("kwh", 3).alias("kwh"),
        F.round("prediction", 3).alias("prediction")).orderBy("hour").show())

train.unpersist()
test.unpersist()
spark.stop()
print("\nSiguiente: python lessons/09_mllib_fraud.py")
