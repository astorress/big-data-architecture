# Lección 09 - MLlib: perfiles de carga (no supervisado) y detección de fraude (supervisado).
#
# 1. KMeans agrupa los medidores por la forma de su curva diaria, sin saber de qué tipo es cada uno.
# 2. Con las 300 inspecciones de campo como etiquetas, un Random Forest aprende qué comportamiento delata un fraude y
#    ordena los 700 medidores no inspeccionados por probabilidad: la lista de las próximas visitas.
# Como los datos son sintéticos, al final se compara la lista con la verdad (dataset.FRAUD), algo imposible en la vida
# real.
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import BinaryClassificationEvaluator, ClusteringEvaluator
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql import Window
from pyspark.sql import functions as F

from lab import dataset
from lab.config import GOLD, MODELS, SILVER
from lab.show import step, table, title
from lab.spark import describe, session

title("09 - MLlib: perfiles de carga y fraude")
spark = session("09-mllib-fraud")
describe(spark)
readings = spark.read.parquet(f"{SILVER}/readings")
meters = spark.read.parquet(f"{SILVER}/meters")

step("Perfil de carga: consumo promedio de cada hora en días hábiles, dividido por el promedio del medidor")
hours = [str(h) for h in range(24)]
weekday = readings.where(F.dayofweek("ts").between(2, 6))
profiles = (weekday.groupBy("meter_id",
                            F.hour("ts").alias("h")).agg(F.avg("kwh").alias("kwh")).withColumn(
                                "shape",
                                F.col("kwh") /
                                F.avg("kwh").over(Window.partitionBy("meter_id"))).groupBy("meter_id").pivot(
                                    "h", list(range(24))).agg(F.first("shape")).join(meters, "meter_id"))
profiles = VectorAssembler(inputCols=hours, outputCol="features").transform(profiles).cache()
print(f"{profiles.count()} medidores, cada uno como un vector de 24 valores")

step(
    "KMeans con k = 2 a 5: el coeficiente de silueta mide qué tan separados quedan los grupos (1 = perfecto)")
silhouette = ClusteringEvaluator(featuresCol="features", metricName="silhouette")
scores = []
for k in range(2, 6):
    model = KMeans(k=k, seed=42, featuresCol="features").fit(profiles)
    scores.append({
        "k": k,
        "silhouette": round(silhouette.evaluate(model.transform(profiles)), 3),
        "model": model
    })
table([{k: v for k, v in s.items() if k != "model"} for s in scores])
best = max(scores, key=lambda s: s["silhouette"])
clusters = best["model"].transform(profiles)

step(f"Los {best['k']} grupos contra el tipo de cliente (que KMeans nunca vio)")
clusters.groupBy("prediction").pivot("customer_type").count().na.fill(0).orderBy("prediction").show()
centers = best["model"].clusterCenters()
table([{
    "cluster": i,
    "peak_hour": int(c.argmax()),
    "peak": round(float(c.max()), 2),
    "night_0_5h": round(float(c[0:6].mean()), 2),
    "office_9_17h": round(float(c[9:18].mean()), 2),
} for i, c in enumerate(centers)])
print(
    "Sin etiquetas, la forma de la curva separa hogares (pico en la noche) de comercios e industrias (de día)."
)
profiles.unpersist()

step("Características de cada medidor para detectar fraude (a partir de gold/daily_meter)")
daily = spark.read.parquet(f"{GOLD}/daily_meter")
by_meter = Window.partitionBy("meter_id").orderBy("date")
weekly = (daily.withColumn("kwh_7d",
                           F.avg("kwh").over(by_meter.rowsBetween(-6, 0))).withColumn(
                               "prev_7d",
                               F.avg("kwh").over(by_meter.rowsBetween(-13, -7))).withColumn(
                                   "week_ratio",
                                   F.col("kwh_7d") / F.col("prev_7d")))
night = (readings.groupBy("meter_id").agg(
    (F.sum(F.when(F.hour("ts") < 6, F.col("kwh"))) / F.sum("kwh")).alias("night_share")))
features = (weekly.groupBy("meter_id").agg(
    (F.avg(F.when(F.col("date") >= "2026-09-21", F.col("kwh"))) /
     F.avg(F.when(F.col("date") < "2026-09-11", F.col("kwh")))).alias("last_vs_first_10d"),
    F.min("week_ratio").alias("min_week_ratio"), (F.stddev("kwh") / F.avg("kwh")).alias("daily_cv"),
    F.sum(F.when(F.col("distinct_values") <= 2, 1).otherwise(0)).alias("flat_days"),
    F.avg("kwh").alias("kwh_day")).join(night, "meter_id").join(meters, "meter_id").withColumn(
        "kwh_per_kw",
        F.col("kwh_day") / F.col("contracted_kw")).withColumn("stratum", F.coalesce("stratum", F.lit(0))))
FEATURES = [
    "last_vs_first_10d", "min_week_ratio", "daily_cv", "flat_days", "night_share", "kwh_per_kw", "stratum",
    "customer_type_idx"
]

step("Etiquetas: las inspecciones de campo. 'medidor dañado' no es fraude: se detecta con una regla aparte")
inspections = spark.read.parquet(f"{SILVER}/inspections")
inspections.groupBy("result").count().show()
labeled = (features.join(inspections.where("result != 'medidor dañado'"),
                         "meter_id").withColumn("label", (F.col("result") == "fraude").cast("double")))
# Hay 9 normales por cada fraude: se le da más peso a cada fraude para que el modelo no lo ignore
ratio = labeled.where("label = 0").count() / labeled.where("label = 1").count()
labeled = labeled.withColumn("weight", F.when(F.col("label") == 1, ratio).otherwise(1.0)).cache()
labeled.groupBy("result").agg(
    F.count("*").alias("meters"), *[F.round(F.avg(f), 2).alias(f) for f in FEATURES[:5]]).show()

step("Random Forest con validación cruzada de 3 particiones (pocas etiquetas: se usan todas para validar)")
forest = RandomForestClassifier(featuresCol="features", labelCol="label", weightCol="weight", seed=42)
pipeline = Pipeline(stages=[
    StringIndexer(inputCol="customer_type", outputCol="customer_type_idx"),
    VectorAssembler(inputCols=FEATURES, outputCol="features"),
    forest,
])
evaluator = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderPR")
cv = CrossValidator(estimator=pipeline,
                    estimatorParamMaps=ParamGridBuilder().addGrid(forest.numTrees,
                                                                  [50]).addGrid(forest.maxDepth,
                                                                                [3, 6]).build(),
                    evaluator=evaluator,
                    numFolds=3,
                    seed=42)
model = cv.fit(labeled)
table([{
    "maxDepth": p[forest.maxDepth],
    "area_under_pr": round(m, 3)
} for p, m in zip(cv.getEstimatorParamMaps(), model.avgMetrics)])
print(
    "Área bajo la curva precisión-recall: con clases desbalanceadas dice más que la exactitud (un modelo que")
print("siempre dice 'normal' acierta el 90 % y no sirve para nada).")
best_forest = model.bestModel.stages[-1]
table(
    sorted(({
        "feature": f,
        "importance": round(float(i), 3)
    } for f, i in zip(FEATURES, best_forest.featureImportances.toArray())),
           key=lambda r: -r["importance"]))
model.bestModel.write().overwrite().save(f"{MODELS}/fraud_rf")

step("Lista de inspección: los 700 medidores sin visitar, ordenados por probabilidad de fraude")
pending = features.join(inspections, "meter_id", "left_anti")
ranked = (model.bestModel.transform(pending).withColumn(
    "fraud_probability",
    F.round(vector_to_array("probability")[1],
            3)).select("meter_id", "customer_type", "city", "fraud_probability",
                       F.round("last_vs_first_10d", 2).alias("last_vs_first_10d"),
                       F.round("min_week_ratio",
                               2).alias("min_week_ratio")).orderBy(F.desc("fraud_probability")))
ranked.write.mode("overwrite").parquet(f"{GOLD}/inspection_list")
top = ranked.limit(15).collect()
uninspected_fraud = set(dataset.FRAUD) - {r["meter_id"] for r in inspections.collect()}
table([{**r.asDict(), "really_fraud": r["meter_id"] in uninspected_fraud} for r in top])
found = sum(r["meter_id"] in uninspected_fraud for r in top)
print(
    f"Verdad (datos sintéticos): {found} de los {len(uninspected_fraud)} fraudes sin inspeccionar están en el top 15."
)

step("Regla para medidores dañados: varios días repitiendo la misma lectura")
flat = (features.where("flat_days >= 3").select("meter_id", "customer_type", "flat_days").join(
    inspections, "meter_id", "left").orderBy(F.desc("flat_days")).collect())
table([{**r.asDict(), "really_stuck": r["meter_id"] in dataset.STUCK} for r in flat])

labeled.unpersist()
spark.stop()
print("\nSiguiente: python lessons/10_streaming_ml.py")
