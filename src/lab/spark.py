import requests
from pyspark.sql import SparkSession

from lab.config import RESOURCEMANAGER_HTTP, SPARK_MASTER


# La configuración del clúster (YARN, memoria de los ejecutores, zona horaria) está en .docker/spark/spark-defaults.conf.
# Aquí solo se elige el nombre de la aplicación y lo propio de cada lección.
def session(name, **conf):
    builder = SparkSession.builder.master(SPARK_MASTER).appName(name)
    for key, value in conf.items():
        builder = builder.config(key.replace("__", "."), value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


# Dirección de la Spark UI vista desde el host. En YARN se entra por el proxy del ResourceManager.
def ui_url(spark):
    sc = spark.sparkContext
    if sc.master == "yarn":
        return f"http://localhost:8088/proxy/{sc.applicationId}/"
    return "http://localhost:4040/"


def describe(spark):
    sc = spark.sparkContext
    executors = sc._jsc.sc().getExecutorMemoryStatus().size() - 1  # sin contar el driver
    print(f"Spark {spark.version} · master={sc.master} · app={sc.applicationId} · ejecutores={executors} · "
          f"paralelismo={sc.defaultParallelism}")
    print("Spark UI:", ui_url(spark))


# API REST de la Spark UI (la misma información que se ve en el navegador). En YARN, a través del proxy del
# ResourceManager: el driver solo responde a peticiones que llegan por ese proxy.
def rest(spark, path):
    sc = spark.sparkContext
    base = f"{RESOURCEMANAGER_HTTP}/proxy/{sc.applicationId}" if sc.master == "yarn" else "http://localhost:4040"
    return requests.get(f"{base}/api/v1/applications/{sc.applicationId}/{path}", timeout=10).json()
