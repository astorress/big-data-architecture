import os

# Direcciones de los servicios (.envs/lab.env)
HDFS_URL = os.getenv("HDFS_URL", "hdfs://master:8020")
NAMENODE_HTTP = os.getenv("NAMENODE_HTTP", "http://master:9870")
RESOURCEMANAGER_HTTP = os.getenv("RESOURCEMANAGER_HTTP", "http://master:8088")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka1:9092,kafka2:9092,kafka3:9092")
SPARK_MASTER = os.getenv("SPARK_MASTER", "yarn")

# Zonas del data lake en HDFS (arquitectura medallion)
LAKE = "/datalake"
BRONZE = f"{LAKE}/bronze"  # los archivos tal como llegan (CSV), sin tocar
SILVER = f"{LAKE}/silver"  # datos limpios y tipados (Parquet, particionado por fecha)
GOLD = f"{LAKE}/gold"  # agregados listos para el negocio y los modelos
MODELS = "/models"  # modelos de Spark MLlib
CHECKPOINTS = "/checkpoints"  # estado y offsets de las consultas de streaming

# Temas de Kafka
READINGS_TOPIC = "meter-readings"
ALERTS_TOPIC = "meter-alerts"
