#!/bin/bash

COLOR_GREEN=$(tput setaf 2)
COLOR_BLUE=$(tput setaf 4)
COLOR_MAGENTA=$(tput setaf 5)
COLOR_DEFAULT=$(tput sgr0)

EXEC_PATH=$(dirname $(readlink -f "$0"))
HOME_PATH=$EXEC_PATH/..
SRC_PATH=$HOME_PATH/src
PYTHON_DIST_PACKAGES_PATH=/usr/local/lib/python3.13/site-packages
SPARK_JARS_ARCHIVE=/spark/spark-jars.zip
STEP=1
STEP_COUNT=6

# Memory check: the whole cluster (Hadoop, Kafka, Spark executors, this container and VS Code) needs ~8 GB
MEMORY_GB=$(awk '/MemTotal/ {printf "%d", $2 / 1048576}' /proc/meminfo)
if [ "$MEMORY_GB" -lt 9 ]; then
  echo "${COLOR_MAGENTA}WARNING:${COLOR_DEFAULT} Docker has ${MEMORY_GB} GB of memory. Spark jobs may be killed (OOM)."
  echo "         Give Docker at least 10 GB: Docker Desktop > Settings > Resources > Memory."
fi

# Generate home path file and add it to Python search path
echo "${COLOR_MAGENTA}${STEP}/$STEP_COUNT:${COLOR_GREEN} Generating ${COLOR_BLUE}big-data-architecture.pth${COLOR_GREEN} file and adding it to Python search path.${COLOR_DEFAULT}"
echo "$(readlink -f $SRC_PATH)" | sudo tee $PYTHON_DIST_PACKAGES_PATH/big-data-architecture.pth
let STEP++

# Install Python requirements.txt once. If requirements are satisfied, installation is skipped
pip freeze | sort > /tmp/installed.txt
sed -E 's/#.*//; s/^[[:space:]]+//; s/[[:space:]]+$//' "$HOME_PATH/requirements.txt" \
  | sed '/^$/d' \
  | sort > /tmp/required.txt
MISSING_LINES=$(comm -23 /tmp/required.txt /tmp/installed.txt | wc -l)
if [ "$MISSING_LINES" -eq 0 ]; then
  echo "${COLOR_MAGENTA}${STEP}/$STEP_COUNT:${COLOR_GREEN} Python requirements already satisfied. Skipping installation.${COLOR_DEFAULT}"
else
  echo "${COLOR_MAGENTA}${STEP}/$STEP_COUNT:${COLOR_GREEN} Installing Python requirements.txt dependencies...${COLOR_DEFAULT}"
  sudo pip install --upgrade --upgrade-strategy only-if-needed -r "$HOME_PATH/requirements.txt"
fi
rm /tmp/installed.txt /tmp/required.txt
let STEP++

# HDFS: the NameNode leaves safe mode once the DataNodes have reported their blocks
echo "${COLOR_MAGENTA}${STEP}/$STEP_COUNT:${COLOR_GREEN} Waiting for ${COLOR_BLUE}HDFS${COLOR_GREEN} (NameNode out of safe mode and 2 DataNodes)...${COLOR_DEFAULT}"
until hdfs dfsadmin -safemode get 2>/dev/null | grep -q "OFF"; do
  sleep 2
done
until [ "$(curl -s "$NAMENODE_HTTP/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState" | jq '.beans[0].NumLiveDataNodes')" -ge 2 ] 2>/dev/null; do
  sleep 2
done
hdfs dfs -mkdir -p /user/$USER /datalake /spark
let STEP++

# YARN: both NodeManagers registered with the ResourceManager
echo "${COLOR_MAGENTA}${STEP}/$STEP_COUNT:${COLOR_GREEN} Waiting for ${COLOR_BLUE}YARN${COLOR_GREEN} (2 NodeManagers)...${COLOR_DEFAULT}"
until [ "$(curl -s "$RESOURCEMANAGER_HTTP/ws/v1/cluster/metrics" | jq '.clusterMetrics.activeNodes')" -ge 2 ] 2>/dev/null; do
  sleep 2
done
let STEP++

# Spark jars in HDFS once (spark.yarn.archive): otherwise every application uploads ~400 MB of jars to YARN
echo "${COLOR_MAGENTA}${STEP}/$STEP_COUNT:${COLOR_GREEN} Uploading Spark jars to ${COLOR_BLUE}hdfs://$SPARK_JARS_ARCHIVE${COLOR_GREEN}...${COLOR_DEFAULT}"
SPARK_JARS=$(python -c "import os, pyspark; print(os.path.join(os.path.dirname(pyspark.__file__), 'jars'))")
SPARK_JARS_ID=$(ls "$SPARK_JARS" | md5sum | cut -c1-12)   # cambia si cambia la versión de Spark o algún jar
if [ "$(hdfs dfs -cat /spark/ID 2>/dev/null)" == "$SPARK_JARS_ID" ]; then
  echo "  Spark jars already uploaded ($SPARK_JARS_ID). Skipping."
else
  (cd "$SPARK_JARS" && python -m zipfile -c /tmp/spark-jars.zip *.jar)
  hdfs dfs -put -f /tmp/spark-jars.zip $SPARK_JARS_ARCHIVE
  echo "$SPARK_JARS_ID" | hdfs dfs -put -f - /spark/ID
  rm /tmp/spark-jars.zip
fi
let STEP++

# Kafka: the three brokers in the cluster metadata
echo "${COLOR_MAGENTA}${STEP}/$STEP_COUNT:${COLOR_GREEN} Waiting for ${COLOR_BLUE}Kafka${COLOR_GREEN} (3 brokers in $KAFKA_BOOTSTRAP_SERVERS)...${COLOR_DEFAULT}"
until python -c "
from confluent_kafka.admin import AdminClient
brokers = AdminClient({'bootstrap.servers': '$KAFKA_BOOTSTRAP_SERVERS'}).list_topics(timeout=5).brokers
assert len(brokers) == 3
" >/dev/null 2>&1; do
  sleep 2
done

hdfs dfsadmin -report 2>/dev/null | grep -E "^(Name|Live datanodes)"
yarn node -list 2>/dev/null | grep RUNNING
echo "${COLOR_GREEN}Ready. Next step: ${COLOR_BLUE}python lessons/00_seed.py${COLOR_DEFAULT}"
