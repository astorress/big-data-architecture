######## HADOOP DISTRIBUTION ########
# La imagen oficial solo existe para amd64. De ella se copia /opt/hadoop, que es Java y corre en cualquier arquitectura.
FROM --platform=linux/amd64 apache/hadoop:3.5.0 AS hadoop


######## BASE IMAGE ########
# La misma imagen corre los nodos de Hadoop (HDFS + YARN) y el laboratorio. Así los ejecutores de Spark, que viven en
# los NodeManager, tienen la misma versión de Python y de PySpark que el driver.
FROM python:3.13-slim-trixie AS img-base

# Arguments
ARG USER=bigdata
ARG USER_HOME=/home/$USER
ARG VOLUME=/app
ARG SHELL=/bin/bash

RUN pip3 install --upgrade pip

RUN apt-get update

# Inital OS configuration
RUN apt-get install -y locales \
    && sed -i 's/^# *\(en_US.UTF-8\)/\1/' /etc/locale.gen \
    && locale-gen
RUN apt-get install -y sudo git curl jq less procps
# BLAS/LAPACK nativos para el álgebra lineal de Spark MLlib
RUN apt-get install -y libblas3 liblapack3

# Java 17 (Hadoop 3.5 y Spark 4 corren sobre él) y Hadoop: hdfs, yarn, mapred y los demonios
COPY --from=eclipse-temurin:17-jre /opt/java/openjdk /opt/java/openjdk
COPY --from=hadoop /opt/hadoop /opt/hadoop
RUN rm -rf /opt/hadoop/share/doc /opt/hadoop/etc/hadoop
ENV TZ=America/Bogota \
    JAVA_HOME=/opt/java/openjdk \
    HADOOP_HOME=/opt/hadoop \
    HADOOP_CONF_DIR=/hadoop/conf \
    YARN_CONF_DIR=/hadoop/conf \
    HADOOP_LOG_DIR=/data/logs \
    HADOOP_HEAPSIZE_MAX=256m \
    PATH=/opt/java/openjdk/bin:/opt/hadoop/bin:$PATH
RUN java -version && hadoop version | head -1

# Python requirements (PySpark incluido): los necesitan el driver y los ejecutores
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install -r /tmp/requirements.txt

# Conector de Kafka para Spark (no viene con Spark): sus jars van junto a los de Spark
ARG MAVEN=https://repo1.maven.org/maven2
RUN SPARK_VERSION=$(python -c "import pyspark; print(pyspark.__version__)") \
    && cd /usr/local/lib/python3.13/site-packages/pyspark/jars \
    && curl -fsSLO $MAVEN/org/apache/spark/spark-sql-kafka-0-10_2.13/$SPARK_VERSION/spark-sql-kafka-0-10_2.13-$SPARK_VERSION.jar \
    && curl -fsSLO $MAVEN/org/apache/spark/spark-token-provider-kafka-0-10_2.13/$SPARK_VERSION/spark-token-provider-kafka-0-10_2.13-$SPARK_VERSION.jar \
    && curl -fsSLO $MAVEN/org/apache/kafka/kafka-clients/3.9.2/kafka-clients-3.9.2.jar \
    && curl -fsSLO $MAVEN/org/apache/commons/commons-pool2/2.13.1/commons-pool2-2.13.1.jar

# Set up non root user
RUN echo "$USER ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers
RUN useradd -ms $SHELL $USER
RUN mkdir -p $VOLUME /data \
    && chown -R $USER:$USER $VOLUME /data

# Preserve bash history
RUN SNIPPET="export PROMPT_COMMAND='history -a' && export HISTFILE=/commandhistory/.bash_history" \
    && echo $SNIPPET >> "/root/.bashrc" \
    && mkdir /commandhistory \
    && touch /commandhistory/.bash_history \
    && chown -R $USER /commandhistory \
    && echo $SNIPPET >> "$USER_HOME/.bashrc"

USER $USER


######## LAB IMAGE ########
FROM img-base AS img-lab

# Install VSCode extensions dependencies
USER root
RUN pip3 install pylint yapf isort
USER $USER
