#!/bin/bash
# Nodo maestro: guarda los metadatos de HDFS (NameNode) y reparte los recursos del clúster (ResourceManager).
# La primera vez formatea el NameNode: crea un sistema de archivos vacío en /data/namenode.
set -e

if [ ! -f /data/namenode/current/VERSION ]; then
  echo "Formateando el NameNode (primera vez)"
  hdfs namenode -format -nonInteractive -clusterId big-data-architecture
fi

hdfs namenode &
yarn resourcemanager &
mapred historyserver &

# Si un proceso termina, el contenedor también, y Docker lo reinicia (restart: unless-stopped)
wait -n
exit 1
