#!/bin/bash
# Nodo trabajador: guarda bloques de HDFS (DataNode) y ejecuta contenedores de YARN (NodeManager).
# Que ambos vivan en la misma máquina es lo que permite la localidad de datos: llevar el cómputo a donde están los bloques.
set -e

hdfs datanode &
yarn nodemanager &

wait -n
exit 1
