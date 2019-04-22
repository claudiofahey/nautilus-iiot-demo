#!/usr/bin/env bash

set -ex

# Use below for Nautilus
packages="--packages \
io.pravega:pravega-connectors-spark:0.4.0-SNAPSHOT,\
io.pravega:pravega-keycloak-credentials:0.4.0-2030.d99411b-0.0.1-020.26736d2"
export PRAVEGA_CONTROLLER=${PRAVEGA_CONTROLLER:-tcp://nautilus-pravega-controller.nautilus-pravega.svc.cluster.local:9090}

# Use below for local Pravega
#packages="--jars ${HOME}/.m2/repository/io/pravega/pravega-connectors-spark/0.4.0-SNAPSHOT/pravega-connectors-spark-0.4.0-SNAPSHOT.jar"
#export PRAVEGA_CONTROLLER=${PRAVEGA_CONTROLLER:-tcp://localhost:9090}

export PRAVEGA_SCOPE=${PRAVEGA_SCOPE:-examples}
export PYSPARK_PYTHON=$PWD/env/bin/python
export PATH=$PATH:$HOME/spark/current/bin

master=local[2]
#master=spark://localhost:7077

spark-submit \
--master $master \
--driver-memory 4g \
--executor-memory 4g \
$packages \
src/main/python/test_video_and_sensor_processor.py
