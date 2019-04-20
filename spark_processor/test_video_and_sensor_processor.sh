#!/usr/bin/env bash

set -ex

export PATH=$PATH:$HOME/spark/current/bin
export PRAVEGA_CONTROLLER=tcp://${HOST_IP:-127.0.0.1}:9090
export PRAVEGA_SCOPE=${PRAVEGA_SCOPE:-examples15}
export PYSPARK_PYTHON=$PWD/env/bin/python

master=local[2]
#master=spark://ubuntu:7077

spark-submit \
--master $master \
--driver-memory 12g \
--executor-memory 4g \
--jars ${HOME}/.m2/repository/io/pravega/pravega-connectors-spark/0.4.0-SNAPSHOT/pravega-connectors-spark-0.4.0-SNAPSHOT.jar \
src/main/python/test_video_and_sensor_processor.py
