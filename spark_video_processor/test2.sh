#!/usr/bin/env bash

export PATH=$PATH:$HOME/spark/current/bin

set -ex

export PRAVEGA_CONTROLLER=tcp://${HOST_IP:-127.0.0.1}:9090
export PRAVEGA_SCOPE=${PRAVEGA_SCOPE:-examples4}
export PYSPARK_PYTHON=$PWD/env/bin/python

master=local[2]
#master=spark://ubuntu:7077

spark-submit \
--master $master \
--packages io.pravega:pravega-connectors-spark:0.4.0-SNAPSHOT \
src/main/python/test2.py
