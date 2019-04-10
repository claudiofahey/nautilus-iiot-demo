#!/usr/bin/env bash

export PATH=$PATH:$HOME/nautilus/spark/current/bin

set -ex

#export PRAVEGA_CONTROLLER=tcp://${HOST_IP:-127.0.0.1}:9090
#export PRAVEGA_SCOPE=${PRAVEGA_SCOPE:-examples}

master=local[2]
#master=spark://localhost:7077

spark-submit \
--master $master \
src/main/python/test1.py
