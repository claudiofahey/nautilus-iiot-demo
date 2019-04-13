#!/usr/bin/env bash

export PATH=$PATH:$HOME/spark/current/bin

set -ex

#export PRAVEGA_CONTROLLER=tcp://${HOST_IP:-127.0.0.1}:9090
#export PRAVEGA_SCOPE=${PRAVEGA_SCOPE:-examples}
export PYSPARK_PYTHON=/home/faheyc/anaconda3/envs/spark/bin/python

master=local[2]
#master=spark://ubuntu:7077

spark-submit \
--master $master \
src/main/python/test1.py
