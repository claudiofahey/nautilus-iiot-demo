#!/usr/bin/env bash

set -ex

export PATH=$PATH:$HOME/spark/current/bin
export PYSPARK_PYTHON=$PWD/env/bin/python

master=local[2]
#master=spark://ubuntu:7077

spark-submit \
--master $master \
src/main/python/test_chunk_reassembly_from_files.py
