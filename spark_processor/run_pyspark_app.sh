#!/usr/bin/env bash
# Run Pravega PySpark applications locally.

set -ex

ROOT_DIR=$(dirname $0)/..
#export PYSPARK_PYTHON=${PYSPARK_PYTHON:-$(which python)}
#export PYSPARK_PYTHON=$PWD/env/bin/python
export PATH=$ROOT_DIR/env/bin:$PATH
export PYTHONPATH=$ROOT_DIR/env/bin/python

./run_spark_app.sh $*
