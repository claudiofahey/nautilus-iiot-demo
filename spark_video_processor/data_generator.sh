#!/usr/bin/env bash
set -e
#source activate ./env
export PATH=$PWD/env/bin:$PATH
export PYTHONPATH=$PWD/../../pravega-samples/scenarios/pravega-gateway/src/main/python
src/main/python/data_generator.py $*
