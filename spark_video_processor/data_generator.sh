#!/usr/bin/env bash
set -e
export PATH=$PWD/env/bin:$PATH
export PYTHONPATH=$PWD/../pravega-gateway/src/main/python
src/main/python/data_generator.py $*
