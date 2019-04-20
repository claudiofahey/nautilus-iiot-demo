#!/usr/bin/env bash
set -e
export PATH=$PWD/env/bin:$PATH
export PYTHONPATH=$PWD/../pravega-gateway/src/main/python
export GENERATOR_SCOPE=${PRAVEGA_SCOPE:-examples14}
src/main/python/sensor_data_generator.py $*
