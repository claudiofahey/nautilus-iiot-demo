#!/usr/bin/env bash
set -e
export PATH=$PWD/env/bin:$PATH
export PYTHONPATH=$PWD/../pravega-gateway/src/main/python
export GENERATOR_SCOPE=${PRAVEGA_SCOPE:-examples11}
src/main/python/data_generator.py $*
