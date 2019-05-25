#!/usr/bin/env bash
set -e
ROOT_DIR=$(dirname $0)/..
export PATH=$ROOT_DIR/env/bin:$PATH
export PYTHONPATH=$ROOT_DIR/pravega-gateway/src/main/python
export GENERATOR_SCOPE=${PRAVEGA_SCOPE:-examples}
src/main/python/video_data_generator.py $*
