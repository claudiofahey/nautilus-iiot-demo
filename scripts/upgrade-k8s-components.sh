#! /bin/bash
set -ex

ROOT_DIR=$(dirname $0)/..

helm upgrade \
    pravega-gateway \
    ${ROOT_DIR}/charts/pravega-gateway

helm upgrade \
    streaming-data-generator \
    ${ROOT_DIR}/charts/streaming_data_generator
