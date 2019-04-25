#! /bin/bash
set -ex

: ${DOCKER_REPOSITORY?"You must export DOCKER_REPOSITORY"}
: ${IMAGE_TAG?"You must export IMAGE_TAG"}

ROOT_DIR=$(dirname $0)/..

helm install \
    --name pravega-gateway \
    --namespace examples \
    charts/pravega-gateway \
    --set image.repository=${DOCKER_REPOSITORY}/pravega-gateway \
    --set image.tag=${IMAGE_TAG}

helm install \
    --name streaming-data-generator \
    --namespace examples \
    charts/streaming_data_generator \
    --set image.repository=${DOCKER_REPOSITORY}/streaming_data_generator \
    --set image.tag=${IMAGE_TAG}
