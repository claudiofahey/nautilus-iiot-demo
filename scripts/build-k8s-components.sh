#! /bin/bash
set -ex

: ${DOCKER_REPOSITORY?"You must export DOCKER_REPOSITORY"}
: ${IMAGE_TAG?"You must export IMAGE_TAG"}

ROOT_DIR=$(dirname $0)/..

docker build -f $ROOT_DIR/pravega-gateway/Dockerfile $ROOT_DIR --tag ${DOCKER_REPOSITORY}/pravega-gateway:${IMAGE_TAG}
docker push ${DOCKER_REPOSITORY}/pravega-gateway:${IMAGE_TAG}

docker build -f $ROOT_DIR/streaming_data_generator/Dockerfile $ROOT_DIR --tag ${DOCKER_REPOSITORY}/streaming_data_generator:${IMAGE_TAG}
docker push ${DOCKER_REPOSITORY}/streaming_data_generator:${IMAGE_TAG}
