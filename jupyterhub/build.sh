#! /bin/bash
set -ex

: ${DOCKER_REPOSITORY?"You must export DOCKER_REPOSITORY"}

SOURCE_IMAGE=jupyter/all-spark-notebook
#SOURCE_TAG=2343e33dec46     # 2.4.0
SOURCE_TAG=f646d2b2a3af      # 2.4.1
#SOURCE_TAG=ae5f7e104dd5     # 2.4.2
TARGET_IMAGE=${DOCKER_REPOSITORY}/all-spark-notebook
TARGET_TAG=${TARGET_TAG:-${SOURCE_TAG}}

ROOT_DIR=$(dirname $0)/..

docker build -f Dockerfile ${ROOT_DIR} \
--build-arg SOURCE_IMAGE=${SOURCE_IMAGE} \
--build-arg SOURCE_TAG=${SOURCE_TAG} \
--tag ${TARGET_IMAGE}:${TARGET_TAG}

docker push ${TARGET_IMAGE}:${TARGET_TAG}
