#! /bin/bash
set -ex

IMAGE_TAG=${IMAGE_TAG:-b9ff9c0}

: ${DOCKER_REPOSITORY?"You must export DOCKER_REPOSITORY"}
: ${IMAGE_TAG?"You must export IMAGE_TAG"}

ROOT_DIR=$(dirname $0)/../..

docker build -f Dockerfile ${ROOT_DIR} --tag ${DOCKER_REPOSITORY}/k8s-binderhub:${IMAGE_TAG}

docker push ${DOCKER_REPOSITORY}/k8s-binderhub:${IMAGE_TAG}
