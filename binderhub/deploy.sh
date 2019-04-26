#! /bin/bash
set -ex

ROOT_DIR=$(dirname $0)/..

helm repo add jupyterhub https://jupyterhub.github.io/helm-chart/
helm repo update

RELEASE=binderhub
NAMESPACE=examples

helm upgrade --install \
$RELEASE \
jupyterhub/binderhub \
--namespace $NAMESPACE  \
--version=0.2.0-5536a0f \
--values secret.yaml \
--values config.yaml
