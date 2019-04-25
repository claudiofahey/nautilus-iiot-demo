#! /bin/bash
set -ex

: ${DOCKER_REPOSITORY?"You must export DOCKER_REPOSITORY"}
: ${IMAGE_TAG?"You must export IMAGE_TAG"}

ROOT_DIR=$(dirname $0)/..

IMAGE=${DOCKER_REPOSITORY}/spark_processor:${IMAGE_TAG}
APP_NAME=${1:-test_sensor_processor}
APP_NAME_DASHED=$(echo "$APP_NAME" | tr "_" "-")

export PATH=$PATH:$HOME/spark/current/bin

spark-submit \
--master k8s://10.246.27.93:8443 \
--deploy-mode cluster \
--name ${APP_NAME_DASHED} \
--driver-memory 4g \
--executor-memory 4g \
--conf spark.executor.instances=2 \
--conf spark.kubernetes.container.image=${IMAGE} \
--conf spark.kubernetes.container.image.pullPolicy=Always \
--conf spark.kubernetes.authenticate.driver.serviceAccountName=spark-processor-service-account \
--conf spark.kubernetes.namespace=examples \
--conf spark.kubernetes.pyspark.pythonVersion=3 \
--conf spark.kubernetes.driver.secrets.examples-pravega=/var/run/secrets/nautilus.dellemc.com/serviceaccount \
--conf spark.kubernetes.executor.secrets.examples-pravega=/var/run/secrets/nautilus.dellemc.com/serviceaccount \
--conf spark.kubernetes.submission.waitAppCompletion=false \
--conf spark.kubernetes.driver.volumes.persistentVolumeClaim.checkpointpvc.mount.path=/checkpoint \
--conf spark.kubernetes.driver.volumes.persistentVolumeClaim.checkpointpvc.mount.readOnly=false \
--conf spark.kubernetes.driver.volumes.persistentVolumeClaim.checkpointpvc.options.claimName=data-project \
--conf spark.kubernetes.driverEnv.CHECKPOINT_LOCATION=/checkpoint/spark_checkpoints_${APP_NAME} \
--jars \
local:///home/lib/pravega-connectors-spark-0.4.0-SNAPSHOT.jar,\
local:///home/lib/pravega-keycloak-credentials-0.4.0-2030.d99411b-0.0.1-020.26736d2-shadow.jar \
local:///home/spark_processor/src/main/python/${APP_NAME}.py \
$*
