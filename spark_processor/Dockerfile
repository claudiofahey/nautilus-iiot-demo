# Dockerfile to run Spark Processor in Kubernetes

FROM continuumio/miniconda:4.5.12

RUN apt-get update \
    && apt-get install -y \
        libgl1-mesa-glx \
        openjdk-8-jdk \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/jre

ENV SPARK_VERSION 2.4.1
ENV SPARK_PACKAGE spark-${SPARK_VERSION}-bin-hadoop2.7
ENV SPARK_HOME /opt/spark
RUN curl -sL --retry 3 \
  "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_PACKAGE}.tgz" \
  | gunzip \
  | tar xv -C /opt/ \
 && mv /opt/$SPARK_PACKAGE $SPARK_HOME \
 && chown -R root:root $SPARK_HOME \
 && ln -s /opt/spark/kubernetes/dockerfiles/spark/entrypoint.sh /opt/ \
 && ln -s /usr/bin/tini /sbin/tini

RUN conda create -y -n env -c conda-forge \
      anaconda \
      ConfigArgParse \
      pyarrow \
      pyspark=2.4.1 \
      opencv \
      python=3.6 \
      tensorflow \
    && echo "source activate env" > ~/.bashrc

WORKDIR /home

COPY lib /home/lib
COPY spark_processor/src /home/spark_processor/src

ENV PYTHONPATH=/home/lib:$PYTHONPATH
ENV PATH=/opt/conda/envs/env/bin:$PATH

ENV pravega_client_auth_method=Bearer
ENV pravega_client_auth_loadDynamic=true
ENV KEYCLOAK_SERVICE_ACCOUNT_FILE=/var/run/secrets/nautilus.dellemc.com/serviceaccount/keycloak.json
ENV PRAVEGA_CONTROLLER=tcp://nautilus-pravega-controller.nautilus-pravega.svc.cluster.local:9090

ENTRYPOINT [ "/opt/entrypoint.sh" ]
