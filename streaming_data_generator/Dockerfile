FROM continuumio/miniconda:4.5.12

WORKDIR /home

RUN conda create -y -n env -c conda-forge \
      ConfigArgParse \
      grpcio \
      grpcio-tools \
      opencv \
      python=3.6 && \
    echo "source activate env" > ~/.bashrc

RUN apt-get update \
    && apt-get install -y \
        libgl1-mesa-glx \
    && rm -rf /var/lib/apt/lists/*

COPY streaming_data_generator/src /home/streaming_data_generator/src
COPY pravega-gateway/src/main/python /home/lib

ENV PYTHONPATH=/home/lib
ENV PATH=/opt/conda/envs/env/bin:/home/streaming_data_generator/src/main/python:$PATH
