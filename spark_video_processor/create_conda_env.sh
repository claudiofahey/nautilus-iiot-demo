#!/usr/bin/env bash
conda create --name spark -c conda-forge \
  anaconda \
  ConfigArgParse \
  pyarrow \
  pyspark=2.4.1 \
  python=3.6 \
  tensorflow
