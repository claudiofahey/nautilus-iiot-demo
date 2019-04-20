#!/usr/bin/env bash
conda create -y --prefix env -c conda-forge \
  anaconda \
  ConfigArgParse \
  grpcio \
  grpcio-tools \
  opencv \
  python=3.6
