#! /bin/bash
set -x

helm delete --purge pravega-gateway
helm delete --purge streaming-data-generator
