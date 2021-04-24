#!/bin/bash

SCRIPT_DIR=$(dirname $0)
BASE_DIR=$(cd $SCRIPT_DIR && pwd)

source $BASE_DIR/venv/bin/activate
export PYTHONPATH=src/
pushd $BASE_DIR
DOCKER_IMAGE_ID=$(docker build -q -t riak docker-riak)
DOCKER_ID=$(docker run -d -p 8098:8098 riak)
echo "Waiting for Riak to fully start..."
docker exec $DOCKER_ID riak-admin wait-for-service riak_kv
python -m unittest discover -s src -p *_test.py
docker stop $DOCKER_ID
docker rm $DOCKER_ID
popd