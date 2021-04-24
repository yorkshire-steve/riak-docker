#!/bin/bash

SCRIPT_DIR=$(dirname $0)
BASE_DIR=$(cd $SCRIPT_DIR && pwd)

source $BASE_DIR/venv/bin/activate
pushd $BASE_DIR
export PYTHONPATH=src/
DOCKER_IMAGE_ID=$(docker build -q -t riak docker-riak)
DOCKER_ID=$(docker run -d -p 8098:8098 riak)
echo "Waiting for Riak to fully start..."
docker exec $DOCKER_ID riak-admin wait-for-service riak_kv
coverage run --source=src --omit=src/tests/*,*/__init__.py -m unittest discover -s tests -p *_test.py
coverage report -m
popd