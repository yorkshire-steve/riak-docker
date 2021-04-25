#!/bin/bash

SCRIPT_DIR=$(dirname $0)
BASE_DIR=$(cd $SCRIPT_DIR && pwd)

source $BASE_DIR/venv/bin/activate
pushd $BASE_DIR
export PYTHONPATH=src/
docker-compose up -d
echo "Waiting for Riak to fully start..."
docker-compose exec riak riak-admin wait-for-service riak_kv
coverage run --source=src --omit=src/tests/*,*/__init__.py -m unittest discover -s tests -p *_test.py
coverage report -m
docker-compose down
popd