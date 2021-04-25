#!/bin/bash

SCRIPT_DIR=$(dirname $0)
BASE_DIR=$(cd $SCRIPT_DIR && pwd)

source $BASE_DIR/venv/bin/activate
export PYTHONPATH=src/
pushd $BASE_DIR
docker-compose up -d
echo "Waiting for Riak to fully start..."
docker-compose exec riak riak-admin wait-for-service riak_kv
python -m unittest discover -s src -p *_test.py
docker-compose down
popd