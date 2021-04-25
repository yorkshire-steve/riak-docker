#!/bin/bash

SCRIPT_DIR=$(dirname $0)
BASE_DIR=$(cd $SCRIPT_DIR && pwd)

source $BASE_DIR/venv/bin/activate
export PYTHONPATH=${BASE_DIR}/src/
pushd $BASE_DIR
STARTED_BY_SCRIPT=false
if [ ! `docker-compose ps --services --filter "status=running" | grep riak` ] ; then
    STARTED_BY_SCRIPT=true
    docker-compose up -d
fi
echo "Waiting for Riak to fully start..."
docker-compose exec riak riak-admin wait-for-service riak_kv
python -m unittest discover -s src -p *_test.py
if $STARTED_BY_SCRIPT ; then
    docker-compose down
fi
popd