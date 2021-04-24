#!/bin/bash

SCRIPT_DIR=$(dirname $0)
BASE_DIR=$(cd $SCRIPT_DIR && pwd)

source $BASE_DIR/venv/bin/activate
coverage run --source=src --omit=src/tests/*,*/__init__.py -m unittest discover -s tests -p *_test.py
coverage report -m