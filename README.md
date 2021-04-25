# Riak Docker

## About

This repo contains
- Dockerfile to create a Riak 2.9.8 container

  The included riak.conf sets up a next-gen replication queue named `q1_ttaaefs`.

- Example Python app within `src` and Dockerfile to build

  The example app will consume from a Riak next-gen replication queue and put items into DynamoDB.
  See `record.py` for the decoding of the Riak binary object from the next-gen repl queue.

- docker-compose.yml

  This will setup a Riak, local DynamoDB and Python app container.

  Anything put into Riak, e.g.
  ```
  curl -XPUT -H "Content-type: application/json" -d '{"test":"data"}' http://localhost:8098/buckets/test/keys/testKey
  ```
  will be consumed by the Python app and put into the local DynamoDB
  ```
  aws dynamodb scan --table-name test --endpoint-url http://localhost:8000
  ```

## Getting started

Run the following command in the root of the repo directory
```
docker-compose --profile app up -d
```
Often DynamoDB hasn't started before the Python app, so simply do `docker-compose --profile app up -d` again.
(TODO: Fix this)

## Tests

There is good coverage of unit tests for `record.py` and `sink.py`, the latter requires a local Riak instance on port 8098.

Running `ci-test.sh` should take care of everything for you.