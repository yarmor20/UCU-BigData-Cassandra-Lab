#!/bin/bash

docker stop cassandra-node
docker rm cassandra-node

docker network rm my-cassandra-network
