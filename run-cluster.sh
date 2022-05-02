#!/bin/bash

docker network create my-cassandra-network
docker run --name cassandra-node --network my-cassandra-network -p 9042:9042 -d cassandra:latest