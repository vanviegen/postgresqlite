#!/bin/sh

TAG=create-standalone-postgresql
docker build -t $TAG .
docker run --rm $TAG sh -c 'tar c /standalone-postgresql-*.tar.gz' | tar xvC ..
docker rmi $TAG

