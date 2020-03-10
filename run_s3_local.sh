#!/bin/sh

docker run -it -d --rm \
    -p 4572:4572 \
    -e SERVICES=s3 \
    localstack/localstack

