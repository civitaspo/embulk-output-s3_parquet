#!/bin/sh

docker run -it -d --rm \
    -p 4566:4566 \
    -e SERVICES=s3 \
    localstack/localstack

