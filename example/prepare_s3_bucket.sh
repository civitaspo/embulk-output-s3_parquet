#!/usr/bin/env bash

aws s3 mb s3://example \
    --endpoint-url http://localhost:4572 \
    --region us-east-1

