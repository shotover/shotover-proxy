#!/usr/bin/env bash

SCRIPT_DIR="$(cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

docker-compose -f $SCRIPT_DIR/../../examples/redis-multi/docker-compose.yml down