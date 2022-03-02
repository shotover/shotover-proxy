#!/usr/bin/env bash

SCRIPT_DIR="$(cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

find $SCRIPT_DIR/../../example-configs/ -name 'docker-compose.yml' -exec docker-compose -f {} rm -f -s \;

yes | docker volume prune
yes | docker network prune
