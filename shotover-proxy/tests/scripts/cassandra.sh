#!/usr/bin/env bash
set -e

# Get the script dir no matter where we exec this script from
SCRIPT_DIR="$(cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# Create a defered function to cleanup docker-compose. This also preserves the exit code before the trap was hit
# and returns that instead. So if our tests fails, our CI system will still pick it up!

function defer {
	docker-compose -f $SCRIPT_DIR/../../example-configs/cassandra-standalone/docker-compose.yml down
}

trap defer EXIT

docker-compose -f $SCRIPT_DIR/../../example-configs/cassandra-standalone/docker-compose.yml up

#cargo run -- --topology-file $SCRIPT_DIR/../../example-configs/redis-cluster/topology.yaml --config-file $SCRIPT_DIR/../../config/config.yaml


