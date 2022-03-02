#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# Create a defered function to cleanup docker-compose. This also preserves the exit code before the trap was hit
# and returns that instead. So if our tests fails, our CI system will still pick it up!

function defer {
	docker-compose -f $SCRIPT_DIR/../../example-configs/redis-cluster/docker-compose.yml down
}

trap defer EXIT

docker-compose -f $SCRIPT_DIR/../../example-configs/redis-cluster/docker-compose.yml up -d
echo "Getting ready to run proxy"
sleep 5
echo "Running shotover"

/usr/bin/time -v -o $SCRIPT_DIR/../../tests/soaktest-`date +%s` $SCRIPT_DIR/../../target/release/shotover-proxy --config-file $SCRIPT_DIR/../../config/config.yaml --topology-file $SCRIPT_DIR/../../example-configs/redis-cluster/topology.yaml




# ./src/redis-benchmark -t set,get,inc,lpush,rpush,lpop,rpop,sadd,hset,spop,lpush,lrange_100,lrange_600 -P 10
