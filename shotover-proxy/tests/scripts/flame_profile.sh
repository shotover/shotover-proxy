#!/usr/bin/env bash

SCRIPT_DIR="$(cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# Create a defered function to cleanup docker-compose. This also preserves the exit code before the trap was hit
# and returns that instead. So if our tests fails, our CI system will still pick it up!

# Run this if the flamegraph doesn't contain kernal function info (e.g. a stack of unknowns).
#echo 0 | sudo tee /proc/sys/kernel/kptr_restrict
#echo -1 | sudo tee /proc/sys/kernel/perf_event_paranoid


docker-compose -f $SCRIPT_DIR/../../example-configs/redis-cluster/docker-compose.yml up -d
echo "Getting ready to run proxy"
sleep 5
echo "Running shotover"

cd $SCRIPT_DIR/../../
timeout 30 cargo flamegraph --bin shotover-proxy -- --topology-file example-configs/redis-cluster/topology.yaml --config-file config/config.yaml &
sleep 10
timeout 20 ~/Downloads/redis-5.0.8/src/redis-benchmark -t set,get -l -q

docker-compose -f $SCRIPT_DIR/../../example-configs/redis-cluster/docker-compose.yml down
