#!/usr/bin/env bash
set -e

# Get the script dir no matter where we exec this script from
SCRIPT_DIR="$(cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

sudo ip addr add 127.0.0.2/32 dev lo || true
sudo ip addr add 127.0.0.3/32 dev lo || true

FAIL=0

echo "starting"

cargo run -- --topology-file $SCRIPT_DIR/../../example-configs/cassandra-cluster/topology1.yaml --config-file $SCRIPT_DIR/../../example-configs/cassandra-cluster/config1.yaml &
cargo run -- --topology-file $SCRIPT_DIR/../../example-configs/cassandra-cluster/topology2.yaml --config-file $SCRIPT_DIR/../../example-configs/cassandra-cluster/config2.yaml &
cargo run -- --topology-file $SCRIPT_DIR/../../example-configs/cassandra-cluster/topology3.yaml --config-file $SCRIPT_DIR/../../example-configs/cassandra-cluster/config3.yaml &

for job in `jobs -p`
do
echo $job
    wait $job || let "FAIL+=1"
done

echo $FAIL

if [ "$FAIL" == "0" ];
then
echo "YAY!"
else
echo "FAIL! ($FAIL)"
fi
