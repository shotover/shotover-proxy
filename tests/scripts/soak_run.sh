#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# Create a defered function to cleanup docker-compose. This also preserves the exit code before the trap was hit
# and returns that instead. So if our tests fails, our CI system will still pick it up!

echo "Getting ready to run bench"
echo "Running redis benchmark"

/usr/bin/time -v -o $SCRIPT_DIR/../../tests/soaktest-bench-`date +%s` ~/Downloads/redis-5.0.8/src/redis-benchmark -t set,get,inc,lpush,rpush,lpop,rpop,sadd,hset,spop,lpush,lrange_100,lrange_600 -l -q -c 500