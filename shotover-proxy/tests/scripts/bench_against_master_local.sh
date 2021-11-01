#!/usr/bin/env bash

# Frontend script for bench_against_master.sh to run it on your local machine

echo "Assuming that origin is the upstream remote"

set -e; set -u

SCRIPT_DIR="$(cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

export LOG_PAGE="log is above"
export GITHUB_SERVER_URL=""
export GITHUB_REPOSITORY=""
export GITHUB_RUN_ID=""
export GITHUB_BASE_REF="main"

$SCRIPT_DIR/bench_against_master.sh 1
