#!/bin/sh

# Creates an ubuntu docker image and installs dependencies needed to run shotover.
# Pulls the latest shotover changes from the specified branch and runs the specified test.
# Only a few tests can be run in this way, any test that itself spins up docker containers will not work as docker cannot run in docker.
#
# Usage:
#   ./run-test-in-docker.sh branch-name test-name
#
# Notes:
#   Manually delete the shotover-test container to recreate it from scratch:
#   docker container stop shotover-test && docker container rm shotover-test
#
# Config:
#    Change REPO value to change the repo to clone
REPO=https://github.com/ric-pro/shotover-proxy

if docker inspect "shotover-test" > /dev/null 2>&1; then
	echo "Container already exists, skipping creation"
else
	docker run -d --name shotover-test ubuntu:22.04 sleep infinity
	docker exec shotover-test bash -c "apt-get update"
	docker exec shotover-test bash -c "apt-get install curl git gcc-aarch64-linux-gnu libssl-dev cmake pkg-config g++ -y"
	docker exec shotover-test bash -c "curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y"
	docker exec shotover-test bash -c "curl -LsSf https://get.nexte.st/latest/linux-arm | tar zxf - -C \${CARGO_HOME:-~/.cargo}/bin"
	docker exec shotover-test bash -c "git clone $REPO"
fi

docker exec shotover-test bash -c "cd shotover-proxy && git fetch && git checkout origin/$1"
docker exec shotover-test bash -c "cd shotover-proxy && source \"\$HOME/.cargo/env\" && cargo nextest run $2"
