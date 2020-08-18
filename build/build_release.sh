#!/usr/bin/env bash

SCRIPT_DIR="$(cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

cd $SCRIPT_DIR/..
cargo clean
cargo build --release
mkdir -p shotover-proxy
cp target/release/shotover-proxy shotover-proxy
cp -r config shotover-proxy
tar -cvzf shotover-proxy-linux_amd64-"$(cargo metadata --format-version 1 --offline --no-deps | jq -c -M -r '.packages[0].version')".tar.gz shotover-proxy
rm -rf shotover-proxy