#!/usr/bin/env bash

set -e

SCRIPT_DIR="$(cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

cd $SCRIPT_DIR

rustup toolchain install
cargo build --release

mkdir -p shotover
cp ../../target/release/shotover-proxy shotover
cp -r ../config shotover

# extract the crate version from Cargo.toml
CRATE_VERSION="$(cargo metadata --format-version 1 --offline --no-deps | jq -c -M -r '.packages[] | select(.name == "shotover-proxy") | .version')"
tar -cvzf out.tar.gz shotover
mv out.tar.gz ../../shotover-proxy-linux_amd64-${CRATE_VERSION}.tar.gz

rm -rf shotover
