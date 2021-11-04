#!/usr/bin/env bash

set -e

SCRIPT_DIR="$(cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

cd $SCRIPT_DIR/../..

cargo test --release
cargo build --release

# Make glibc version
mkdir -p shotover
cp target/release/shotover-proxy shotover
cp -r shotover-proxy/config shotover

# extract the crate version from Cargo.toml
CRATE_VERSION="$(cargo metadata --format-version 1 --offline --no-deps | jq -c -M -r '.packages[] | select(.name == "shotover-proxy") | .version')"
tar -cvzf shotover-proxy-linux_amd64-${CRATE_VERSION}.tar.gz shotover

rm -rf shotover


### Building musl with rust-musl-builder only works on older rust toolchain versions so this is commented out until we can reenable it.
# Make musl version
#$SCRIPT_DIR/build_static.sh
# mkdir -p shotover-proxy
# cp target/x86_64-unknown-linux-musl/release/shotover-proxy shotover-proxy
# cp -r config shotover-proxy
# tar -cvzf shotover-proxy-static_linux_amd64-"$(cargo metadata --format-version 1 --offline --no-deps | jq -c -M -r '.packages[0].version')".tar.gz shotover-proxy
# rm -rf shotover-proxy
