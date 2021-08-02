#!/usr/bin/env bash

set -e

SCRIPT_DIR="$(cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

echo "$SCRIPT_DIR"

docker run --rm -it -v "$(pwd)":/home/rust/src -v "$(realpath ~/.cargo/git)":/home/rust/.cargo/git -v "$(realpath ~/.cargo/registry/)":/home/rust/.cargo/registry ekidd/rust-musl-builder:nightly-2020-08-15 bash -c "sudo chown -R rust:rust .; cargo build --release"

cp "$SCRIPT_DIR"/../target/x86_64-unknown-linux-musl/release/shotover-proxy "$SCRIPT_DIR"/static

docker build -f "$SCRIPT_DIR"/static/Dockerfile -t shotover/shotover-proxy-dev "$SCRIPT_DIR"/static

rm "$SCRIPT_DIR"/static/shotover-proxy