# Contributing to Shotover

This guide contains tips and tricks for working on Shotover Proxy itself. See [transform-development](transform-development.md) for details on writing your own transforms.

## Building Shotover

Shotover is written in Rust, so make sure you have a rust toolchain installed. See [the rustup site](https://rustup.rs/) for a quick way to setup your Rust development environment.

Once you've installed Rust via Rustup (you should just be fine with the latest stable). You will need to install a few other tools needed to compile some of Shotover's dependencies.

Shotover requires the following in order to build:

* cmake
* gcc
* g++
* libssl-dev
* pkg-config (Linux)

On ubuntu you can install them via `sudo apt-get install cmake gcc g++ libssl-dev pkg-config`

While not required for building Shotover, installing `docker` and `docker-compose` will allow you to run Shotover's integration tests and also build the static libc version of Shotover.

Some tests will require `libpcap-dev` to be installed as well (reading pcap files for protocol tests).

Now you can build Shotover by running `cargo build`. The executable will then be found in `target/debug/shotover-proxy`.

## Building Shotover (release)

The way you build Shotover will dramatically impact performance. To build Shotover for deployment in production environments, for maximum performance or for any benchmarking use `cargo build --release`. The resulting executable will be found in `target/release/shotover-proxy`.

## Functionally testing Shotover

To setup Shotover for functional testing perform the following steps:

1. Find an example in `examples/` that is closest to your use case.
    * If you don't know what you want, we suggest starting with `examples/redis-passthrough`.
2. Copy the `topology.yaml` file from that example to `config/topology.yaml`.
3. Do one of the following:
    * In the example directory you copied the `topology.yaml` from, run: `docker-compose -f docker-compose.yaml up`.
    * Modify `config/topology.yaml` to point to a service you have setup and want to use.
4. Run `cargo run`. Or `cargo run --release` to run with optimizations.
5. Connect to Shotover using the relevant client.
    * For example `examples/redis-passthrough` sets up Shotover as a simple redis proxy on the default redis port, so you can connect by just running `redis-cli`.

## Run Shotover tests

Run `cargo test`, refer to the [cargo test documentation](https://doc.rust-lang.org/cargo/commands/cargo-test.html) for more information.

## Submitting a PR

Before submitting a PR you can run the following to ensure it will pass CI:

* Run `cargo fmt`
* Run `cargo clippy` - Ensure you haven't introduced any warnings.
* Run `cargo build --all-targets` - Ensure everything still builds and you havent introduced any warnings.
* Run `cargo test` - All tests pass.
