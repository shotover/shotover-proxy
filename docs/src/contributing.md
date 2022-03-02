# Contributing to Shotover

This guide contains tips and tricks for working on Shotover Proxy itself.

## Configuring the Environment

Shotover is written in Rust, so make sure you have a rust toolchain installed. See [the rustup site](https://rustup.rs/) for a quick way to setup your Rust development environment.

Once you've installed Rust via Rustup (you should just be fine with the latest stable). You will need to install a few other tools needed to compile some of Shotover's dependencies.

Shotover requires the following in order to build:

* gcc
* g++
* libssl-dev
* pkg-config (Linux)

On Ubuntu you can install them via `sudo apt-get install cmake gcc g++ libssl-dev pkg-config`.

### Installing Optional Tools and Libraries

#### Docker

While not required for building Shotover, installing `docker` and `docker-compose` will allow you to run Shotover's integration tests and also build the static libc version of Shotover.

#### libpcap-dev

Some tests will require `libpcap-dev` to be installed as well (reading pcap files for protocol tests).

## Building Shotover

Now you can build Shotover by running `cargo build`. The executable will then be found in `target/debug/shotover-proxy`.

### Building Shotover (release)

The way you build Shotover will dramatically impact performance. To build Shotover for deployment in production environments, for maximum performance or for any benchmarking use `cargo build --release`. The resulting executable will be found in `target/release/shotover-proxy`.

## Running the Tests

The Cassandra tests require the Cassandra CPP driver.

### Installing Cassandra CPP Driver

Installation information and dependencies for the Cassandra CPP driver can be found [here](https://docs.datastax.com/en/developer/cpp-driver/2.4/).

#### Ubuntu 18.04

These instructions are for Ubuntu 18.04, other platform installations will be similar.

1. Download the driver packages and the libuv dependency.

```bash
wget https://downloads.datastax.com/cpp-driver/ubuntu/18.04/cassandra/v2.16.0/cassandra-cpp-driver_2.16.0-1_amd64.deb &
wget https://downloads.datastax.com/cpp-driver/ubuntu/18.04/cassandra/v2.16.0/cassandra-cpp-driver-dev_2.16.0-1_amd64.deb &
wget https://downloads.datastax.com/cpp-driver/ubuntu/18.04/dependencies/libuv/v1.35.0/libuv1_1.35.0-1_amd64.deb &
wget https://downloads.datastax.com/cpp-driver/ubuntu/18.04/dependencies/libuv/v1.35.0/libuv1-dev_1.35.0-1_amd64.deb &
wait
```

2. Install them using the `apt` tool

```bash
sudo apt -y install ./cassandra-cpp-driver_2.16.0-1_amd64.deb ./cassandra-cpp-driver-dev_2.16.0-1_amd64.deb ./libuv1_1.35.0-1_amd64.deb ./libuv1-dev_1.35.0-1_amd64.deb
```

### Functionally Testing Shotover

To setup Shotover for functional testing perform the following steps:

1. Find an example in `example-configs/` that is closest to your use case.
    * If you don't know what you want, we suggest starting with `example-configs/redis-passthrough`.
2. Copy the `topology.yaml` file from that example to `config/topology.yaml`.
3. Do one of the following:
    * In the example directory you copied the `topology.yaml` from, run: `docker-compose -f docker-compose.yaml up`.
    * Modify `config/topology.yaml` to point to a service you have setup and want to use.
4. Run `cargo run`. Or `cargo run --release` to run with optimizations.
5. Connect to Shotover using the relevant client.
    * For example `example-configs/redis-passthrough` sets up Shotover as a simple redis proxy on the default redis port, so you can connect by just running `redis-cli`.

### Run Shotover tests

Run `cargo test`, refer to the [cargo test documentation](https://doc.rust-lang.org/cargo/commands/cargo-test.html) for more information.

## Submitting a PR

Before submitting a PR you can run the following to ensure it will pass CI:

* Run `cargo fmt`
* Run `cargo clippy` - Ensure you haven't introduced any warnings.
* Run `cargo build --all-targets` - Ensure everything still builds and you haven't introduced any warnings.
* Run `cargo test` - All tests pass.
