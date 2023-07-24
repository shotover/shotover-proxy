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
This setup script might work for you: `curl -sSL https://get.docker.com/ | sudo sh`
Do not use the rootless install as many of our tests rely on the user created bridge networks having their interface exposed to the host, which rootless install does not support.
Instead add your user to the docker group: `usermod -aG docker $USER`

## Building Shotover

Now you can build Shotover by running `cargo build`. The executable will then be found in `target/debug/shotover-proxy`.

### Building Shotover (release)

The way you build Shotover will dramatically impact performance. To build Shotover for deployment in production environments, for maximum performance or for any benchmarking use `cargo build --release`. The resulting executable will be found in `target/release/shotover-proxy`.

## Running the Tests

The Cassandra tests require the Cassandra CPP driver.

### Installing Cassandra CPP Driver

Upstream installation information and dependencies for the Cassandra CPP driver can be found [here](https://docs.datastax.com/en/developer/cpp-driver/2.16/).

However that is likely unusable because datastax do not ship packages for modern ubuntu so we have our own script which will compile, package and install the driver on a modern ubuntu.
So to install the driver on ubuntu just run the script at `shotover-proxy/build/install_ubuntu_packages.sh`.

### Run Shotover tests

Run `cargo test`, refer to the [cargo test documentation](https://doc.rust-lang.org/cargo/commands/cargo-test.html) for more information.

The tests rely on configuration in `tests/test-configs/`, so if for example, you wanted to manually setup the services for the redis-passthrough test, you could run these commands in the `shotover-proxy` directory:

* `docker-compose -f shotover-proxy/tests/test-configs/redis-passthrough/docker-compose.yaml up`
* `cargo run -- --topology-file tests/test-configs/redis-passthrough/topology.yaml`

## Submitting a PR

Before submitting a PR you can run the following to ensure it will pass CI:

* Run `cargo fmt`
* Run `cargo clippy` - Ensure you haven't introduced any warnings.
* Run `cargo build --all-targets` - Ensure everything still builds and you haven't introduced any warnings.
* Run `cargo test` - All tests pass.
