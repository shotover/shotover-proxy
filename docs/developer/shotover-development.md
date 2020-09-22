# Shotover Development Guide

This guide contains tips and tricks for working on shotover-proxy itself. 
See [transform-development](transform-development.md) for details on writing your own transforms.

## Building shotover
Shotover is written in Rust, so make sure you have a rust toolchain installed. See [the rustup site](https://rustup.rs/) for a quick way to setup your
rust development environment.

Once you've installed rust via Rustup (you should just be fine with the latest stable). You will need to install a few other tools
needed to compile some of shotover's dependencies. 

Shotover requires the following in order to build:

* cmake
* gcc
* g++
* libssl-dev
* pkg-config (Linux)

On ubuntu you can install them via `sudo apt-get install cmake gcc g++ libssl-dev pkg-config`

While not required for building shotover, installing `docker` will allow you to run shotover's integration tests and also build
the static libc version of shotover. 

Now you can build shotover by running `cargo build`. The executable will then be found in `target/debug/shotover-proxy`.

## Building shotover (release)
The way you build shotover will dramatically impact performance. To build shotover for deployment in production environments, for maximum performance 
or for any benchmarking use `cargo build --release`. The resulting executeable will be found in `target/release/shotover-proxy`. 

## Testing shotover
See cargo's [documentation suite](https://doc.rust-lang.org/cargo/commands/cargo-test.html) for running tests. If you want to run the entire test suite,
you will want to restrict the number of tests that can execute at any given period of time. Some of the integration tests depend on 
external databases running in a docker environment and there are still some overlaps in port numbers etc. This means tests can only run one at a time.

To run the full set of shotover tests, use `cargo test -- --test-threads 1`

