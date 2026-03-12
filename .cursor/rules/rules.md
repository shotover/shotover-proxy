---
description: Shotover development guidelines for this repo
alwaysApply: true
---

# Shotover Development Guidelines

Shotover is a high performance, configurable and extensible L7 data-layer proxy written in rust.
It supports Cassandra, Kafka, and Valkey (experimental opensearch support)
Shotover is configured by a topology.yaml file which sets up a Source which listens for incoming connections, and a series of transforms which inspect and alter requests before finally sending them off to the destination via a Sink Transform.
Responses are processed in reverse.

## Project layout

The project is a cargo workspace consisting of:

* shotover
  + contains the full implementation of shotover, exposed as a library
  + contains all the unittests for shotover
* shotover-proxy
  + a tiny crate, that uses the `shotover` crate to make a shotover binary, this is what gets shipped as Shotover.
  + contains all the integration tests for shotover
* custom-transforms-example
  + a small example demonstrating adding a custom transform to shotover via the `shotover` crate.
* ec2-cargo
  + a tool for spinning up EC2 instances to allow us to test linux-only tests from our mac os laptops.
* website
  + a static site generator for the https://shotover.io website, includes promotional material and generates the includes the docs for past and present versions of shotover.

## Config Deserialization

Config deserialization (topology.yaml and config.yaml) is done via the serde crate.
It is required that all types implement `serde::Deserialize` via `#[derive(Serialize)]` instead of manually implementing the trait.
This ensures that deserialization is robust and that the types map directly to whats in the yaml.

## Verifying your work

* Any time you make a change to a rust file run the following before giving control back to the user
  + cargo clippy --all-targets --features alpha-transforms
    + to resolve clippy failures run `cargo clippy --all-targets --features alpha-transforms --fix`, then manually fix any issues not covered by `--fix`
  + cargo fmt --all
* If you need to, run specific tests via `cargo nextest run $TEST_NAME`, never `cargo test $TEST_NAME`
  + running the full test suite will take too long, so always run specific tests instead.
  + note that users will be running mac os where many integration tests dont work, unittests will always work fine however.