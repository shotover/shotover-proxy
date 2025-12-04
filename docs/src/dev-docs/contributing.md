# Contributing to Shotover

This guide describes how to setup and use your environment for contributing to shotover itself.

## Setup

Shotover requires [rustup](https://rustup.rs/) to be installed so that the project specific rust version will be used.

The rest of the setup is specific to your operating system.
Shotover supports development on linux and partially supports macOS.

### Setting up on Linux

On linux, all tests will pass as long as the required dependencies are installed.

See the [Linux specific setup instructions](setting-up-linux.md)

### Setting up on macOS

* All tests that use a single docker instance will pass. But some tests with more than one docker instance will fail.
* Tests that rely on external C++ dependencies cannot be built.
  * They are hidden behind the `cassandra-cpp-driver-tests` and `kafka-cpp-driver-tests` feature flags to allow the rest of the tests to build on macOS

Everything else should be buildable and pass.

See the [macOS specific setup instructions](setting-up-macos.md)

### Setting up via ec2-cargo

The shotover repo contains a tool `ec2-cargo` which makes it easy to setup and a linux EC2 instance for developing shotover.
This can be used by:

* Linux users to test on a fresh machine or to test on a different cpu architecture
* macOS users to run tests that do not run on macOS
* To enable development from any other OS

Refer to the [ec2-cargo docs](https://github.com/shotover/shotover-proxy/tree/main/ec2-cargo).

## Run Shotover tests

Shotover's test suite must be run via [nextest](https://nexte.st) as we rely on its configuration to avoid running incompatible integration tests concurrently.

To use nextest:

1. Install nextest: `cargo install cargo-nextest --locked`
2. Then run the tests: `cargo nextest run`

The tests rely on configuration in `tests/test-configs/`, so if for example, you wanted to manually setup the services for the valkey-passthrough test, you could run these commands in the `shotover-proxy` directory:

* `docker-compose -f shotover-proxy/tests/test-configs/valkey-passthrough/docker-compose.yaml up`
* `cargo run -- --topology-file tests/test-configs/valkey-passthrough/topology.yaml`

## Submitting a PR

Before submitting a PR you can run the following in preparation to make your PR more likely to pass CI:

* `cargo fmt --all` - Ensure your code follows standard rust formatting.
* `cargo clippy --all-features --all-targets` - Ensure you haven't introduced any warnings.
* `cargo nextest run --all-features --all-targets` - Ensure all tests pass.

For formatting you should configure your IDE to auto-format on save so that you never hit this issue at CI time.
For clippy you can setup your IDE or a tool like [bacon](https://github.com/Canop/bacon) to run clippy while you work.
If you find clippy too noisy, you should setup `cargo check` to run during development instead.
But you will have to eventually resolve the clippy warnings before the PR can be merged.

For the integration tests, CI will complete quicker than your local machine.
So its much more realistic to just:

1. Run some tests related to your changes
2. Submit your PR as a draft
3. See what fails
4. Fix the failures before marking as ready to review.

Also note that CI will run clippy against every permutation of features.
So check what its doing in `.github/workflows/lint.yaml` if you have a failure in CI that is not reproducing locally.

### Building Shotover (release)

To build a release binary of shotover run `cargo build --release -p shotover-proxy`:

* The built binary is located at `target/release/shotover-proxy`
* The `--release` is very important, never deploy a non-release binary as it will be far too slow.
* The `-p shotover-proxy` is optional but skips building artifacts only used during development.
Doing this is much faster to build and avoids extra external dependencies.
