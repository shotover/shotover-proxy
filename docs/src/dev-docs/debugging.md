# Investigating shotover bugs

This document describes the general process I follow for debugging.
There is no need to strictly adhere to it, but maybe you will find something useful here.

## Ensure the project has no warnings

The rust compiler's warnings should always be addressed immediately as they can point out program breaking mistakes.
For example a missed `.await` will leave a future unresolved but is hard to notice from visual inspection.
I recommend ensuring `cargo check --all-targets` returns no errors at all times.
You can set features to reduce runtime e.g. `cargo check --all-targets --no-default-features --features kafka` but that will disable some dead code warnings (fine for development).
You can use a tool like [bacon](https://github.com/Canop/bacon) or [cargo watch](https://github.com/watchexec/cargo-watch) to keep track of warnings.

Note that CI will fail if your PR has any warnings anyway, so you'll have to address them all eventually.

## Failing test

Write or modify an integration test to reproduce the bug.
Integration tests live at `shotover-proxy/tests/`.

## Add DebugPrinter

Add `DebugPrinter` to the test's `topology.yaml`.
This will setup shotover to log every request and response that passes through.
For example:

```yaml
sources:
  - Kafka:
      name: "kafka"
      listen_addr: "127.0.0.1:9192"
      chain:
        - DebugPrinter # Add this line here!
        - KafkaSinkSingle:
            destination_port: 9092
            connect_timeout_ms: 3000
```

For a simple valkey request/response, the logs will look like:

```plain
shotover   06:37:14.712042Z  INFO connection{id=2 source="valkey"}: shotover::transforms::debug::printer: Request: Valkey Array([BulkString(b"GET"), BulkString(b"bar")])
shotover   06:37:14.712212Z  INFO connection{id=2 source="valkey"}: shotover::transforms::debug::printer: Response: Valkey BulkString(b"foo")
```

## Run the test

Run the test by:

```shell
cargo nextest run --no-default-features --features kafka kafka_int_tests::passthrough_standard::case_1_java --nocapture
```

Broken down we have:

* `--no-default-features --features kafka` will compile only the parts of shotover and the integration tests required for the specified protocol.
  * This will drastically improve time to compile shotover and the tests, currently it drops from 15s to 5s. The difference will grow as shotover grows.
  * For a full list of features refer to the `[features]` section in [shotover-proxy/Cargo.toml](https://github.com/shotover/shotover-proxy/blob/main/shotover-proxy/Cargo.toml)
* `kafka_int_tests::passthrough_standard::case_1_java` the exact name of the test to run.
  * We could also specify just `kafka_int_tests::passthrough` to run every test in `kafka_int_tests` that starts with `passthrough`. Refer to [the nextest docs](https://nexte.st/book/filter-expressions) for more advanced filtering.
* `--nocapture` disables the test runner from capturing (and hiding) the test and shotover's stdout+stderr.
  * By default the test runner captures the output so it can avoid displaying output from a test that passed and is therefore uninteresting.
  * Disabling capturing when debugging is useful since:
    * Even if the test passed we can still access the logs
    * We can see the test output in real time.
    * stdout and stderr remain intermingled instead of separated.

## Log levels

By default shotover and the integration tests will run at log level INFO. This means `INFO`, `WARN` and `ERROR` level logs will be emitted, while `DEBUG` and `TRACE` level logs will be dropped.
You can alter the log level used by a test run by running `RUST_LOG=DEBUG cargo nextest run ..`.

## Exploration

Examine the logs emitted by `DebugPrinter`.
From the perspective of a `DebugPrinter` in a simple chain that looks like `Source` -> `DebugPrinter` -> `Sink`:

* Requests are coming from the source codec.
* Responses are coming from the sink and into the sink from the sink's codec.

So determine whether to first investigate the source codec or sink by determining if the problem is occurring in the debug printer's requests or responses.

Sprinkle `tracing::info!("some log {}", some.variable)` around suspect areas in the codebase to narrow down whats going wrong.
Rerun the test each time to learn more about the behavior of the system and narrow down your search.

Delete these extra logs when you are finished your investigation. Some of them could be downgraded to a `tracing::debug!()` and kept if they are found to be generally valuable.
