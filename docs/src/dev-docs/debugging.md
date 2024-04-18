# Investigating shotover bugs

This document describes the general process I follow for debugging.
There is no need to strictly adhere to it, but maybe you will find something useful here.

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

## Run the test

Run the test by:

```shell
cargo nextest run --no-default-features --features kafka kafka_int_tests::passthrough_standard --nocapture
```

Broken down we have:

* `--no-default-features --features kafka` will compile only the parts of shotover and the integration tests required for the specified protocol.
  * This will drastically improve time to compile shotover and the tests, currently it drops from 15s to 5s. The difference will grow as shotover grows.
* `kafka_int_tests::passthrough_standard::case_1` the exact name of the test to run.
  * We could also specify just `kafka_int_tests::passthrough` to run every kafka passthrough integration test.
* `--nocapture` disable the test runner from capturing stdout, this way we get all output from stdout+stderr emitted by the test, undisturbed by the test runner. This includes shotover's tracing logs.
  * By default the test runner captures the output so it can avoid displaying output from a test that passed and is therefore uninteresting.

## Exploration

Sprinkle `tracing::info!("some log {}", some.variable)` around suspect areas in the codebase to narrow down whats going wrong.
Rerun the test each time to learn more about the behaviour of the system and narrow down your search.

Delete these extra logs when you are finished your investigation. Some of them could be downgraded to a `tracing::debug!()` and kept if they are found to be generally valuable.
