use crate::shotover_process;
use serde_json::Value;
use std::time::Duration;
use test_helpers::metrics::get_metrics_value;
use test_helpers::shotover_process::{EventMatcher, Level};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{sleep, timeout};

#[tokio::test]
async fn test_request_id_increments() {
    // Ensure it isnt reliant on timing
    let shotover_process = shotover_process("tests/test-configs/null-valkey/topology.yaml")
        .start()
        .await;
    for _ in 0..1000 {
        TcpStream::connect("127.0.0.1:6379").await.unwrap();
    }

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let events = shotover_process.shutdown_and_then_consume_events(&[]).await;
    let mut previous_id = 0;
    for event in events.events {
        for span in event.spans {
            if let Some(name) = span.get("name") {
                if *name == Value::String("connection".into()) {
                    if let Some(id) = span.get("id").and_then(|x| x.as_i64()) {
                        // ensure that the ID increases by 1 and monotonically
                        assert!(previous_id == id || previous_id + 1 == id);
                        previous_id = id;
                    }
                }
            }
        }
    }
    // ensure that this test does something
    assert_eq!(previous_id, 1000);
}

#[tokio::test]
#[cfg(feature = "cassandra")]
async fn test_early_shutdown_cassandra_source() {
    shotover_process("tests/test-configs/null-cassandra/topology.yaml")
        .start()
        .await
        .shutdown_and_then_consume_events(&[])
        .await;
}

#[tokio::test]
async fn test_shotover_responds_sigterm() {
    // Ensure it isnt reliant on timing
    for _ in 0..1000 {
        let shotover_process = shotover_process("tests/test-configs/null-valkey/topology.yaml")
            .start()
            .await;
        shotover_process.send_sigterm();

        let events = shotover_process.consume_remaining_events(&[]).await;
        events.assert_contains(
            &EventMatcher::new()
                .with_level(Level::Info)
                .with_target("shotover::runner")
                .with_message("received SIGTERM"),
        );
    }
}

#[tokio::test]
async fn test_shotover_responds_sigint() {
    let shotover_process = shotover_process("tests/test-configs/null-valkey/topology.yaml")
        .start()
        .await;
    shotover_process.send_sigint();

    let events = shotover_process.consume_remaining_events(&[]).await;
    events.assert_contains(
        &EventMatcher::new()
            .with_level(Level::Info)
            .with_target("shotover::runner")
            .with_message("received SIGINT"),
    );
}

#[tokio::test]
async fn test_shotover_shutdown_when_invalid_topology_non_terminating_last() {
    shotover_process(
        "tests/test-configs/invalid_non_terminating_last.yaml",
    )
    .assert_fails_to_start(&[EventMatcher::new()
        .with_level(Level::Error)
        .with_target("shotover::runner")
        .with_message("Failed to start shotover

Caused by:
    Topology errors
    valkey source:
      valkey chain:
        Non-terminating transform \"DebugPrinter\" is last in chain. Last transform must be terminating.
    ")])
    .await;
}

#[tokio::test]
async fn test_shotover_shutdown_when_invalid_topology_terminating_not_last() {
    shotover_process(
        "tests/test-configs/invalid_terminating_not_last.yaml",
    )
    .assert_fails_to_start(&[EventMatcher::new()
        .with_level(Level::Error)
        .with_target("shotover::runner")
        .with_message("Failed to start shotover

Caused by:
    Topology errors
    valkey source:
      valkey chain:
        Terminating transform \"NullSink\" is not last in chain. Terminating transform must be last in chain.
    ")])
    .await;
}

#[tokio::test]
async fn test_shotover_startup_fails_when_source_port_is_already_bound() {
    let blocked_port_listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    shotover_process("tests/test-configs/null-valkey/topology.yaml")
        .assert_fails_to_start(&[EventMatcher::new()
            .with_level(Level::Error)
            .with_target("shotover::runner")
            // match on the bound address and avoid OS-specific error codes.
            .with_message_regex(r"(?s)Failed to start shotover.*address=127\.0\.0\.1:6379")])
        .await;

    drop(blocked_port_listener);

    let shotover = shotover_process("tests/test-configs/null-valkey/topology.yaml")
        .start()
        .await;
    TcpStream::connect("127.0.0.1:6379").await.unwrap();
    shotover.shutdown_and_then_consume_events(&[]).await;
}

#[tokio::test]
async fn test_shotover_runtime_listener_create_failure_increments_metric() {
    const LISTENER_CREATE_FAILURES_METRIC_KEY: &str =
        r#"shotover_listener_create_failures_count{source="valkey"}"#;
    // Uses hard_connection_limit=1 so the listener is dropped and recreated at runtime.
    let topology = "tests/test-configs/null-valkey/topology-hard-connection-limit.yaml";

    let shotover = shotover_process(topology).start().await;
    let baseline_failures = get_metrics_value(LISTENER_CREATE_FAILURES_METRIC_KEY)
        .await
        .parse::<u64>()
        .unwrap_or_else(|error| {
            panic!("Failed to parse listener create failures baseline metric value: {error:?}")
        });

    // Consume the only permit; this causes SourceTask to temporarily drop its listener.
    let held_connection = TcpStream::connect("127.0.0.1:6379").await.unwrap();
    // Grab the port so Shotover's listener recreation attempt must fail.
    let blocked_runtime_listener = timeout(Duration::from_secs(10), async {
        loop {
            match TcpListener::bind("127.0.0.1:6379").await {
                Ok(listener) => return listener,
                Err(_) => sleep(Duration::from_millis(50)).await,
            }
        }
    })
    .await
    .expect("Timed out waiting for source listener to close under hard connection limit");

    // Release the held client connection so Shotover attempts listener recreation.
    drop(held_connection);

    // Wait until at least one additional listener creation failure is observed.
    timeout(Duration::from_secs(10), async {
        loop {
            let metric_value = get_metrics_value(LISTENER_CREATE_FAILURES_METRIC_KEY)
                .await
                .parse::<u64>()
                .unwrap_or_else(|error| {
                    panic!("Failed to parse listener create failures metric value: {error:?}")
                });
            if metric_value > baseline_failures {
                break;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("Timed out waiting for listener create failures metric to increase");

    // Unblock the port and ensure listener recreation eventually succeeds.
    drop(blocked_runtime_listener);
    timeout(Duration::from_secs(10), async {
        loop {
            match TcpStream::connect("127.0.0.1:6379").await {
                Ok(connection) => {
                    drop(connection);
                    break;
                }
                Err(_) => sleep(Duration::from_millis(50)).await,
            }
        }
    })
    .await
    .expect("Timed out waiting for listener to recover and accept connections");

    // The runtime failure path logs a warning; allow it explicitly in this test.
    shotover
        .shutdown_and_then_consume_events(&[EventMatcher::new()
            .with_level(Level::Warn)
            .with_target("shotover::source_task")
            .with_message_regex(
                r"\[valkey\] Failed to create listener on \[127\.0\.0\.1:6379\].*address=127\.0\.0\.1:6379",
            )])
        .await;
}

#[tokio::test]
async fn test_shotover_shutdown_when_topology_invalid_topology_subchains() {
    shotover_process(
        "tests/test-configs/invalid_subchains.yaml",
    ).assert_fails_to_start(
        &[
            EventMatcher::new().with_level(Level::Error)
                .with_target("shotover::runner")
                .with_message(r#"Failed to start shotover

Caused by:
    Topology errors
    valkey1 source:
      valkey1 chain:
        Terminating transform "NullSink" is not last in chain. Terminating transform must be last in chain.
        Terminating transform "NullSink" is not last in chain. Terminating transform must be last in chain.
        Non-terminating transform "DebugPrinter" is last in chain. Last transform must be terminating.
    valkey2 source:
      valkey2 chain:
        ParallelMap:
          parallel_map_chain chain:
            Terminating transform "NullSink" is not last in chain. Terminating transform must be last in chain.
            Non-terminating transform "DebugPrinter" is last in chain. Last transform must be terminating.
    "#),
        ],
    )
    .await;
}

#[tokio::test]
#[cfg(all(feature = "valkey", feature = "cassandra"))]
async fn test_shotover_shutdown_when_protocol_mismatch() {
    shotover_process("tests/test-configs/invalid_protocol_mismatch.yaml")
        .assert_fails_to_start(&[EventMatcher::new()
            .with_level(Level::Error)
            .with_target("shotover::runner")
            .with_message(
                r#"Failed to start shotover

Caused by:
    Topology errors
    Transform ValkeySinkSingle requires upchain protocol to be one of [Valkey] but was Cassandra
    "#,
            )])
        .await;
}
