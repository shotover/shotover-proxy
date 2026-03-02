use crate::shotover_process;
use serde_json::Value;
use test_helpers::shotover_process::{EventMatcher, Level};
use tokio::net::TcpStream;

#[tokio::test]
async fn test_request_id_increments() {
    // Ensure it isnt reliant on timing
    let shotover_process = shotover_process("tests/test-configs/null-valkey/topology.yaml")
        .start()
        .await;
    for _ in 0..1000 {
        TcpStream::connect("127.0.0.1:6379").await.unwrap();
    }

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
