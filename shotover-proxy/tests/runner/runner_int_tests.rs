use serial_test::serial;
use test_helpers::shotover_process::{
    shotover_from_topology_file, shotover_from_topology_file_fail_to_startup, Count, EventMatcher,
    Level,
};

#[tokio::test]
#[serial]
async fn test_early_shutdown_cassandra_source() {
    shotover_from_topology_file("example-configs/null-cassandra/topology.yaml")
        .await
        .shutdown_and_then_consume_events(&[])
        .await;
}

#[tokio::test]
#[serial]
async fn test_shotover_responds_sigterm() {
    // Ensure it isnt reliant on timing
    for _ in 0..1000 {
        let shotover_process =
            shotover_from_topology_file("example-configs/null-redis/topology.yaml").await;
        shotover_process.signal(nix::sys::signal::Signal::SIGTERM);

        let events = shotover_process.consume_remaining_events(&[]).await;
        events.contains(
            &EventMatcher::new()
                .with_level(Level::Info)
                .with_target("shotover_proxy::runner")
                .with_message("received SIGTERM"),
        );
    }
}

#[tokio::test]
#[serial]
async fn test_shotover_responds_sigint() {
    let shotover_process =
        shotover_from_topology_file("example-configs/null-redis/topology.yaml").await;
    shotover_process.signal(nix::sys::signal::Signal::SIGINT);

    let events = shotover_process.consume_remaining_events(&[]).await;
    events.contains(
        &EventMatcher::new()
            .with_level(Level::Info)
            .with_target("shotover_proxy::runner")
            .with_message("received SIGINT"),
    );
}

#[tokio::test]
#[serial]
async fn test_shotover_shutdown_when_invalid_topology_non_terminating_last() {
    shotover_from_topology_file_fail_to_startup(
        "tests/test-configs/invalid_non_terminating_last.yaml",
        &[],
    )
    .await;
}

#[tokio::test]
#[serial]
async fn test_shotover_shutdown_when_invalid_topology_terminating_not_last() {
    shotover_from_topology_file_fail_to_startup(
        "tests/test-configs/invalid_terminating_not_last.yaml",
        &[],
    )
    .await;
}

#[tokio::test]
#[serial]
async fn test_shotover_shutdown_when_topology_invalid_topology_subchains() {
    shotover_from_topology_file_fail_to_startup(
        "tests/test-configs/invalid_subchains.yaml",
        &[
            EventMatcher::new().with_level(Level::Error)
                .with_target("shotover_proxy::runner")
                .with_message(r#"Topology errors
a_first_chain:
  Terminating transform "NullSink" is not last in chain. Terminating transform must be last in chain.
  Terminating transform "NullSink" is not last in chain. Terminating transform must be last in chain.
  Non-terminating transform "DebugPrinter" is last in chain. Last transform must be terminating.
b_second_chain:
  ConsistentScatter:
    a_chain_1:
      Terminating transform "NullSink" is not last in chain. Terminating transform must be last in chain.
      Non-terminating transform "DebugPrinter" is last in chain. Last transform must be terminating.
    b_chain_2:
      Terminating transform "NullSink" is not last in chain. Terminating transform must be last in chain.
    c_chain_3:
      ConsistentScatter:
        sub_chain_2:
          Terminating transform "NullSink" is not last in chain. Terminating transform must be last in chain.
"#),
            EventMatcher::new().with_level(Level::Warn)
                .with_target("shotover_proxy::transforms::distributed::consistent_scatter")
                .with_message("Using this transform is considered unstable - Does not work with REDIS pipelines")
                .with_count(Count::Times(2)),
            // TODO: Investigate these
            EventMatcher::new().with_level(Level::Error)
                .with_message("failed response Couldn't send message to wrapped chain SendError(BufferedChainMessages { local_addr: 127.0.0.1:10000, messages: [], flush: true, return_chan: Some(Sender { inner: Some(Inner { state: State { is_complete: false, is_closed: false, is_rx_task_set: false, is_tx_task_set: false } }) }) })")
                .with_count(Count::Any),
            EventMatcher::new().with_level(Level::Error)
                .with_target("shotover_proxy::transforms::distributed::consistent_scatter")
                .with_message("failed response channel closed")
                .with_count(Count::Any),
        ],
    )
    .await;
}
