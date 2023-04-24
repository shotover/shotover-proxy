use serial_test::serial;
use test_helpers::shotover_process::{Count, EventMatcher, Level, ShotoverProcessBuilder};

#[tokio::test]
#[serial]
async fn test_early_shutdown_cassandra_source() {
    ShotoverProcessBuilder::new_with_topology("tests/test-configs/null-cassandra/topology.yaml")
        .start()
        .await
        .shutdown_and_then_consume_events(&[])
        .await;
}

#[tokio::test]
#[serial]
async fn test_shotover_responds_sigterm() {
    // Ensure it isnt reliant on timing
    for _ in 0..1000 {
        let shotover_process = ShotoverProcessBuilder::new_with_topology(
            "tests/test-configs/null-redis/topology.yaml",
        )
        .start()
        .await;
        shotover_process.signal(nix::sys::signal::Signal::SIGTERM);

        let events = shotover_process.consume_remaining_events(&[]).await;
        events.contains(
            &EventMatcher::new()
                .with_level(Level::Info)
                .with_target("shotover::runner")
                .with_message("received SIGTERM"),
        );
    }
}

#[tokio::test]
#[serial]
async fn test_shotover_responds_sigint() {
    let shotover_process =
        ShotoverProcessBuilder::new_with_topology("tests/test-configs/null-redis/topology.yaml")
            .start()
            .await;
    shotover_process.signal(nix::sys::signal::Signal::SIGINT);

    let events = shotover_process.consume_remaining_events(&[]).await;
    events.contains(
        &EventMatcher::new()
            .with_level(Level::Info)
            .with_target("shotover::runner")
            .with_message("received SIGINT"),
    );
}

#[tokio::test]
#[serial]
async fn test_shotover_shutdown_when_invalid_topology_non_terminating_last() {
    ShotoverProcessBuilder::new_with_topology(
        "tests/test-configs/invalid_non_terminating_last.yaml",
    )
    .assert_fails_to_start(&[EventMatcher::new()
        .with_level(Level::Error)
        .with_target("shotover::runner")
        .with_message("Failed to start shotover

Caused by:
    Topology errors
    redis_chain:
      Non-terminating transform \"DebugPrinter\" is last in chain. Last transform must be terminating.
    ")])
    .await;
}

#[tokio::test]
#[serial]
async fn test_shotover_shutdown_when_invalid_topology_terminating_not_last() {
    ShotoverProcessBuilder::new_with_topology(
        "tests/test-configs/invalid_terminating_not_last.yaml",
    )
    .assert_fails_to_start(&[EventMatcher::new()
        .with_level(Level::Error)
        .with_target("shotover::runner")
        .with_message("Failed to start shotover

Caused by:
    Topology errors
    redis_chain:
      Terminating transform \"NullSink\" is not last in chain. Terminating transform must be last in chain.
    ")])
    .await;
}

#[tokio::test]
#[serial]
async fn test_shotover_shutdown_when_topology_invalid_topology_subchains() {
    ShotoverProcessBuilder::new_with_topology(
        "tests/test-configs/invalid_subchains.yaml",
    ).assert_fails_to_start(
        &[
            EventMatcher::new().with_level(Level::Error)
                .with_target("shotover::runner")
                .with_message(r#"Failed to start shotover

Caused by:
    Topology errors
    a_first_chain:
      Terminating transform "NullSink" is not last in chain. Terminating transform must be last in chain.
      Terminating transform "NullSink" is not last in chain. Terminating transform must be last in chain.
      Non-terminating transform "DebugPrinter" is last in chain. Last transform must be terminating.
    b_second_chain:
      TuneableConsistencyScatter:
        a_chain_1:
          Terminating transform "NullSink" is not last in chain. Terminating transform must be last in chain.
          Non-terminating transform "DebugPrinter" is last in chain. Last transform must be terminating.
        b_chain_2:
          Terminating transform "NullSink" is not last in chain. Terminating transform must be last in chain.
        c_chain_3:
          TuneableConsistencyScatter:
            sub_chain_2:
              Terminating transform "NullSink" is not last in chain. Terminating transform must be last in chain.
    "#),
            EventMatcher::new().with_level(Level::Warn)
                .with_target("shotover::transforms::distributed::tuneable_consistency_scatter")
                .with_message("Using this transform is considered unstable - Does not work with REDIS pipelines")
                .with_count(Count::Times(2)),
            // TODO: Investigate these
            EventMatcher::new().with_level(Level::Error)
                .with_message("failed response Couldn't send message to wrapped chain SendError(BufferedChainMessages { local_addr: 127.0.0.1:10000, messages: [], flush: true, return_chan: Some(Sender { inner: Some(Inner { state: State { is_complete: false, is_closed: false, is_rx_task_set: false, is_tx_task_set: false } }) }) })")
                .with_count(Count::Any),
            EventMatcher::new().with_level(Level::Error)
                .with_target("shotover::transforms::distributed::tuneable_consistency_scatter")
                .with_message("failed response channel closed")
                .with_count(Count::Any),
        ],
    )
    .await;
}
