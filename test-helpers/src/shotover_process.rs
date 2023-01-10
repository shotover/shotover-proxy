use std::time::Duration;

pub use tokio_bin_process::event::{Event, Level};
pub use tokio_bin_process::event_matcher::{Count, EventMatcher, Events};
pub use tokio_bin_process::BinProcess;

pub async fn shotover_from_topology_file(topology_path: &str) -> BinProcess {
    let mut shotover = BinProcess::start_with_args(
        "shotover-proxy",
        &["-t", topology_path, "--log-format", "json"],
    )
    .await;

    tokio::time::timeout(
        Duration::from_secs(30),
        shotover.wait_for(
            &EventMatcher::new()
                .with_level(Level::Info)
                .with_target("shotover_proxy::server")
                .with_message("accepting inbound connections"),
        ),
    )
    .await
    .unwrap();

    shotover
}

pub async fn shotover_from_topology_file_fail_to_startup(
    topology_path: &str,
    expected_errors_and_warnings: &[EventMatcher],
) -> Events {
    BinProcess::start_with_args(
        "shotover-proxy",
        &["-t", topology_path, "--log-format", "json"],
    )
    .await
    .consume_remaining_events_expect_failure(expected_errors_and_warnings)
    .await
}
