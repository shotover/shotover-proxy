use crate::shotover_process;
use test_helpers::shotover_process::{EventMatcher, Level};
#[tokio::test]
async fn test_hotreload_cli_flag_works() {
    let shotover_process = shotover_process("tests/test-configs/null-valkey/topology.yaml")
        .with_args(&["--hotreload"])
        .with_config("tests/test-configs/shotover-config/config_metrics_disabled.yaml")
        .start()
        .await;
    // log messages
    let events = shotover_process
        .shutdown_and_then_consume_events(&[EventMatcher::new()
            .with_level(Level::Info)
            .with_target("shotover::runner")
            .with_message(
                "Hot reloading is ENABLED - shotover will support hot reload operations",
            )])
        .await;
    events.assert_contains(
        &EventMatcher::new()
            .with_level(Level::Info)
            .with_target("shotover::runner")
            .with_message("Hot reloading is ENABLED - shotover will support hot reload operations"),
    );
}
