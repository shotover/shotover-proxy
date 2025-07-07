use crate::shotover_process;
#[tokio::test]
async fn test_hotreload_cli_flag_accepted() {
    let shotover_process = shotover_process("tests/test-configs/null-valkey/topology.yaml")
        .with_hotreload(true) // Use the new method we'll create
        .with_config("tests/test-configs/shotover-config/config_metrics_disabled.yaml")
        .start()
        .await;
    shotover_process.shutdown_and_then_consume_events(&[]).await;
}
