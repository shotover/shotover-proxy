use pretty_assertions::assert_eq;
use redis::Cmd;
use redis::aio::Connection;
use std::time::Duration;
use test_helpers::connection::valkey_connection;
use test_helpers::docker_compose::docker_compose;
use test_helpers::shotover_process::{
    BinProcess, BinProcessBuilder, EventMatcher, Level, bin_path,
};

#[tokio::test(flavor = "multi_thread")]
async fn test_custom_transform() {
    // Setup shotover and the valkey server it connects to
    let _compose = docker_compose("config/docker-compose.yaml");
    let shotover = shotover_proxy("config/topology.yaml").await;
    let mut connection = valkey_connection::new_async("127.0.0.1", 6379).await;

    // Verify functionality of transform
    assert_ok(
        redis::cmd("SET").arg("foo").arg("some value"),
        &mut connection,
    )
    .await;
    assert_bytes(
        redis::cmd("GET").arg("foo"),
        &mut connection,
        b"Rewritten value",
    )
    .await;
    assert_bytes(
        redis::cmd("GET").arg("bar"),
        &mut connection,
        b"Rewritten value",
    )
    .await;

    // Shutdown shotover asserting that it encountered no errors
    shotover.shutdown_and_then_consume_events(&[]).await;
}

async fn shotover_proxy(topology_path: &str) -> BinProcess {
    let mut shotover = BinProcessBuilder::from_path(bin_path!("custom-transforms-example"))
        .with_log_name(Some("shotover".to_owned()))
        .with_args(vec![
            "-t".to_owned(),
            topology_path.to_owned(),
            "--log-format".to_owned(),
            "json".to_owned(),
        ])
        .start()
        .await;

    tokio::time::timeout(
        Duration::from_secs(30),
        shotover.wait_for(
            &EventMatcher::new()
                .with_level(Level::Info)
                .with_message("Shotover is now accepting inbound connections"),
            &[],
        ),
    )
    .await
    .unwrap();
    shotover
}

pub async fn assert_ok(cmd: &mut Cmd, connection: &mut Connection) {
    assert_eq!(cmd.query_async(connection).await, Ok("OK".to_string()));
}

pub async fn assert_bytes(cmd: &mut Cmd, connection: &mut Connection, value: &[u8]) {
    assert_eq!(cmd.query_async(connection).await, Ok(value.to_vec()));
}
