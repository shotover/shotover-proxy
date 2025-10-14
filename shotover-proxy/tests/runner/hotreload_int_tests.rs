use crate::shotover_process;
use redis::{Client, Commands};
use test_helpers::docker_compose::docker_compose;

/// Helper function to verify comprehensive Valkey connection functionality
fn assert_valkey_connection_works(
    connection: &mut redis::Connection,
    expected_counter: Option<i32>,
    expected_data: &[(&str, &str)],
) -> Result<(), Box<dyn std::error::Error>> {
    // Test basic PING
    let pong: String = redis::cmd("PING").query(connection)?;
    assert_eq!(pong, "PONG");

    // Test data persistence for specific keys
    for (key, expected_value) in expected_data {
        let actual_value: String = connection.get(*key)?;
        assert_eq!(
            actual_value, *expected_value,
            "Key '{key}' should contain '{expected_value}' but contained '{actual_value}'"
        );
    }

    // Test basic SET/GET operation using a dedicated test key
    let test_key = "_connection_test_key";
    let test_value = "test_connection_works";
    let _: () = connection.set(test_key, test_value)?;
    let result: String = connection.get(test_key)?;
    assert_eq!(result, test_value);

    // Test counter if expected value is provided
    if let Some(expected) = expected_counter {
        let counter_value: i32 = connection.incr("counter", 1)?;
        assert_eq!(
            counter_value, expected,
            "Counter value should be {expected} but was {counter_value}"
        );
    }

    Ok(())
}

#[tokio::test]
async fn test_hotreload_basic_valkey_connection() {
    let _compose = docker_compose("tests/test-configs/hotreload/docker-compose.yaml");
    let shotover_process = shotover_process("tests/test-configs/hotreload/topology.yaml")
        .with_hotreload(true)
        .with_config("tests/test-configs/shotover-config/config_metrics_disabled.yaml")
        .start()
        .await;
    let client = Client::open("valkey://127.0.0.1:6380").unwrap();
    let mut con = client.get_connection().unwrap();

    assert_valkey_connection_works(&mut con, None, &[]).unwrap();

    shotover_process.shutdown_and_then_consume_events(&[]).await;
}

#[tokio::test]
async fn test_hot_reload_with_old_instance_shutdown() {
    let socket_path = "/tmp/test-hotreload-shutdown.sock";
    let _compose = docker_compose("tests/test-configs/hotreload/docker-compose.yaml");

    // Start the old shotover instance
    let shotover_old = shotover_process("tests/test-configs/hotreload/topology.yaml")
        .with_hotreload(true)
        .with_log_name("shot_old")
        .with_hotreload_socket_path(socket_path)
        .with_config("tests/test-configs/shotover-config/config_metrics_disabled.yaml")
        .start()
        .await;

    // Establish connection to old instance and store test data
    let client_old = Client::open("valkey://127.0.0.1:6380").unwrap();
    let mut con_old = client_old.get_connection().unwrap();

    // Store data in the old instance
    let _: () = con_old.set("test_key", "test_value").unwrap();
    let _: () = con_old.set("counter", 0).unwrap();

    // Verify data is accessible through old instance
    let stored_value: String = con_old.get("test_key").unwrap();
    assert_eq!(stored_value, "test_value");

    // Start the new shotover instance that will request hot reload
    // The new instance will automatically request the old instance to shut down
    // after successfully receiving the socket file descriptors
    let shotover_new = shotover_process("tests/test-configs/hotreload/topology.yaml")
        .with_hotreload(true)
        .with_log_name("shot_new")
        .with_hotreload_socket_path("/tmp/shotover-new-shutdown.sock")
        .with_hotreload_from_socket(socket_path)
        .with_config("tests/test-configs/shotover-config/config_metrics_disabled.yaml")
        .start()
        .await;

    // Give some time for the hot reload handoff and shutdown to complete
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    // The old shotover should have shut down now
    shotover_old.consume_remaining_events(&[]).await;

    // Verify that new shotover is still running and can handle connections
    let client_new = Client::open("valkey://127.0.0.1:6380").unwrap();
    let mut con_new = client_new.get_connection().unwrap();

    // Verify data persistence
    let value: String = con_new.get("test_key").unwrap();
    assert_eq!(value, "test_value");

    // Verify new operations work
    assert_valkey_connection_works(&mut con_new, Some(1), &[("test_key", "test_value")]).unwrap();

    // Cleanup
    shotover_new.shutdown_and_then_consume_events(&[]).await;
}
