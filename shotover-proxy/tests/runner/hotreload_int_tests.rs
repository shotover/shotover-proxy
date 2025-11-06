use crate::shotover_process;
use redis::{Client, Commands};
use std::time::Duration;
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
#[cfg_attr(not(target_os = "linux"), ignore)]
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
#[cfg_attr(not(target_os = "linux"), ignore)]
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

    // Establish multiple connections to old instance to test gradual draining
    let client_old = Client::open("valkey://127.0.0.1:6380").unwrap();

    // Create 13 connections to the old instance
    let mut connections = Vec::new();
    for i in 0..13 {
        let mut con = client_old.get_connection().unwrap();
        // Store some data in each connection
        let _: () = con
            .set(format!("key_{}", i), format!("value_{}", i))
            .unwrap();
        connections.push(con);
    }

    // Set a counter to track
    let _: () = connections[0].set("counter", 0).unwrap();

    // Verify all connections work
    for (i, con) in connections.iter_mut().enumerate() {
        let value: String = con.get(format!("key_{}", i)).unwrap();
        assert_eq!(value, format!("value_{}", i));
    }

    // Start the new shotover instance that will request hot reload
    let shotover_new = shotover_process("tests/test-configs/hotreload/topology.yaml")
        .with_hotreload(true)
        .with_log_name("shot_new")
        .with_hotreload_socket_path("/tmp/shotover-new-shutdown.sock")
        .with_hotreload_from_socket(socket_path)
        .with_config("tests/test-configs/shotover-config/config_metrics_disabled.yaml")
        .start()
        .await;

    // Verify that new shotover is running and can handle new connections
    let client_new = Client::open("valkey://127.0.0.1:6380").unwrap();
    let mut con_new = client_new.get_connection().unwrap();

    // Verify data persistence
    let value: String = con_new.get("key_0").unwrap();
    assert_eq!(value, "value_0");

    // Keep testing old connections periodically
    // Some should start failing as they are gradually drained
    let mut connections_failed = 0;

    // Test over a period of time (e.g., 35 seconds to allow for 3-4 drain cycles)
    // Each cycle drains 10% every 10 seconds
    for _ in 0..7 {
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

        connections_failed = 0;

        for (i, con) in connections.iter_mut().enumerate() {
            if con.get::<_, String>(format!("key_{}", i)).is_err() {
                connections_failed += 1;
            }
        }
    }

    // After sufficient time, most or all old connections should have been drained
    assert!(
        connections_failed > 0,
        "Expected some connections to be drained, but none were"
    );

    // Verify new connections still work
    assert_valkey_connection_works(&mut con_new, Some(1), &[("key_0", "value_0")]).unwrap();

    // Wait for the old shotover to complete draining
    tokio::time::timeout(
        Duration::from_secs(100),
        shotover_old.consume_remaining_events(&[]),
    )
    .await
    .unwrap();

    // Cleanup
    shotover_new.shutdown_and_then_consume_events(&[]).await;
}
