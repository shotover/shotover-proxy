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

    // Track connection failures over multiple drain cycles
    // The gradual shutdown drains 10% of initial connections (rounded up to at least 1)
    // For 13 connections: 10% = 1.3 -> rounds to 2 connections per cycle
    // Each drain cycle waits 10 seconds between drains
    //
    // Note: Low connection counts give non-ideal cycle counts due to rounding.
    // For example, 13 connections requires 7 cycles to fully drain.
    // With realistic connection counts (100+), a 10% drain rate consistently takes ~10 cycles.
    let total_connections = connections.len();
    let expected_drain_per_cycle =
        std::cmp::max(1, (total_connections as f64 * 0.1).ceil() as usize);

    // Calculate how many drain cycles we need to verify
    // We'll verify enough cycles to drain at least 50% of connections
    let num_cycles_to_verify = std::cmp::min(
        5,
        (total_connections as f64 / expected_drain_per_cycle as f64 / 2.0).ceil() as usize,
    );

    // Verify each drain cycle
    for cycle in 1..=num_cycles_to_verify {
        tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;

        let mut connections_failed = 0;
        for (i, con) in connections.iter_mut().enumerate() {
            if con.get::<_, String>(format!("key_{}", i)).is_err() {
                connections_failed += 1;
            }
        }

        let expected_drained = expected_drain_per_cycle * cycle;
        assert!(
            connections_failed >= expected_drained,
            "After drain cycle {}: expected at least {} connections drained, but only {} were drained",
            cycle,
            expected_drained,
            connections_failed
        );
    }

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

#[tokio::test]
#[cfg_attr(not(target_os = "linux"), ignore)]
async fn test_hot_reload_with_zero_connections() {
    let socket_path = "/tmp/test-hotreload-zero-connections.sock";

    let _compose = docker_compose("tests/test-configs/hotreload/docker-compose.yaml");

    // Start the old shotover instance
    let shotover_old = shotover_process("tests/test-configs/hotreload/topology.yaml")
        .with_hotreload(true)
        .with_log_name("shot_old_0")
        .with_hotreload_socket_path(socket_path)
        .with_config("tests/test-configs/shotover-config/config_metrics_disabled.yaml")
        .start()
        .await;

    // Start the new shotover instance that will request hot reload
    let shotover_new = shotover_process("tests/test-configs/hotreload/topology.yaml")
        .with_hotreload(true)
        .with_log_name("shot_new_0")
        .with_hotreload_socket_path("/tmp/shotover-new-zero-connections.sock")
        .with_hotreload_from_socket(socket_path)
        .with_config("tests/test-configs/shotover-config/config_metrics_disabled.yaml")
        .start()
        .await;

    // Verify that new shotover is running and can handle new connections
    let client_new = Client::open("valkey://127.0.0.1:6380").unwrap();
    let mut con_new = client_new.get_connection().unwrap();

    // Verify basic connectivity works
    let pong: String = redis::cmd("PING").query(&mut con_new).unwrap();
    assert_eq!(pong, "PONG");

    // The old shotover should shutdown almost immediately since there are no connections to drain
    // This validates that the gradual shutdown recognizes the zero-connections case
    // and doesn't waste time running unnecessary drain cycles
    tokio::time::timeout(
        Duration::from_secs(5),
        shotover_old.consume_remaining_events(&[]),
    )
    .await
    .expect("Old shotover should shutdown immediately when there are no connections to drain");

    // Verify new shotover is still working after old one shut down
    assert_valkey_connection_works(&mut con_new, None, &[]).unwrap();

    // Cleanup
    shotover_new.shutdown_and_then_consume_events(&[]).await;
}
