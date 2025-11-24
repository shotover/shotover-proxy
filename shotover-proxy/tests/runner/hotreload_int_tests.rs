use crate::shotover_process;
use redis::{Client, Commands};
use std::path::Path;
use std::time::Duration;
use test_helpers::connection::valkey_connection::create_tls_valkey_client_from_certs;
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
        .with_hotreload_socket("/tmp/shotover-basic-test.sock")
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
        .with_log_name("shot_old")
        .with_hotreload_socket(socket_path)
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
        .with_log_name("shot_new")
        .with_hotreload_socket(socket_path)
        .with_config("tests/test-configs/shotover-config/config_metrics_disabled.yaml")
        .start()
        .await;

    // Verify that new shotover is running and can handle new connections
    let client_new = Client::open("valkey://127.0.0.1:6380").unwrap();
    let mut con_new = client_new.get_connection().unwrap();

    // Verify data persistence
    let value: String = con_new.get("key_0").unwrap();
    assert_eq!(value, "value_0");

    // After the new shotover starts, gradual shutdown begins immediately with the first drain.
    // Subsequent drains happen every 10 seconds.
    // Timeline: t=0: 1st drain, t=10: 2nd drain, t=20: 3rd drain, etc.
    // Sleep 5s to position ourselves in the middle of the time between 1st and 2nd drain.
    // This reduces race conditions since we're furthest from drain boundaries.
    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

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

    // Verify the drain rate calculation matches expected value for 13 connections
    assert_eq!(
        expected_drain_per_cycle, 2,
        "Expected 2 connections to drain per cycle (10% of 13), but got {}",
        expected_drain_per_cycle
    );

    // Calculate how many drain cycles we need to verify
    // We'll verify enough cycles to drain at least 50% of connections
    let num_cycles_to_verify = std::cmp::min(
        5,
        (total_connections as f64 / expected_drain_per_cycle as f64 / 2.0).ceil() as usize,
    );

    // Verify we're testing the expected number of cycles for 13 connections
    assert_eq!(
        num_cycles_to_verify, 4,
        "Expected to verify 4 drain cycles (50% of 13 connections / 2 per cycle), but got {}",
        num_cycles_to_verify
    );

    // Verify each drain cycle
    for cycle in 1..=num_cycles_to_verify {
        let mut connections_failed = 0;
        for (i, con) in connections.iter_mut().enumerate() {
            if con.get::<_, String>(format!("key_{}", i)).is_err() {
                connections_failed += 1;
            }
        }

        let expected_drained = expected_drain_per_cycle * cycle;
        assert_eq!(
            connections_failed, expected_drained,
            "After drain cycle {}: expected exactly {} connections drained, but {} were drained",
            cycle, expected_drained, connections_failed
        );

        // Sleep before next cycle check
        if cycle < num_cycles_to_verify {
            tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
        }
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
        .with_log_name("shot_old_0")
        .with_hotreload_socket(socket_path)
        .with_config("tests/test-configs/shotover-config/config_metrics_disabled.yaml")
        .start()
        .await;

    // Start the new shotover instance that will request hot reload
    let shotover_new = shotover_process("tests/test-configs/hotreload/topology.yaml")
        .with_log_name("shot_new_0")
        .with_hotreload_socket(socket_path)
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

#[tokio::test]
#[cfg_attr(not(target_os = "linux"), ignore)]
async fn test_hot_reload_certificate_change() {
    let socket_path = "/tmp/test-hotreload-cert-change.sock";

    // Generate two distinct certificate sets for testing certificate rotation
    // The certificates will have different serial numbers and timestamps
    test_helpers::cert::generate_test_certs(Path::new(
        "shotover-proxy/tests/test-configs/valkey/tls/certs",
    ));
    test_helpers::cert::generate_test_certs(Path::new(
        "shotover-proxy/tests/test-configs/valkey/tls2/certs",
    ));

    let _compose = docker_compose("tests/test-configs/hotreload/docker-compose.yaml");

    // Start the old shotover instance with old certificates
    let shotover_old = shotover_process("tests/test-configs/hotreload/topology-tls-old.yaml")
        .with_log_name("old_tls")
        .with_hotreload_socket(socket_path)
        .with_config("tests/test-configs/shotover-config/config_metrics_disabled.yaml")
        .start()
        .await;

    // Create TLS clients configured with old CA certificate
    let client_old =
        create_tls_valkey_client_from_certs("127.0.0.1", 6380, "shotover-proxy/tests/test-configs/valkey/tls/certs");

    // Establish multiple TLS connections to old instance
    let mut connections = Vec::new();
    for i in 0..10 {
        let mut con = client_old.get_connection().unwrap();
        // Store some data in each connection to verify persistence
        let _: () = con
            .set(format!("cert_key_{}", i), format!("cert_value_{}", i))
            .unwrap();
        connections.push(con);
    }

    // Set a counter to track across the certificate change
    let _: () = connections[0].set("counter", 0).unwrap();

    // Verify all old connections work with old certificate
    for (i, con) in connections.iter_mut().enumerate() {
        let value: String = con.get(format!("cert_key_{}", i)).unwrap();
        assert_eq!(value, format!("cert_value_{}", i));
    }

    // Start the new shotover instance with NEW certificates - triggers hot reload
    let shotover_new = shotover_process("tests/test-configs/hotreload/topology-tls-new.yaml")
        .with_log_name("new_tls")
        .with_hotreload_socket(socket_path)
        .with_config("tests/test-configs/shotover-config/config_metrics_disabled.yaml")
        .start()
        .await;

    // Create a new TLS client configured with certificate
    let client_new =
        create_tls_valkey_client_from_certs("127.0.0.1", 6380, "shotover-proxy/tests/test-configs/valkey/tls2/certs");
    let mut con_new = client_new.get_connection().unwrap();

    // Verify data persistence across certificate change
    let value: String = con_new.get("cert_key_0").unwrap();
    assert_eq!(value, "cert_value_0");

    // Verify the new connection works with the new certificate
    let pong: String = redis::cmd("PING").query(&mut con_new).unwrap();
    assert_eq!(pong, "PONG");

    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

    // Verify that old connections with old certificates still work during drain
    // The old instance should still be serving requests with the old certificate
    let mut old_connections_working = 0;
    for con in connections.iter_mut() {
        if con.get::<_, String>("cert_key_0").is_ok() {
            old_connections_working += 1;
        }
    }

    // At least some old connections should still be working
    assert!(
        old_connections_working > 0,
        "Some old certificate connections should still be working during gradual drain"
    );

    // Verify new certificate client continues to work throughout the transition
    // Counter starts at 0, this will increment to 1
    assert_valkey_connection_works(&mut con_new, Some(1), &[("cert_key_0", "cert_value_0")])
        .unwrap();

    // Wait for multiple drain cycles to observe gradual connection closure
    tokio::time::sleep(tokio::time::Duration::from_secs(20)).await;

    // Check that more old connections have been drained
    let mut old_connections_still_working = 0;
    for con in connections.iter_mut() {
        if con.get::<_, String>("cert_key_0").is_ok() {
            old_connections_still_working += 1;
        }
    }

    // Should have fewer working connections after additional drain cycles
    assert!(
        old_connections_still_working < old_connections_working,
        "Old connections should be gradually drained. Before: {}, After: {}",
        old_connections_working,
        old_connections_still_working
    );

    // Verify new certificate client is still working perfectly
    // Counter is now at 1, this will increment to 2
    assert_valkey_connection_works(&mut con_new, Some(2), &[("cert_key_0", "cert_value_0")])
        .unwrap();

    // Create another new client to ensure new connections use the new certificate
    let client_new2 =
        create_tls_valkey_client_from_certs("127.0.0.1", 6380, "shotover-proxy/tests/test-configs/valkey/tls2/certs");
    let mut con_new2 = client_new2.get_connection().unwrap();

    // Verify this new connection also works with new certificate
    let pong: String = redis::cmd("PING").query(&mut con_new2).unwrap();
    assert_eq!(pong, "PONG");

    // Verify data is accessible
    let value: String = con_new2.get("cert_key_5").unwrap();
    assert_eq!(value, "cert_value_5");

    // Wait for the old shotover to complete draining
    tokio::time::timeout(
        Duration::from_secs(120),
        shotover_old.consume_remaining_events(&[]),
    )
    .await
    .expect("Old shotover should complete drain and shutdown within timeout");

    // Verify all old connections are now closed
    let mut old_connections_remaining = 0;
    for con in connections.iter_mut() {
        if con.get::<_, String>("cert_key_0").is_ok() {
            old_connections_remaining += 1;
        }
    }

    assert_eq!(
        old_connections_remaining, 0,
        "All old certificate connections should be drained and closed after full drain cycle"
    );

    // new certificate connections still work perfectly
    // Counter is now at 2, this will increment to 3
    assert_valkey_connection_works(
        &mut con_new,
        Some(3),
        &[
            ("cert_key_0", "cert_value_0"),
            ("cert_key_5", "cert_value_5"),
        ],
    )
    .unwrap();

    shotover_new.shutdown_and_then_consume_events(&[]).await;
}
