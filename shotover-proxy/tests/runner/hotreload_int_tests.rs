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
    assert_valkey_connection_works(&mut con_new, None, &[("key_0", "value_0")]).unwrap();

    // After the new shotover starts, gradual shutdown begins immediately.
    // The implementation drains connections continuously in chunks:
    // - Chunk duration: 200ms
    // - For 10s shutdown: 50 chunks
    // - For 13 connections: ceil(13/50) = 1 connection per chunk
    //
    // Timeline: Every 200ms, 1 connection is drained continuously
    // We'll verify the drain behavior at specific time intervals
    let total_connections = connections.len();
    let shutdown_duration = Duration::from_secs(10);
    let chunk_duration = Duration::from_millis(200);
    let num_chunks = shutdown_duration.div_duration_f64(chunk_duration) as usize;
    let connections_per_chunk = std::cmp::max(1, total_connections.div_ceil(num_chunks));

    // Verify the drain rate calculation matches expected value for 13 connections
    assert_eq!(
        connections_per_chunk, 1,
        "Expected 1 connection to drain per chunk (ceil(13/50)), but got {}",
        connections_per_chunk
    );

    // We'll check the state after a few drain cycles
    // Each cycle consists of multiple chunks, so we need to wait appropriately
    // Let's verify after approximately 20%, 40%, 60%, and 80% of the shutdown period
    let check_interval = chunk_duration * 10; // 10 chunks = ~2 seconds per check

    // Verify the drain behavior at each check interval
    let mut cumulative_duration = Duration::ZERO;
    for check_num in 0..4 {
        // Sleep for the interval duration
        tokio::time::sleep(check_interval).await;

        cumulative_duration += check_interval;
        let cumulative_chunks = cumulative_duration.div_duration_f64(chunk_duration) as usize;

        // Count how many connections have been drained
        let mut connections_failed = 0;
        for (i, con) in connections.iter_mut().enumerate() {
            if con.get::<_, String>(format!("key_{}", i)).is_err() {
                connections_failed += 1;
            }
        }

        // Calculate expected drained connections
        // We expect approximately cumulative_chunks * connections_per_chunk to be drained
        // Allow some tolerance since timing isn't perfectly precise
        let expected_drained =
            std::cmp::min(cumulative_chunks * connections_per_chunk, total_connections);

        // Allow +/- 2 connections tolerance for timing variations
        let tolerance = 2;
        assert!(
            connections_failed >= expected_drained.saturating_sub(tolerance)
                && connections_failed <= (expected_drained + tolerance).min(total_connections),
            "After check {}: expected approximately {} connections drained (±{} tolerance), but {} were drained (after {} chunks)",
            check_num + 1,
            expected_drained,
            tolerance,
            connections_failed,
            cumulative_chunks
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
    assert_valkey_connection_works(&mut con_new, None, &[]).unwrap();

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
    test_helpers::cert::generate_test_certs(Path::new("tests/test-configs/valkey/tls/certs"));
    test_helpers::cert::generate_test_certs(Path::new("tests/test-configs/valkey/tls2/certs"));

    let _compose = docker_compose("tests/test-configs/hotreload/docker-compose.yaml");

    // Start the old shotover instance with old certificates
    let shotover_old = shotover_process("tests/test-configs/hotreload/topology-tls-old.yaml")
        .with_log_name("old_tls")
        .with_hotreload_socket(socket_path)
        .with_config("tests/test-configs/shotover-config/config_metrics_disabled.yaml")
        .start()
        .await;

    // Create TLS clients configured with old CA certificate
    let client_old = create_tls_valkey_client_from_certs(
        "127.0.0.1",
        6380,
        "tests/test-configs/valkey/tls/certs",
    );

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
    let client_new = create_tls_valkey_client_from_certs(
        "127.0.0.1",
        6380,
        "tests/test-configs/valkey/tls2/certs",
    );
    let mut con_new = client_new.get_connection().unwrap();

    // Verify data persistence across certificate change and that new connection works
    assert_valkey_connection_works(&mut con_new, None, &[("cert_key_0", "cert_value_0")]).unwrap();

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

    // Create another new connection to ensure new connections use the new certificate
    let mut con_new2 = client_new.get_connection().unwrap();

    // Verify this new connection also works with new certificate
    assert_valkey_connection_works(&mut con_new2, None, &[("cert_key_5", "cert_value_5")]).unwrap();

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
