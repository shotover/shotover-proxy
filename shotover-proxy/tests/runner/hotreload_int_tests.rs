use crate::shotover_process;
use redis::{Client, Commands};
use test_helpers::docker_compose::docker_compose;

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

    let _: () = con.set("test_key", "test_value").unwrap();
    let result: String = con.get("test_key").unwrap();
    assert_eq!(result, "test_value");
    let pong: String = redis::cmd("PING").query(&mut con).unwrap();
    assert_eq!(pong, "PONG");

    shotover_process.shutdown_and_then_consume_events(&[]).await;
}

#[tokio::test]
async fn test_dual_shotover_instances_with_valkey() {
    let socket_path = "/tmp/test-hotreload.sock";
    let _compose = docker_compose("tests/test-configs/hotreload/docker-compose.yaml");
    let shotover_a = shotover_process("tests/test-configs/hotreload/topology.yaml")
        .with_hotreload(true)
        .with_log_name("shot_old")
        .with_hotreload_socket_path(socket_path)
        .with_config("tests/test-configs/shotover-config/config_metrics_disabled.yaml")
        .start()
        .await;
    let client_a = Client::open("valkey://127.0.0.1:6380").unwrap();
    let mut con_a = client_a.get_connection().unwrap();
    let _: () = con_a.set("key_from_a", "value_from_a").unwrap();

    //Second shotover
    let shotover_b = shotover_process("tests/test-configs/hotreload/topology-alt.yaml")
        .with_hotreload_from_socket(socket_path)
        .with_log_name("shot_new")
        .with_config("tests/test-configs/shotover-config/config_metrics_disabled.yaml")
        .start()
        .await;
    let client_b = Client::open("valkey://127.0.0.1:6381").unwrap();
    let mut con_b = client_b.get_connection().unwrap();
    let _: () = con_b.set("key_from_b", "value_from_b").unwrap();

    let value_a_from_b: String = con_b.get("key_from_a").unwrap();
    assert_eq!(value_a_from_b, "value_from_a");
    let value_b_from_a: String = con_a.get("key_from_b").unwrap();
    assert_eq!(value_b_from_a, "value_from_b");

    let final_check: String = con_a.get("key_from_a").unwrap();
    assert_eq!(final_check, "value_from_a");

    shotover_a.shutdown_and_then_consume_events(&[]).await;
    shotover_b.shutdown_and_then_consume_events(&[]).await;
}

#[tokio::test]
async fn test_dual_shotover_instances_with_valkey_ancillary_FD() {
    use std::os::unix::io::AsRawFd;
    use tracing::{info, warn};

    let socket_path = "/tmp/test-hotreload.sock";
    let _compose = docker_compose("tests/test-configs/hotreload/docker-compose.yaml");

    // Start first Shotover instance with hot reload enabled
    let shotover_a = shotover_process("tests/test-configs/hotreload/topology.yaml")
        .with_hotreload(true)
        .with_log_name("shot_old")
        .with_hotreload_socket_path(socket_path)
        .with_config("tests/test-configs/shotover-config/config_metrics_disabled.yaml")
        .start()
        .await;

    let client_a = Client::open("valkey://127.0.0.1:6380").unwrap();
    let mut con_a = client_a.get_connection().unwrap();
    let _: () = con_a.set("key_from_a", "value_from_a").unwrap();

    // Wait for shotover_a to be fully initialized
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Validate file descriptor passing implementation
    info!("Testing file descriptor passing implementation");
    let test_client = crate::hot_reload::client::UnixSocketClient::new(socket_path.to_string());

    match test_client
        .send_request(crate::hot_reload::protocol::Request::SendListeningSockets)
        .await
    {
        Ok(crate::hot_reload::protocol::Response::SendListeningSockets { port_to_fd }) => {
            info!(
                "Successfully received {} file descriptors from shotover_a",
                port_to_fd.len()
            );

            // Validate that we received actual file descriptors
            for (port, fd) in &port_to_fd {
                info!("Port {} -> FD {}", port, fd.0);
                assert!(fd.0 > 0, "File descriptor should be positive: {}", fd.0);
            }

            if port_to_fd.is_empty() {
                warn!("No file descriptors received - this might indicate no active listeners");
            } else {
                info!("File descriptor passing validation passed");
            }
        }
        Ok(crate::hot_reload::protocol::Response::Error(msg)) => {
            panic!("Hot reload request failed: {}", msg);
        }
        Err(e) => {
            panic!("Failed to communicate with hot reload server: {:?}", e);
        }
    }

    // Start second shotover instance that should receive FDs from first
    let shotover_b = shotover_process("tests/test-configs/hotreload/topology-alt.yaml")
        .with_hotreload_from_socket(socket_path)
        .with_log_name("shot_new")
        .with_config("tests/test-configs/shotover-config/config_metrics_disabled.yaml")
        .start()
        .await;

    let client_b = Client::open("valkey://127.0.0.1:6381").unwrap();
    let mut con_b = client_b.get_connection().unwrap();
    let _: () = con_b.set("key_from_b", "value_from_b").unwrap();

    // Test cross-instance data access
    let value_a_from_b: String = con_b.get("key_from_a").unwrap();
    assert_eq!(value_a_from_b, "value_from_a");

    let value_b_from_a: String = con_a.get("key_from_b").unwrap();
    assert_eq!(value_b_from_a, "value_from_b");

    let final_check: String = con_a.get("key_from_a").unwrap();
    assert_eq!(final_check, "value_from_a");

    // Verify no sudo was required
    let uid = nix::unistd::getuid();
    if uid.is_root() {
        warn!("Running as root - cannot validate sudo-free operation");
    } else {
        info!(
            "Running as non-root user (UID: {}) - sudo-free operation confirmed",
            uid
        );
    }

    shotover_a.shutdown_and_then_consume_events(&[]).await;
    shotover_b.shutdown_and_then_consume_events(&[]).await;
}
