use crate::shotover_process;
use redis::{Client, Commands};
use test_helpers::docker_compose::docker_compose;

#[tokio::test]
async fn test_hotreload_basic_redis_connection() {
    let _compose = docker_compose("tests/test-configs/hotreload/docker-compose.yaml");
    let shotover_process = shotover_process("tests/test-configs/hotreload/topology.yaml")
        .with_hotreload(true)
        .with_config("tests/test-configs/shotover-config/config_metrics_disabled.yaml")
        .start()
        .await;
    let client = Client::open("redis://127.0.0.1:6380").expect("Failed to create Redis client");
    let mut con = client
        .get_connection()
        .expect("Failed to connect to Redis through shotover");

    let _: () = con
        .set("test_key", "test_value")
        .expect("Failed to SET key");
    let result: String = con.get("test_key").expect("Failed to GET key");
    assert_eq!(result, "test_value");
    let pong: String = redis::cmd("PING").query(&mut con).expect("Failed to PING");
    assert_eq!(pong, "PONG");

    shotover_process.shutdown_and_then_consume_events(&[]).await;
}

#[tokio::test]
async fn test_dual_shotover_instances_with_redis() {
    let _compose = docker_compose("tests/test-configs/hotreload/docker-compose.yaml");
    let shotover_a = shotover_process("tests/test-configs/hotreload/topology.yaml")
        .with_hotreload(true)
        .with_config("tests/test-configs/shotover-config/config_metrics_disabled.yaml")
        .start()
        .await;
    let client_a = Client::open("redis://127.0.0.1:6380")
        .expect("Failed to create Redis client for instance A");
    let mut con_a = client_a
        .get_connection()
        .expect("Failed to connect to shotover instance A");
    let _: () = con_a
        .set("key_from_a", "value_from_a")
        .expect("Failed to SET on instance A");
    let shotover_b = shotover_process("tests/test-configs/hotreload/topology-alt.yaml")
        .with_hotreload(true)
        .with_config("tests/test-configs/shotover-config/config_metrics_disabled.yaml")
        .start()
        .await;
    let client_b = Client::open("redis://127.0.0.1:6381")
        .expect("Failed to create Redis client for instance B");
    let mut con_b = client_b
        .get_connection()
        .expect("Failed to connect to shotover instance B");
    let _: () = con_b
        .set("key_from_b", "value_from_b")
        .expect("Failed to SET on instance B");

    let value_a_from_b: String = con_b
        .get("key_from_a")
        .expect("Failed to GET key_from_a on instance B");
    assert_eq!(value_a_from_b, "value_from_a");
    let value_b_from_a: String = con_a
        .get("key_from_b")
        .expect("Failed to GET key_from_b on instance A");
    assert_eq!(value_b_from_a, "value_from_b");

    let final_check: String = con_a
        .get("key_from_a")
        .expect("Failed to GET key_from_a on instance A");
    assert_eq!(final_check, "value_from_a");

    shotover_a.shutdown_and_then_consume_events(&[]).await;
    shotover_b.shutdown_and_then_consume_events(&[]).await;
}
