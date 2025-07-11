use crate::shotover_process;
use redis::{Client, Commands};
use std::time::Duration;
use test_helpers::docker_compose::docker_compose;
use tokio::time::sleep;

#[tokio::test]
async fn test_hotreload_basic_redis_connection() {
    println!(" Test 1: Basic Redis connection with hotreload");
    let _compose = docker_compose("tests/transforms/docker-compose-redis.yaml");
    sleep(Duration::from_millis(2000)).await;
    println!(" Starting shotover with --hotreload...");
    let shotover_process = shotover_process("tests/test-configs/redis-passthrough/topology.yaml")
        .with_hotreload(true)
        .with_config("tests/test-configs/shotover-config/config_metrics_disabled.yaml")
        .start()
        .await;
    sleep(Duration::from_millis(1000)).await;
    println!(" Connecting to shotover (port 6380)...");
    let client = Client::open("redis://127.0.0.1:6380").expect("Failed to create Redis client");
    let mut con = client
        .get_connection()
        .expect("Failed to connect to Redis through shotover");

    let _: () = con
        .set("test_key", "test_value")
        .expect("Failed to SET key");
    println!(" SET command successful");
    let result: String = con.get("test_key").expect("Failed to GET key");
    assert_eq!(result, "test_value");
    println!(" GET command successful: {}", result);
    let pong: String = redis::cmd("PING").query(&mut con).expect("Failed to PING");

    assert_eq!(pong, "PONG");
    println!(" PING command successful: {}", pong);
    println!(" Test 1 completed successfully!");
    shotover_process.shutdown_and_then_consume_events(&[]).await;
}

#[tokio::test]
async fn test_dual_shotover_instances_with_redis() {
    println!(" Test 2: Dual shotover instances with Redis");

    let _compose = docker_compose("tests/transforms/docker-compose-redis.yaml");
    sleep(Duration::from_millis(2000)).await;
    println!(" Starting shotover instance A with --hotreload...");
    let shotover_a = shotover_process("tests/test-configs/redis-passthrough/topology.yaml")
        .with_hotreload(true)
        .with_config("tests/test-configs/shotover-config/config_metrics_disabled.yaml")
        .start()
        .await;
    println!(" Connecting to shotover instance A (port 6380)...");
    let client_a = Client::open("redis://127.0.0.1:6380")
        .expect("Failed to create Redis client for instance A");
    let mut con_a = client_a
        .get_connection()
        .expect("Failed to connect to shotover instance A");
    let _: () = con_a
        .set("key_from_a", "value_from_a")
        .expect("Failed to SET on instance A");
    println!(" Instance A: SET key_from_a = value_from_a");
    println!(" Starting shotover instance B with --hotreload...");
    let shotover_b =
        shotover_process("tests/test-configs/redis-passthrough-alt-port/topology.yaml")
            .with_hotreload(true)
            .with_config("tests/test-configs/shotover-config/config_metrics_disabled.yaml")
            .start()
            .await;
    println!(" Connecting to shotover instance B (port 6381)...");
    let client_b = Client::open("redis://127.0.0.1:6381")
        .expect("Failed to create Redis client for instance B");
    let mut con_b = client_b
        .get_connection()
        .expect("Failed to connect to shotover instance B");
    let _: () = con_b
        .set("key_from_b", "value_from_b")
        .expect("Failed to SET on instance B");
    println!(" Instance B: SET key_from_b = value_from_b");

    let value_a_from_b: String = con_b
        .get("key_from_a")
        .expect("Failed to GET key_from_a on instance B");
    assert_eq!(value_a_from_b, "value_from_a");
    println!(" Instance B can read data written by instance A");
    let value_b_from_a: String = con_a
        .get("key_from_b")
        .expect("Failed to GET key_from_b on instance A");
    assert_eq!(value_b_from_a, "value_from_b");
    println!(" Instance A can read data written by instance B");

    let final_check: String = con_a
        .get("key_from_a")
        .expect("Failed to GET key_from_a on instance A");
    assert_eq!(final_check, "value_from_a");
    println!(" Instance A still working after instance B started");
    println!(" Test 2 completed successfully!");

    shotover_a.shutdown_and_then_consume_events(&[]).await;
    shotover_b.shutdown_and_then_consume_events(&[]).await;
}
