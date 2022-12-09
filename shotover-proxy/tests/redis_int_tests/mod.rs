use crate::helpers::redis_connection;
use crate::helpers::ShotoverManager;
use basic_driver_tests::*;
use redis::aio::Connection;
use redis::Commands;
use serial_test::serial;
use shotover_proxy::tls::TlsConnectorConfig;
use std::path::Path;
use std::thread::sleep;
use std::time::Duration;
use test_helpers::docker_compose::DockerCompose;

pub mod assert;
pub mod basic_driver_tests;

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn passthrough() {
    let _compose = DockerCompose::new("example-configs/redis-passthrough/docker-compose.yaml");
    let _shotover_manager =
        ShotoverManager::from_topology_file("example-configs/redis-passthrough/topology.yaml");
    let mut connection = redis_connection::new_async(6379).await;
    let mut flusher = Flusher::new_single_connection(redis_connection::new_async(6379).await).await;

    run_all(&mut connection, &mut flusher).await;
    test_invalid_frame().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn passthrough_redis_down() {
    let _shotover_manager =
        ShotoverManager::from_topology_file("example-configs/redis-passthrough/topology.yaml");
    let mut connection = redis_connection::new_async(6379).await;

    test_trigger_transform_failure_driver(&mut connection).await;
    test_trigger_transform_failure_raw().await;
    test_invalid_frame().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn cluster_tls() {
    test_helpers::cert::generate_redis_test_certs(Path::new("example-configs/redis-tls/certs"));

    {
        let _compose = DockerCompose::new("example-configs/redis-cluster-tls/docker-compose.yaml");
        let _shotover_manager =
            ShotoverManager::from_topology_file("example-configs/redis-cluster-tls/topology.yaml");

        let mut connection = redis_connection::new_async(6379).await;
        let mut flusher = Flusher::new_cluster().await;

        run_all_cluster_hiding(&mut connection, &mut flusher).await;
        test_cluster_ports_rewrite_slots(&mut connection, 6379).await;
    }

    // Quick test to verify it works with private key
    {
        let _compose =
            DockerCompose::new("example-configs/redis-cluster-tls/docker-compose-with-key.yaml");
        let _shotover_manager = ShotoverManager::from_topology_file(
            "example-configs/redis-cluster-tls/topology-with-key.yaml",
        );

        let mut connection = redis_connection::new_async(6379).await;
        test_cluster_basics(&mut connection).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn source_tls_and_single_tls() {
    test_helpers::cert::generate_redis_test_certs(Path::new("example-configs/redis-tls/certs"));

    let _compose = DockerCompose::new("example-configs/redis-tls/docker-compose.yaml");
    let _shotover_manager =
        ShotoverManager::from_topology_file("example-configs/redis-tls/topology.yaml");

    let tls_config = TlsConnectorConfig {
        certificate_authority_path: "example-configs/redis-tls/certs/ca.crt".into(),
        certificate_path: Some("example-configs/redis-tls/certs/redis.crt".into()),
        private_key_path: Some("example-configs/redis-tls/certs/redis.key".into()),
        verify_hostname: false,
    };

    let mut connection = redis_connection::new_async_tls(6380, tls_config.clone()).await;
    let mut flusher =
        Flusher::new_single_connection(redis_connection::new_async_tls(6380, tls_config).await)
            .await;

    run_all(&mut connection, &mut flusher).await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn cluster_ports_rewrite() {
    let _compose =
        DockerCompose::new("tests/test-configs/redis-cluster-ports-rewrite/docker-compose.yaml");
    let _shotover_manager = ShotoverManager::from_topology_file(
        "tests/test-configs/redis-cluster-ports-rewrite/topology.yaml",
    );

    let mut connection = redis_connection::new_async(6380).await;
    let mut flusher = Flusher::new_single_connection(redis_connection::new_async(6380).await).await;

    run_all_cluster_hiding(&mut connection, &mut flusher).await;

    test_cluster_ports_rewrite_slots(&mut connection, 6380).await;
    test_cluster_ports_rewrite_nodes(&mut connection, 6380).await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn multi() {
    let _compose = DockerCompose::new("example-configs/redis-multi/docker-compose.yaml");
    let _shotover_manager =
        ShotoverManager::from_topology_file("example-configs/redis-multi/topology.yaml");
    let mut connection = redis_connection::new_async(6379).await;
    let mut flusher = Flusher::new_single_connection(redis_connection::new_async(6379).await).await;

    run_all_multi_safe(&mut connection, &mut flusher).await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn cluster_auth() {
    let _compose = DockerCompose::new("tests/test-configs/redis-cluster-auth/docker-compose.yaml");
    let _shotover_manager =
        ShotoverManager::from_topology_file("tests/test-configs/redis-cluster-auth/topology.yaml");
    let mut connection = redis_connection::new_async(6379).await;

    test_auth(&mut connection).await;
    test_auth_isolation(&mut connection).await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn cluster_hiding() {
    let _compose = DockerCompose::new("example-configs/redis-cluster-hiding/docker-compose.yaml");
    let _shotover_manager =
        ShotoverManager::from_topology_file("example-configs/redis-cluster-hiding/topology.yaml");

    let mut connection = redis_connection::new_async(6379).await;
    let connection = &mut connection;
    let mut flusher = Flusher::new_cluster().await;

    run_all_cluster_hiding(connection, &mut flusher).await;
    test_cluster_ports_rewrite_slots(connection, 6379).await;
    test_cluster_ports_rewrite_nodes(connection, 6379).await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn cluster_handling() {
    let _compose = DockerCompose::new("example-configs/redis-cluster-handling/docker-compose.yaml");
    let _shotover_manager =
        ShotoverManager::from_topology_file("example-configs/redis-cluster-handling/topology.yaml");

    let mut connection = redis_connection::new_async(6379).await;
    let connection = &mut connection;

    let mut flusher = Flusher::new_cluster().await;

    run_all_cluster_handling(connection, &mut flusher).await;
    test_cluster_ports_rewrite_slots(connection, 6379).await;
    test_cluster_ports_rewrite_nodes(connection, 6379).await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn cluster_dr() {
    let _compose = DockerCompose::new("example-configs/redis-cluster-dr/docker-compose.yaml");

    let nodes = vec![
        "redis://127.0.0.1:2120/",
        "redis://127.0.0.1:2121/",
        "redis://127.0.0.1:2122/",
        "redis://127.0.0.1:2123/",
        "redis://127.0.0.1:2124/",
        "redis://127.0.0.1:2125/",
    ];
    let client = redis::cluster::ClusterClientBuilder::new(nodes)
        .password("shotover".to_string())
        .build()
        .unwrap();
    let mut replication_connection = client.get_connection().unwrap();

    // test coalesce sends messages on shotover shutdown
    {
        let _shotover_manager =
            ShotoverManager::from_topology_file("example-configs/redis-cluster-dr/topology.yaml");
        let mut connection = redis_connection::new_async(6379).await;
        redis::cmd("AUTH")
            .arg("default")
            .arg("shotover")
            .query_async::<_, ()>(&mut connection)
            .await
            .unwrap();

        redis::cmd("SET")
            .arg("key1")
            .arg(42)
            .query_async::<_, ()>(&mut connection)
            .await
            .unwrap();
        redis::cmd("SET")
            .arg("key2")
            .arg(358)
            .query_async::<_, ()>(&mut connection)
            .await
            .unwrap();

        // shotover is shutdown here because shotover_manager goes out of scope and is dropped.
    }
    sleep(Duration::from_secs(1));
    assert_eq!(replication_connection.get::<&str, i32>("key1").unwrap(), 42);
    assert_eq!(
        replication_connection.get::<&str, i32>("key2").unwrap(),
        358
    );

    let _shotover_manager =
        ShotoverManager::from_topology_file("example-configs/redis-cluster-dr/topology.yaml");

    async fn new_connection() -> Connection {
        let mut connection = redis_connection::new_async(6379).await;

        redis::cmd("AUTH")
            .arg("default")
            .arg("shotover")
            .query_async::<_, ()>(&mut connection)
            .await
            .unwrap();

        connection
    }
    let mut connection = new_connection().await;
    let mut flusher = Flusher::new_single_connection(new_connection().await).await;

    test_cluster_replication(&mut connection, &mut replication_connection).await;
    test_dr_auth().await;
    run_all_cluster_hiding(&mut connection, &mut flusher).await;
}
