use crate::helpers::ShotoverManager;
use serial_test::serial;
use test_helpers::docker_compose::DockerCompose;

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_metrics() {
    let _compose = DockerCompose::new("examples/redis-passthrough/docker-compose.yml");
    _compose.wait_for("Ready to accept connections").unwrap();
    let shotover_manager =
        ShotoverManager::from_topology_file("examples/redis-passthrough/topology.yaml");

    let mut connection = shotover_manager.redis_connection(6379);

    redis::cmd("SET")
        .arg("the_key")
        .arg(42)
        .execute(&mut connection);

    redis::cmd("SET")
        .arg("the_key")
        .arg(43)
        .execute(&mut connection);

    let client = hyper::Client::new();
    let uri = "http://localhost:9001/metrics".parse().unwrap();
    let res = client.get(uri).await.unwrap();
    let body_bytes = hyper::body::to_bytes(res.into_body()).await.unwrap();
    let body = String::from_utf8(body_bytes.to_vec()).unwrap();

    // If the body contains these substrings, we can assume metrics are working
    assert!(body.contains("# TYPE shotover_transform_total counter"));
    assert!(body.contains("# TYPE shotover_chain_total counter"));
    assert!(body.contains("# TYPE shotover_available_connections gauge"));
    assert!(body.contains("# TYPE shotover_transform_latency summary"));
    assert!(body.contains("# TYPE shotover_chain_latency summary"));
}
