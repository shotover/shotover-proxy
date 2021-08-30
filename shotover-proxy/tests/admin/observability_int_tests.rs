use crate::helpers::ShotoverManager;
use serial_test::serial;
use test_helpers::docker_compose::DockerCompose;

#[test]
#[serial]
fn test_metrics() {
    let _compose = DockerCompose::new("examples/redis-passthrough/docker-compose.yml");

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

    let body = reqwest::blocking::get("http://localhost:9001/metrics")
        .unwrap()
        .text()
        .unwrap();

    // If the body contains these substrings, we can assume metrics are working
    assert!(body.contains("# TYPE shotover_transform_total counter"));
    assert!(body.contains("# TYPE shotover_chain_total counter"));
    assert!(body.contains("# TYPE shotover_available_connections gauge"));
    assert!(body.contains("# TYPE shotover_transform_latency summary"));
    assert!(body.contains("# TYPE shotover_chain_latency summary"));
}
