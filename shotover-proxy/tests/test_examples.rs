mod helpers;

use helpers::ShotoverManager;
use serial_test::serial;
use test_helpers::docker_compose::DockerCompose;

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_cass_redis_kafka_topology_valid() {
    let _compose = DockerCompose::new("examples/cass-redis-kafka/docker-compose.yml")
        .wait_for_n("Startup complete", 1);

    let _shotover_proxy =
        ShotoverManager::from_topology_file("examples/cass-redis-kafka/topology.yaml");
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_cassandra_standalone() {
    let _compose = DockerCompose::new("examples/cassandra-standalone/docker-compose.yml")
        .wait_for_n("Startup complete", 1);

    let _shotover_proxy =
        ShotoverManager::from_topology_file("examples/cassandra-standalone/topology.yaml");
}
