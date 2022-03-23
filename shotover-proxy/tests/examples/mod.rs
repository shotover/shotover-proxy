use crate::cassandra_int_tests::{assert_query_result, ResultValue};
use crate::helpers::ShotoverManager;
use serial_test::serial;
use test_helpers::docker_compose::DockerCompose;

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_cassandra_rewrite_peers_example() {
    let _docker_compose =
        DockerCompose::new("example-configs/cassandra-rewrite-peers/docker-compose.yml");

    let shotover_manager = ShotoverManager::from_topology_file(
        "example-configs/cassandra-rewrite-peers/topology.yaml",
    );

    let connection = shotover_manager.cassandra_connection("172.16.1.2", 9043);

    assert_query_result(
        &connection,
        "SELECT native_port FROM system.peers_v2;",
        &[&[ResultValue::Int(9043)], &[ResultValue::Int(9043)]],
    );
}
