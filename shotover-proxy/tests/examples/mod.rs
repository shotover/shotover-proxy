use crate::helpers::cassandra::{assert_query_result, cassandra_connection, ResultValue};
use serial_test::serial;
use test_helpers::docker_compose::DockerCompose;

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_cassandra_rewrite_peers_example() {
    let _docker_compose =
        DockerCompose::new("example-configs-docker/cassandra-rewrite-peers/docker-compose.yml");

    let connection = cassandra_connection("172.16.1.2", 9043);

    assert_query_result(
        &connection,
        "SELECT native_port FROM system.peers_v2;",
        &[&[ResultValue::Int(9043)], &[ResultValue::Int(9043)]],
    );
}
