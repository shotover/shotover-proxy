use crate::cassandra_int_tests::cluster::run_topology_task;
use crate::helpers::cassandra::{assert_query_result, CassandraConnection, ResultValue};
use std::net::SocketAddr;

async fn test_rewrite_system_peers(connection: &CassandraConnection) {
    let all_columns = "peer, data_center, host_id, preferred_ip, rack, release_version, rpc_address, schema_version, tokens";
    assert_query_result(connection, "SELECT * FROM system.peers;", &[]).await;
    assert_query_result(
        connection,
        &format!("SELECT {all_columns} FROM system.peers;"),
        &[],
    )
    .await;
    assert_query_result(
        connection,
        &format!("SELECT {all_columns}, {all_columns} FROM system.peers;"),
        &[],
    )
    .await;
}

async fn test_rewrite_system_peers_v2(connection: &CassandraConnection) {
    let all_columns = "peer, peer_port, data_center, host_id, native_address, native_port, preferred_ip, preferred_port, rack, release_version, schema_version, tokens";
    assert_query_result(connection, "SELECT * FROM system.peers_v2;", &[]).await;
    assert_query_result(
        connection,
        &format!("SELECT {all_columns} FROM system.peers_v2;"),
        &[],
    )
    .await;
    assert_query_result(
        connection,
        &format!("SELECT {all_columns}, {all_columns} FROM system.peers_v2;"),
        &[],
    )
    .await;
}

async fn test_rewrite_system_peers_dummy_peers(connection: &CassandraConnection) {
    let star_results1 = [
        ResultValue::Inet("127.0.0.1".parse().unwrap()),
        ResultValue::Varchar("dc1".into()),
        ResultValue::Uuid("3c3c4e2d-ba74-4f76-b52e-fb5bcee6a9f4".parse().unwrap()),
        ResultValue::Null,
        ResultValue::Varchar("rack1".into()),
        ResultValue::Varchar("4.0.6".into()),
        ResultValue::Null,
        // schema_version is non deterministic so we cant assert on it.
        ResultValue::Any,
        // Unfortunately token generation appears to be non-deterministic but we can at least assert that
        // there are 128 tokens per node
        ResultValue::Set(std::iter::repeat(ResultValue::Any).take(3 * 128).collect()),
    ];
    let star_results2 = [
        ResultValue::Inet("127.0.0.1".parse().unwrap()),
        ResultValue::Varchar("dc1".into()),
        ResultValue::Uuid("fa74d7ec-1223-472b-97de-04a32ccdb70b".parse().unwrap()),
        ResultValue::Null,
        ResultValue::Varchar("rack1".into()),
        ResultValue::Varchar("4.0.6".into()),
        ResultValue::Null,
        // schema_version is non deterministic so we cant assert on it.
        ResultValue::Any,
        // Unfortunately token generation appears to be non-deterministic but we can at least assert that
        // there are 128 tokens per node
        ResultValue::Set(std::iter::repeat(ResultValue::Any).take(3 * 128).collect()),
    ];

    let all_columns = "peer, data_center, host_id, preferred_ip, rack, release_version, rpc_address, schema_version, tokens";
    assert_query_result(
        connection,
        "SELECT * FROM system.peers;",
        &[&star_results1, &star_results2],
    )
    .await;
    assert_query_result(
        connection,
        &format!("SELECT {all_columns} FROM system.peers;"),
        &[&star_results1, &star_results2],
    )
    .await;
    assert_query_result(
        connection,
        &format!("SELECT {all_columns}, {all_columns} FROM system.peers;"),
        &[
            &[star_results1.as_slice(), star_results1.as_slice()].concat(),
            &[star_results2.as_slice(), star_results2.as_slice()].concat(),
        ],
    )
    .await;
}

async fn test_rewrite_system_peers_v2_dummy_peers(connection: &CassandraConnection) {
    let star_results1 = [
        ResultValue::Inet("127.0.0.1".parse().unwrap()),
        ResultValue::Int(7000),
        ResultValue::Varchar("dc1".into()),
        ResultValue::Uuid("3c3c4e2d-ba74-4f76-b52e-fb5bcee6a9f4".parse().unwrap()),
        ResultValue::Inet("127.0.0.1".into()),
        ResultValue::Int(9042),
        ResultValue::Null,
        ResultValue::Null,
        ResultValue::Varchar("rack1".into()),
        ResultValue::Varchar("4.0.6".into()),
        // schema_version is non deterministic so we cant assert on it.
        ResultValue::Any,
        // Unfortunately token generation appears to be non-deterministic but we can at least assert that
        // there are 128 tokens per node
        ResultValue::Set(std::iter::repeat(ResultValue::Any).take(3 * 128).collect()),
    ];
    let star_results2 = [
        ResultValue::Inet("127.0.0.1".parse().unwrap()),
        ResultValue::Int(7000),
        ResultValue::Varchar("dc1".into()),
        ResultValue::Uuid("fa74d7ec-1223-472b-97de-04a32ccdb70b".parse().unwrap()),
        ResultValue::Inet("127.0.0.1".parse().unwrap()),
        ResultValue::Int(9042),
        ResultValue::Null,
        ResultValue::Null,
        ResultValue::Varchar("rack1".into()),
        ResultValue::Varchar("4.0.6".into()),
        // schema_version is non deterministic so we cant assert on it.
        ResultValue::Any,
        // Unfortunately token generation appears to be non-deterministic but we can at least assert that
        // there are 128 tokens per node
        ResultValue::Set(std::iter::repeat(ResultValue::Any).take(3 * 128).collect()),
    ];

    let all_columns = "peer, peer_port, data_center, host_id, native_address, native_port, preferred_ip, preferred_port, rack, release_version, schema_version, tokens";
    assert_query_result(
        connection,
        "SELECT * FROM system.peers_v2;",
        &[&star_results1, &star_results2],
    )
    .await;
    assert_query_result(
        connection,
        &format!("SELECT {all_columns} FROM system.peers_v2;"),
        &[&star_results1, &star_results2],
    )
    .await;
    assert_query_result(
        connection,
        &format!("SELECT {all_columns}, {all_columns} FROM system.peers_v2;"),
        &[
            &[star_results1.as_slice(), star_results1.as_slice()].concat(),
            &[star_results2.as_slice(), star_results2.as_slice()].concat(),
        ],
    )
    .await;
}

async fn test_rewrite_system_local(connection: &CassandraConnection) {
    let star_results = [
        ResultValue::Varchar("local".into()),
        ResultValue::Varchar("COMPLETED".into()),
        ResultValue::Inet("127.0.0.1".parse().unwrap()),
        ResultValue::Int(7000),
        ResultValue::Varchar("TestCluster".into()),
        ResultValue::Varchar("3.4.5".into()),
        ResultValue::Varchar("dc1".into()),
        // gossip_generation is non deterministic cant assert on it
        ResultValue::Any,
        ResultValue::Uuid("2dd022d6-2937-4754-89d6-02d2933a8f7a".parse().unwrap()),
        ResultValue::Inet("127.0.0.1".parse().unwrap()),
        ResultValue::Int(7000),
        ResultValue::Varchar("5".into()),
        ResultValue::Varchar("org.apache.cassandra.dht.Murmur3Partitioner".into()),
        ResultValue::Varchar("rack1".into()),
        ResultValue::Varchar("4.0.6".into()),
        ResultValue::Inet("0.0.0.0".parse().unwrap()),
        ResultValue::Int(9042),
        // schema_version is non deterministic so we cant assert on it.
        ResultValue::Any,
        // Unfortunately token generation appears to be non-deterministic but we can at least assert that
        // there are 128 tokens per node
        ResultValue::Set(std::iter::repeat(ResultValue::Any).take(3 * 128).collect()),
        // truncated_at is non deterministic so we cant assert on it.
        ResultValue::Any,
    ];

    let all_columns =
        "key, bootstrapped, broadcast_address, broadcast_port, cluster_name, cql_version, data_center,
        gossip_generation, host_id, listen_address, listen_port, native_protocol_version, partitioner, rack,
        release_version, rpc_address, rpc_port, schema_version, tokens, truncated_at";

    assert_query_result(connection, "SELECT * FROM system.local;", &[&star_results]).await;
    assert_query_result(
        connection,
        &format!("SELECT {all_columns} FROM system.local;"),
        &[&star_results],
    )
    .await;
    assert_query_result(
        connection,
        &format!("SELECT {all_columns}, {all_columns} FROM system.local;"),
        &[&[star_results.as_slice(), star_results.as_slice()].concat()],
    )
    .await;
}

pub async fn test(connection: &CassandraConnection) {
    test_rewrite_system_local(connection).await;
    test_rewrite_system_peers(connection).await;
    test_rewrite_system_peers_v2(connection).await;
}

pub async fn test_dummy_peers(connection: &CassandraConnection) {
    test_rewrite_system_local(connection).await;
    test_rewrite_system_peers_dummy_peers(connection).await;
    test_rewrite_system_peers_v2_dummy_peers(connection).await;
}

pub async fn test_topology_task(ca_path: Option<&str>) {
    let nodes = run_topology_task(ca_path).await;

    assert_eq!(nodes.len(), 3);
    let mut possible_addresses: Vec<SocketAddr> = vec![
        "172.16.1.2:9042".parse().unwrap(),
        "172.16.1.3:9042".parse().unwrap(),
        "172.16.1.4:9042".parse().unwrap(),
    ];
    for node in &nodes {
        let address_index = possible_addresses
            .iter()
            .position(|x| *x == node.address)
            .expect("Node did not contain a unique expected address");
        possible_addresses.remove(address_index);

        assert_eq!(node.rack, "rack1");
        assert_eq!(node._tokens.len(), 128);
    }
}
