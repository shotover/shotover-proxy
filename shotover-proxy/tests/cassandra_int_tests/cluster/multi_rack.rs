use crate::cassandra_int_tests::cluster::run_topology_task;
use pretty_assertions::assert_eq;
use std::net::SocketAddr;
use test_helpers::{
    connection::cassandra::{CassandraConnection, ResultValue, assert_query_result},
    metrics::get_metrics_value,
};

async fn test_rewrite_system_peers(connection: &CassandraConnection) {
    let star_results = [
        // peer is non-determistic because we dont know which node this will be
        ResultValue::Any,
        ResultValue::Varchar("datacenter1".into()),
        // host_id is non-determistic because we dont know which node this will be
        ResultValue::Any,
        ResultValue::Null,
        // rack is non-determistic because we dont know which node this will be
        ResultValue::Any,
        ResultValue::Varchar("4.0.6".into()),
        // rpc_address is non-determistic because we dont know which node this will be
        ResultValue::Any,
        // schema_version is non deterministic so we cant assert on it.
        ResultValue::Any,
        // Unfortunately token generation appears to be non-deterministic but we can at least assert that
        // there are 128 tokens per node
        ResultValue::Set(std::iter::repeat(ResultValue::Any).take(128).collect()),
    ];

    let all_columns = "peer, data_center, host_id, preferred_ip, rack, release_version, rpc_address, schema_version, tokens";
    assert_query_result(
        connection,
        "SELECT * FROM system.peers;",
        &[&star_results, &star_results],
    )
    .await;
    assert_query_result(
        connection,
        &format!("SELECT {all_columns} FROM system.peers;"),
        &[&star_results, &star_results],
    )
    .await;
    assert_query_result(
        connection,
        &format!("SELECT {all_columns}, {all_columns} FROM system.peers;"),
        &[
            &[star_results.as_slice(), star_results.as_slice()].concat(),
            &[star_results.as_slice(), star_results.as_slice()].concat(),
        ],
    )
    .await;
}

async fn test_rewrite_system_peers_v2(connection: &CassandraConnection) {
    let star_results = [
        // peer is non-determistic because we dont know which node this will be
        ResultValue::Any,
        ResultValue::Int(7000),
        ResultValue::Varchar("datacenter1".into()),
        // host_id is non-determistic because we dont know which node this will be
        ResultValue::Any,
        // native_address is non-determistic because we dont know which node this will be
        ResultValue::Any,
        ResultValue::Int(9042),
        ResultValue::Null,
        ResultValue::Null,
        // rack is non-determistic because we dont know which node this will be
        ResultValue::Any,
        ResultValue::Varchar("4.0.6".into()),
        // schema_version is non deterministic so we cant assert on it.
        ResultValue::Any,
        // Unfortunately token generation appears to be non-deterministic but we can at least assert that
        // there are 128 tokens per node
        ResultValue::Set(std::iter::repeat(ResultValue::Any).take(128).collect()),
    ];

    let all_columns = "peer, peer_port, data_center, host_id, native_address, native_port, preferred_ip, preferred_port, rack, release_version, schema_version, tokens";
    assert_query_result(
        connection,
        "SELECT * FROM system.peers_v2;",
        &[&star_results, &star_results],
    )
    .await;
    assert_query_result(
        connection,
        &format!("SELECT {all_columns} FROM system.peers_v2;"),
        &[&star_results, &star_results],
    )
    .await;
    assert_query_result(
        connection,
        &format!("SELECT {all_columns}, {all_columns} FROM system.peers_v2;"),
        &[
            &[star_results.as_slice(), star_results.as_slice()].concat(),
            &[star_results.as_slice(), star_results.as_slice()].concat(),
        ],
    )
    .await;
}

async fn test_rewrite_system_local(connection: &CassandraConnection) {
    let star_results = [
        ResultValue::Varchar("local".into()),
        ResultValue::Varchar("COMPLETED".into()),
        // broadcast address is non-deterministic because we dont know which shotover node this will be
        ResultValue::Any,
        ResultValue::Int(7000),
        ResultValue::Varchar("TestCluster".into()),
        ResultValue::Varchar("3.4.5".into()),
        ResultValue::Varchar("datacenter1".into()),
        // gossip_generation is non deterministic cant assert on it
        ResultValue::Any,
        // host_id is non-deterministic because we dont know which shotover node this will be
        ResultValue::Any,
        // listen_address is non-deterministic because we dont know which shotover node this will be
        ResultValue::Any,
        ResultValue::Int(7000),
        ResultValue::Varchar("5".into()),
        ResultValue::Varchar("org.apache.cassandra.dht.Murmur3Partitioner".into()),
        // rack is non-deterministic because we dont know which shotover node this will be
        ResultValue::Any,
        ResultValue::Varchar("4.0.6".into()),
        // rpc_address is non deterministic so we cant assert on it
        ResultValue::Any,
        ResultValue::Int(9042),
        // schema_version is non deterministic so we cant assert on it.
        ResultValue::Any,
        // Unfortunately token generation appears to be non-deterministic but we can at least assert that
        // there are 128 tokens per node
        ResultValue::Set(std::iter::repeat(ResultValue::Any).take(128).collect()),
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

    let out_of_rack_request = get_metrics_value(
        "shotover_out_of_rack_requests_count{chain=\"cassandra\",transform=\"CassandraSinkCluster\"}",
    )
    .await;
    assert_eq!(out_of_rack_request, "0");
}

pub async fn test_topology_task(
    ca_path: Option<&str>,
    mut expected_nodes: Vec<(SocketAddr, &'static str)>,
    token_count: usize,
) {
    let nodes = run_topology_task(ca_path, None).await;

    assert_eq!(nodes.len(), expected_nodes.len());
    for node in &nodes {
        let address_index = expected_nodes
            .iter()
            .position(|(address, rack)| *address == node.address && *rack == node.rack)
            .unwrap_or_else(|| {
                panic!("An expected node was missing from the list of actual nodes. Expected nodes:{expected_nodes:#?}\nactual nodes:{nodes:#?}")
            });
        expected_nodes.remove(address_index);

        assert_eq!(node.tokens.len(), token_count);
        assert!(node.is_up);
    }
}
