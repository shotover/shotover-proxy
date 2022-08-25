use crate::cassandra_int_tests::cluster::run_topology_task;
use crate::helpers::cassandra::{assert_query_result, CassandraConnection, ResultValue};
use std::net::IpAddr;

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

async fn test_rewrite_system_local(connection: &CassandraConnection) {
    let star_results = [
        ResultValue::Varchar("local".into()),
        ResultValue::Varchar("COMPLETED".into()),
        // broadcast address is non-deterministic because we dont know which node this will be
        ResultValue::Any,
        ResultValue::Varchar("TestCluster".into()),
        ResultValue::Varchar("3.4.4".into()),
        ResultValue::Varchar("dc1".into()),
        // gossip_generation is non deterministic cant assert on it
        ResultValue::Any,
        // host_id is non-deterministic because we dont know which node this will be
        ResultValue::Any,
        ResultValue::Inet("127.0.0.1".parse().unwrap()),
        ResultValue::Varchar("4".into()),
        ResultValue::Varchar("org.apache.cassandra.dht.Murmur3Partitioner".into()),
        // rack is non-deterministic because we dont know which node this will be
        ResultValue::Any,
        ResultValue::Varchar("3.11.13".into()),
        ResultValue::Inet("0.0.0.0".parse().unwrap()),
        // schema_version is non deterministic so we cant assert on it.
        ResultValue::Any,
        // thrift_version isnt used anymore so I dont really care what it maps to
        ResultValue::Any,
        // Unfortunately token generation appears to be non-deterministic but we can at least assert that
        // there are 128 tokens per node
        ResultValue::Set(std::iter::repeat(ResultValue::Any).take(128).collect()),
        ResultValue::Map(vec![]),
    ];

    let all_columns =
        "key, bootstrapped, broadcast_address, cluster_name, cql_version, data_center,
        gossip_generation, host_id, listen_address, native_protocol_version, partitioner, rack,
        release_version, rpc_address, schema_version, thrift_version, tokens, truncated_at";

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
}

pub async fn test_topology_task(ca_path: Option<&str>) {
    let nodes = run_topology_task(ca_path).await;

    assert_eq!(nodes.len(), 3);
    let mut possible_addresses: Vec<IpAddr> = vec![
        "172.16.1.2".parse().unwrap(),
        "172.16.1.3".parse().unwrap(),
        "172.16.1.4".parse().unwrap(),
    ];
    let mut possible_racks: Vec<&str> = vec!["rack1", "rack2", "rack3"];
    for node in &nodes {
        let address_index = possible_addresses
            .iter()
            .position(|x| *x == node.address)
            .expect("Node did not contain a unique expected address");
        possible_addresses.remove(address_index);

        let rack_index = possible_racks
            .iter()
            .position(|x| *x == node._rack)
            .expect("Node did not contain a unique expected rack");
        possible_racks.remove(rack_index);

        assert_eq!(node._tokens.len(), 128);
    }
}
