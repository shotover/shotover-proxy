use crate::helpers::cassandra::{assert_query_result, CassandraConnection, ResultValue};

async fn test_rewrite_system_peers_dummy_peers(connection: &CassandraConnection) {
    let star_results1 = [
        ResultValue::Inet("127.0.0.1".parse().unwrap()),
        ResultValue::Varchar("dc1".into()),
        ResultValue::Uuid("3c3c4e2d-ba74-4f76-b52e-fb5bcee6a9f4".parse().unwrap()),
        ResultValue::Inet("255.255.255.255".into()),
        ResultValue::Varchar("rack1".into()),
        ResultValue::Varchar("3.11.13".into()),
        ResultValue::Inet("255.255.255.255".into()),
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
        ResultValue::Inet("255.255.255.255".into()),
        ResultValue::Varchar("rack1".into()),
        ResultValue::Varchar("3.11.13".into()),
        ResultValue::Inet("255.255.255.255".into()),
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

async fn test_rewrite_system_local(connection: &CassandraConnection) {
    let star_results = [
        ResultValue::Varchar("local".into()),
        ResultValue::Varchar("COMPLETED".into()),
        ResultValue::Inet("127.0.0.1".parse().unwrap()),
        ResultValue::Varchar("TestCluster".into()),
        ResultValue::Varchar("3.4.4".into()),
        ResultValue::Varchar("dc1".into()),
        // gossip_generation is non deterministic cant assert on it
        ResultValue::Any,
        ResultValue::Uuid("2dd022d6-2937-4754-89d6-02d2933a8f7a".parse().unwrap()),
        ResultValue::Inet("127.0.0.1".parse().unwrap()),
        ResultValue::Varchar("4".into()),
        ResultValue::Varchar("org.apache.cassandra.dht.Murmur3Partitioner".into()),
        ResultValue::Varchar("rack1".into()),
        ResultValue::Varchar("3.11.13".into()),
        ResultValue::Inet("0.0.0.0".parse().unwrap()),
        // schema_version is non deterministic so we cant assert on it.
        ResultValue::Any,
        // thrift_version isnt used anymore so I dont really care what it maps to
        ResultValue::Any,
        // Unfortunately token generation appears to be non-deterministic but we can at least assert that
        // there are 128 tokens per node
        ResultValue::Set(std::iter::repeat(ResultValue::Any).take(3 * 128).collect()),
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

pub async fn test_dummy_peers(connection: &CassandraConnection) {
    test_rewrite_system_local(connection).await;
    test_rewrite_system_peers_dummy_peers(connection).await;
}
