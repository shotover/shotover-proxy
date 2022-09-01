use crate::helpers::cassandra::{assert_query_result, CassandraConnection, ResultValue};
use cassandra_protocol::frame::Version;
use shotover_proxy::frame::{CassandraFrame, CassandraOperation, Frame};
use shotover_proxy::message::Message;
use shotover_proxy::tls::{TlsConnector, TlsConnectorConfig};
use shotover_proxy::transforms::cassandra::sink_cluster::{
    node::{CassandraNode, ConnectionFactory},
    topology::{create_topology_task, TaskConnectionInfo},
};
use std::net::IpAddr;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};

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

pub async fn test(connection: &CassandraConnection) {
    test_rewrite_system_local(connection).await;
    test_rewrite_system_peers(connection).await;
}

pub async fn test_dummy_peers(connection: &CassandraConnection) {
    test_rewrite_system_local(connection).await;
    test_rewrite_system_peers_dummy_peers(connection).await;
}

pub async fn test_topology_task(ca_path: Option<&str>) {
    let nodes = run_topology_task(ca_path).await;

    assert_eq!(nodes.len(), 3);
    let mut possible_addresses: Vec<IpAddr> = vec![
        "172.16.1.2".parse().unwrap(),
        "172.16.1.3".parse().unwrap(),
        "172.16.1.4".parse().unwrap(),
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
pub async fn run_topology_task(ca_path: Option<&str>) -> Vec<CassandraNode> {
    let nodes_shared = Arc::new(RwLock::new(vec![]));
    let (task_handshake_tx, task_handshake_rx) = mpsc::channel(1);
    let tls = ca_path.map(|ca_path| {
        TlsConnector::new(TlsConnectorConfig {
            certificate_authority_path: ca_path.into(),
            certificate_path: None,
            private_key_path: None,
        })
        .unwrap()
    });

    let mut connection_factory = ConnectionFactory::new(tls);
    for message in create_handshake() {
        connection_factory.push_handshake_message(message);
    }

    create_topology_task(nodes_shared.clone(), task_handshake_rx, "dc1".to_string());

    // Give the handshake task a hardcoded handshake.
    // Normally the handshake is the handshake that the client gave shotover.
    task_handshake_tx
        .send(TaskConnectionInfo {
            connection_factory: connection_factory.clone(),
            address: "172.16.1.2:9042".parse().unwrap(),
        })
        .await
        .unwrap();

    // keep attempting to read the nodes list until it is populated.
    let mut nodes = vec![];
    let mut tries = 0;
    while nodes.is_empty() {
        nodes = nodes_shared.read().await.clone();
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;

        if tries > 2000 {
            panic!("Ran out of retries for the topology task to write the nodes list");
        }
        tries += 1;
    }
    nodes
}

fn create_handshake() -> Vec<Message> {
    vec![
        Message::from_frame(Frame::Cassandra(CassandraFrame {
            version: Version::V4,
            stream_id: 64,
            tracing_id: None,
            warnings: vec![],
            operation: CassandraOperation::Startup(b"\0\x01\0\x0bCQL_VERSION\0\x053.0.0".to_vec()),
        })),
        Message::from_frame(Frame::Cassandra(CassandraFrame {
            version: Version::V4,
            stream_id: 128,
            tracing_id: None,
            warnings: vec![],
            operation: CassandraOperation::AuthResponse(
                b"\0\0\0\x14\0cassandra\0cassandra".to_vec(),
            ),
        })),
    ]
}
