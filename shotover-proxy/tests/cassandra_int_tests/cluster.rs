use cassandra_protocol::frame::Version;
use shotover_proxy::frame::{CassandraFrame, CassandraOperation, Frame};
use shotover_proxy::message::Message;
use shotover_proxy::tls::{TlsConnector, TlsConnectorConfig};
use shotover_proxy::transforms::cassandra::sink_cluster::{
    node::{CassandraNode, ConnectionFactory},
    topology::{create_topology_task, TaskConnectionInfo},
};
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};

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
