use cassandra_protocol::frame::Version;
use shotover_proxy::frame::{cassandra::Tracing, CassandraFrame, CassandraOperation, Frame};
use shotover_proxy::message::Message;
use shotover_proxy::tls::{TlsConnector, TlsConnectorConfig};
use shotover_proxy::transforms::cassandra::sink_cluster::{
    node::{CassandraNode, ConnectionFactory},
    topology::{create_topology_task, TaskConnectionInfo},
};
use tokio::sync::{mpsc, watch};

pub async fn run_topology_task(ca_path: Option<&str>, port: Option<u32>) -> Vec<CassandraNode> {
    let port = port.unwrap_or(9042);
    let (nodes_tx, mut nodes_rx) = watch::channel(vec![]);
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

    create_topology_task(nodes_tx, task_handshake_rx, "dc1".to_string());

    // Give the handshake task a hardcoded handshake.
    // Normally the handshake is the handshake that the client gave shotover.
    task_handshake_tx
        .send(TaskConnectionInfo {
            connection_factory: connection_factory.clone(),
            address: format!("172.16.1.2:{port}").parse().unwrap(),
        })
        .await
        .unwrap();

    nodes_rx.changed().await.unwrap();
    let nodes = nodes_rx.borrow().clone();
    nodes
}

fn create_handshake() -> Vec<Message> {
    vec![
        Message::from_frame(Frame::Cassandra(CassandraFrame {
            version: Version::V4,
            stream_id: 64,
            tracing: Tracing::Request(false),
            warnings: vec![],
            operation: CassandraOperation::Startup(b"\0\x01\0\x0bCQL_VERSION\0\x053.0.0".to_vec()),
        })),
        Message::from_frame(Frame::Cassandra(CassandraFrame {
            version: Version::V4,
            stream_id: 128,
            tracing: Tracing::Request(false),
            warnings: vec![],
            operation: CassandraOperation::AuthResponse(
                b"\0\0\0\x14\0cassandra\0cassandra".to_vec(),
            ),
        })),
    ]
}
