use cassandra_protocol::frame::Version;
use cassandra_protocol::frame::message_startup::BodyReqStartup;
use shotover::frame::{CassandraFrame, CassandraOperation, Frame, cassandra::Tracing};
use shotover::message::Message;
use shotover::tls::{TlsConnector, TlsConnectorConfig};
use shotover::transforms::cassandra::sink_cluster::{
    node::{CassandraNode, ConnectionFactory},
    topology::{TaskConnectionInfo, create_topology_task},
};
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use tokio::time::timeout;

pub mod multi_rack;
pub mod single_rack_v3;
pub mod single_rack_v4;

pub async fn run_topology_task(ca_path: Option<&str>, port: Option<u32>) -> Vec<CassandraNode> {
    let port = port.unwrap_or(9042);
    let (nodes_tx, mut nodes_rx) = watch::channel(vec![]);
    let (keyspaces_tx, _keyspaces_rx) = watch::channel(HashMap::new());
    let (task_handshake_tx, task_handshake_rx) = mpsc::channel(1);
    let tls = ca_path.map(|ca_path| {
        TlsConnector::new(&TlsConnectorConfig {
            certificate_authority_path: ca_path.into(),
            certificate_path: None,
            private_key_path: None,
            verify_hostname: false,
        })
        .unwrap()
    });

    let mut connection_factory = ConnectionFactory::new(Duration::from_secs(3), None, tls);
    for message in create_handshake() {
        connection_factory.push_handshake_message(message);
    }

    create_topology_task(
        nodes_tx,
        keyspaces_tx,
        task_handshake_rx,
        "datacenter1".to_string(),
    );

    // Give the handshake task a hardcoded handshake.
    // Normally the handshake is the handshake that the client gave shotover.
    task_handshake_tx
        .send(TaskConnectionInfo {
            connection_factory: connection_factory.clone(),
            address: format!("172.16.1.2:{port}").parse().unwrap(),
        })
        .await
        .unwrap();

    timeout(Duration::from_secs(30), nodes_rx.changed())
        .await
        .unwrap()
        .unwrap();
    let nodes = nodes_rx.borrow().clone();
    nodes
}

fn create_handshake() -> Vec<Message> {
    let mut startup_body: HashMap<String, String> = HashMap::new();
    startup_body.insert("CQL_VERSION".into(), "3.0.0".into());

    vec![
        Message::from_frame(Frame::Cassandra(CassandraFrame {
            version: Version::V4,
            stream_id: 64,
            tracing: Tracing::Request(false),
            warnings: vec![],
            operation: CassandraOperation::Startup(BodyReqStartup { map: startup_body }),
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
