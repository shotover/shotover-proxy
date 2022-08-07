use crate::helpers::cassandra::{CassandraConnection, ResultValue};
use cdrs_tokio::authenticators::StaticPasswordAuthenticatorProvider;
use cdrs_tokio::cluster::session::{Session, SessionBuilder, TcpSessionBuilder};
use cdrs_tokio::cluster::{NodeTcpConfigBuilder, TcpConnectionManager};
use cdrs_tokio::load_balancing::RoundRobinLoadBalancingStrategy;
use cdrs_tokio::transport::TransportTcp;
use std::sync::Arc;

pub struct DirectConnections {
    connections: Vec<CassandraConnection>,
}

impl DirectConnections {
    pub fn new() -> Self {
        DirectConnections {
            connections: vec![CassandraConnection::new("127.0.0.1", 9043)],
        }
    }

    pub fn new_cluster() -> Self {
        DirectConnections {
            connections: vec![
                // TODO: uhhh how do we make this connect directly to this node only
                CassandraConnection::new("172.16.1.2", 9042),
                CassandraConnection::new("172.16.1.3", 9042),
                CassandraConnection::new("172.16.1.4", 9042),
            ],
        }
    }

    pub fn wait_for_function_to_propogate(&self, search_keyspace: &str, search_name: &str) {
        let mut fully_propogated = false;
        while !fully_propogated {
            fully_propogated = self.connections.iter().all(|connection| {
                connection
                    .execute("SELECT * FROM system_schema.functions;")
                    .iter()
                    .any(|function| match function.as_slice() {
                        [ResultValue::Varchar(keyspace), ResultValue::Varchar(name), ..] => {
                            search_keyspace == keyspace && search_name == name
                        }
                        _ => false,
                    })
            });
            tracing::error!("{}", fully_propogated);
        }
    }
}

type Connection = Session<
    TransportTcp,
    TcpConnectionManager,
    RoundRobinLoadBalancingStrategy<TransportTcp, TcpConnectionManager>,
>;

async fn cdrs_direct_connection() -> Connection {
    TcpSessionBuilder::new(
        RoundRobinLoadBalancingStrategy::new(),
        NodeTcpConfigBuilder::new()
            .with_contact_point("127.0.0.1:9042".into())
            .with_authenticator_provider(Arc::new(StaticPasswordAuthenticatorProvider::new(
                "cassandra",
                "cassandra",
            )))
            .build()
            .await
            .unwrap(),
    )
    .build()
    .unwrap()
}
