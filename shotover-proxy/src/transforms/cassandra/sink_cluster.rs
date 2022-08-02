use super::connection::CassandraConnection;
use crate::codec::cassandra::CassandraCodec;
use crate::concurrency::FuturesOrdered;
use crate::error::ChainResponse;
use crate::frame::{CassandraFrame, CassandraOperation, CassandraResult, Frame};
use crate::message::{Message, MessageValue, Messages};
use crate::tls::{TlsConnector, TlsConnectorConfig};
use crate::transforms::{Transform, Transforms, Wrapper};
use anyhow::Result;
use async_trait::async_trait;
use cql3_parser::cassandra_statement::CassandraStatement;
use cql3_parser::common::{FQName, Identifier};
use cql3_parser::select::SelectElement;
use metrics::{register_counter, Counter};
use rand::prelude::*;
use serde::Deserialize;
use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, RwLock};

#[derive(Deserialize, Debug, Clone)]
pub struct CassandraSinkClusterConfig {
    pub first_contact_points: Vec<String>,
    pub data_center: String,
    pub tls: Option<TlsConnectorConfig>,
    pub read_timeout: Option<u64>,
}

impl CassandraSinkClusterConfig {
    pub async fn get_transform(&self, chain_name: String) -> Result<Transforms> {
        let tls = self.tls.clone().map(TlsConnector::new).transpose()?;
        Ok(Transforms::CassandraSinkCluster(CassandraSinkCluster::new(
            self.first_contact_points.clone(),
            chain_name,
            self.data_center.clone(),
            tls,
            self.read_timeout,
        )))
    }
}

pub struct CassandraSinkCluster {
    contact_points: Vec<String>,
    contact_point_handshake: Vec<Message>,
    contact_point_handshake_complete: bool,
    contact_point_connection: Option<CassandraConnection>,
    chain_name: String,
    failed_requests: Counter,
    tls: Option<TlsConnector>,
    pushed_messages_tx: Option<mpsc::UnboundedSender<Messages>>,
    read_timeout: Option<Duration>,
    peer_table: FQName,
    data_center: String,
    nodes: Vec<CassandraNode>,
    nodes_shared: Arc<RwLock<Vec<CassandraNode>>>,
    rng: SmallRng,
}

impl Clone for CassandraSinkCluster {
    fn clone(&self) -> Self {
        CassandraSinkCluster {
            contact_points: self.contact_points.clone(),
            contact_point_handshake: vec![],
            contact_point_handshake_complete: false,
            contact_point_connection: None,
            chain_name: self.chain_name.clone(),
            tls: self.tls.clone(),
            failed_requests: self.failed_requests.clone(),
            pushed_messages_tx: None,
            read_timeout: self.read_timeout,
            peer_table: self.peer_table.clone(),
            data_center: self.data_center.clone(),
            nodes: vec![],
            nodes_shared: self.nodes_shared.clone(),
            rng: SmallRng::from_rng(rand::thread_rng()).unwrap(),
        }
    }
}

impl CassandraSinkCluster {
    pub fn new(
        contact_points: Vec<String>,
        chain_name: String,
        data_center: String,
        tls: Option<TlsConnector>,
        timeout: Option<u64>,
    ) -> CassandraSinkCluster {
        let failed_requests = register_counter!("failed_requests", "chain" => chain_name.clone(), "transform" => "CassandraSinkCluster");
        let receive_timeout = timeout.map(Duration::from_secs);

        CassandraSinkCluster {
            contact_points,
            contact_point_handshake: vec![],
            contact_point_handshake_complete: false,
            contact_point_connection: None,
            chain_name,
            failed_requests,
            tls,
            pushed_messages_tx: None,
            read_timeout: receive_timeout,
            peer_table: FQName::new("system", "peers"),
            data_center,
            nodes: vec![],
            nodes_shared: Arc::new(RwLock::new(vec![])),
            rng: SmallRng::from_rng(rand::thread_rng()).unwrap(),
        }
    }
}

impl CassandraSinkCluster {
    async fn send_message(&mut self, mut messages: Messages) -> ChainResponse {
        // Create the initial connection.
        // Messages will be sent through this connection until we have extracted the handshake and list of nodes
        if self.contact_point_connection.is_none() {
            let random_point = self
                .contact_points
                .choose_mut(&mut self.rng)
                .unwrap()
                .clone();
            self.contact_point_connection = Some(
                CassandraConnection::new(
                    random_point,
                    CassandraCodec::new(),
                    self.tls.clone(),
                    self.pushed_messages_tx.clone(),
                )
                .await?,
            );
        }

        // Attempt to populate nodes list if we still dont have one yet
        if self.nodes.is_empty() {
            let nodes_shared = self.nodes_shared.read().await;
            self.nodes.extend(nodes_shared.iter().cloned());
        }

        let system_peers_index = self.get_system_peers_index(&mut messages);

        for message in &messages {
            if let Some(last) = self.contact_point_handshake.last_mut() {
                if let Some(Frame::Cassandra(CassandraFrame {
                    operation: CassandraOperation::AuthResponse(_),
                    ..
                })) = last.frame()
                {
                    self.contact_point_handshake_complete = true;
                    break;
                }
            }
            self.contact_point_handshake.push(message.clone());
        }

        let mut responses_future = FuturesOrdered::new();
        for message in messages {
            let (return_chan_tx, return_chan_rx) = oneshot::channel();
            if self.nodes.is_empty() || !self.contact_point_handshake_complete {
                self.contact_point_connection.as_mut().unwrap()
            } else {
                let random_node = self.nodes.choose_mut(&mut self.rng).unwrap();
                random_node
                    .get_connection(
                        &self.contact_point_handshake,
                        &self.tls,
                        &self.pushed_messages_tx,
                    )
                    .await?
            }
            .send(message, return_chan_tx)?;

            responses_future.push(return_chan_rx)
        }

        let mut responses =
            super::connection::receive(self.read_timeout, &self.failed_requests, responses_future)
                .await?;

        if let Some(query_occured) = system_peers_index {
            if let Some(response) = responses.get_mut(query_occured.message_index) {
                let new_nodes = get_nodes_from_system_peers(
                    response,
                    query_occured.alias_to_name,
                    &self.data_center,
                );
                if new_nodes.len() > 1 {
                    self.nodes.clear();
                    self.nodes.extend(new_nodes.iter().cloned());

                    let mut write_lock = self.nodes_shared.write().await;
                    let expensive_drop = std::mem::replace(&mut *write_lock, new_nodes);

                    // Make sure to drop write_lock before the expensive_drop which may have to perform many deallocations.
                    std::mem::drop(write_lock);
                    std::mem::drop(expensive_drop);
                }
            }
        }

        Ok(responses)
    }

    fn get_system_peers_index(&self, messages: &mut [Message]) -> Option<QueryOccured> {
        let mut system_peers_index: Option<QueryOccured> = None;
        for (message_index, message) in messages.iter_mut().enumerate() {
            if let Some(Frame::Cassandra(cassandra)) = message.frame() {
                // No need to handle Batch as selects can only occur on Query
                if let CassandraOperation::Query { query, .. } = &cassandra.operation {
                    if let CassandraStatement::Select(select) = query.as_ref() {
                        if select.where_clause.is_empty() && select.table_name == self.peer_table {
                            let mut alias_to_name = HashMap::new();
                            for select_element in &select.columns {
                                match select_element {
                                    SelectElement::Star => {
                                        // Always overwrite, give priority to * entries
                                        return Some(QueryOccured {
                                            message_index,
                                            alias_to_name,
                                        });
                                    }
                                    SelectElement::Column(ident) => {
                                        if let Some(alias) = &ident.alias {
                                            alias_to_name.insert(alias.clone(), ident.name.clone());
                                        }
                                    }
                                    _ => {}
                                }
                            }
                            // Give priority to existing entry in the hope that its a * entry
                            if system_peers_index.is_none() {
                                system_peers_index = Some(QueryOccured {
                                    message_index,
                                    alias_to_name,
                                })
                            }
                        }
                    }
                }
            }
        }
        system_peers_index
    }
}

fn get_nodes_from_system_peers(
    response: &mut Message,
    mut alias_to_name: HashMap<Identifier, Identifier>,
    config_data_center: &str,
) -> Vec<CassandraNode> {
    let mut new_nodes = vec![];
    fn ident(name: &str, map: &mut HashMap<Identifier, Identifier>) -> Identifier {
        let ident = Identifier::Unquoted(name.into());
        map.remove(&ident).unwrap_or(ident)
    }
    let peer_ident = ident("peer", &mut alias_to_name);
    let rack_ident = ident("rack", &mut alias_to_name);
    let data_center_ident = ident("data_center", &mut alias_to_name);
    let tokens_ident = ident("tokens", &mut alias_to_name);

    if let Some(Frame::Cassandra(frame)) = response.frame() {
        // CassandraOperation::Error(_) is another possible case, we should silently ignore such cases
        if let CassandraOperation::Result(CassandraResult::Rows {
            value: MessageValue::Rows(rows),
            metadata,
        }) = &mut frame.operation
        {
            for row in rows.iter() {
                let mut address = None;
                let mut rack = None;
                let mut data_center = None;
                let mut tokens = vec![];
                for (i, col) in metadata.col_specs.iter().enumerate() {
                    let ident = Identifier::parse(&col.name);
                    if ident == peer_ident {
                        if let Some(MessageValue::Inet(value)) = row.get(i) {
                            address = Some(*value);
                        }
                    } else if ident == rack_ident {
                        if let Some(MessageValue::Varchar(value)) = row.get(i) {
                            rack = Some(value.clone());
                        }
                    } else if ident == data_center_ident {
                        if let Some(MessageValue::Varchar(value)) = row.get(i) {
                            data_center = Some(value.clone());
                        }
                    } else if ident == tokens_ident {
                        if let Some(MessageValue::List(list)) = row.get(i) {
                            tokens = list
                                .iter()
                                .filter_map(|x| match x {
                                    MessageValue::Varchar(a) => Some(a.clone()),
                                    _ => None,
                                })
                                .collect();
                        }
                    }
                }
                if let (Some(address), Some(rack), Some(data_center)) = (address, rack, data_center)
                {
                    if data_center == config_data_center {
                        new_nodes.push(CassandraNode {
                            address,
                            _rack: rack,
                            _tokens: tokens,
                            outbound: None,
                        });
                    }
                }
            }
        }
    }
    new_nodes
}

#[async_trait]
impl Transform for CassandraSinkCluster {
    async fn transform<'a>(&'a mut self, message_wrapper: Wrapper<'a>) -> ChainResponse {
        self.send_message(message_wrapper.messages).await
    }

    fn is_terminating(&self) -> bool {
        true
    }

    fn add_pushed_messages_tx(&mut self, pushed_messages_tx: mpsc::UnboundedSender<Messages>) {
        self.pushed_messages_tx = Some(pushed_messages_tx);
    }
}

struct QueryOccured {
    message_index: usize,
    alias_to_name: HashMap<Identifier, Identifier>,
}

#[derive(Debug, Clone)]
struct CassandraNode {
    address: IpAddr,
    _rack: String,
    _tokens: Vec<String>,
    outbound: Option<CassandraConnection>,
}

impl CassandraNode {
    async fn get_connection(
        &mut self,
        handshake: &[Message],
        tls: &Option<TlsConnector>,
        pushed_messages_tx: &Option<mpsc::UnboundedSender<Messages>>,
    ) -> Result<&mut CassandraConnection> {
        if self.outbound.is_none() {
            self.outbound = Some(
                CassandraConnection::new(
                    (self.address, 9042),
                    CassandraCodec::new(),
                    tls.clone(),
                    pushed_messages_tx.clone(),
                )
                .await?,
            );
        }

        for handshake_message in handshake {
            let (return_chan_tx, return_chan_rx) = oneshot::channel();
            self.outbound
                .as_ref()
                .unwrap()
                .send(handshake_message.clone(), return_chan_tx)?;
            return_chan_rx.await?;
        }

        Ok(self.outbound.as_mut().unwrap())
    }
}
