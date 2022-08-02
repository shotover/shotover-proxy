use super::connection::CassandraConnection;
use crate::codec::cassandra::CassandraCodec;
use crate::concurrency::FuturesOrdered;
use crate::error::ChainResponse;
use crate::frame::cassandra;
use crate::message::Messages;
use crate::tls::TlsConfig;
use crate::tls::TlsConnector;
use crate::transforms::util::Response;
use crate::transforms::{Transform, Transforms, Wrapper};
use anyhow::Result;
use async_trait::async_trait;
use cassandra_protocol::frame::Opcode;
use metrics::{register_counter, Counter};
use serde::Deserialize;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::time::timeout;
use tokio_stream::StreamExt;
use tracing::{info, trace};

#[derive(Deserialize, Debug, Clone)]
pub struct CassandraSinkSingleConfig {
    #[serde(rename = "remote_address")]
    pub address: String,
    pub tls: Option<TlsConfig>,
}

impl CassandraSinkSingleConfig {
    pub async fn get_transform(&self, chain_name: String) -> Result<Transforms> {
        let tls = self.tls.clone().map(TlsConnector::new).transpose()?;
        Ok(Transforms::CassandraSinkSingle(CassandraSinkSingle::new(
            self.address.clone(),
            chain_name,
            tls,
        )))
    }
}

pub struct CassandraSinkSingle {
    address: String,
    outbound: Option<CassandraConnection>,
    chain_name: String,
    failed_requests: Counter,
    tls: Option<TlsConnector>,
    pushed_messages_tx: Option<mpsc::UnboundedSender<Messages>>,
}

impl Clone for CassandraSinkSingle {
    fn clone(&self) -> Self {
        CassandraSinkSingle {
            address: self.address.clone(),
            outbound: None,
            chain_name: self.chain_name.clone(),
            tls: self.tls.clone(),
            failed_requests: self.failed_requests.clone(),
            pushed_messages_tx: None,
        }
    }
}

impl CassandraSinkSingle {
    pub fn new(
        address: String,
        chain_name: String,
        tls: Option<TlsConnector>,
    ) -> CassandraSinkSingle {
        let failed_requests = register_counter!("failed_requests", "chain" => chain_name.clone(), "transform" => "CassandraSinkSingle");

        CassandraSinkSingle {
            address,
            outbound: None,
            chain_name,
            failed_requests,
            tls,
            pushed_messages_tx: None,
        }
    }
}

impl CassandraSinkSingle {
    async fn send_message(&mut self, messages: Messages) -> ChainResponse {
        if self.outbound.is_none() {
            trace!("creating outbound connection {:?}", self.address);
            self.outbound = Some(
                CassandraConnection::new(
                    self.address.clone(),
                    CassandraCodec::new(),
                    self.tls.clone(),
                    self.pushed_messages_tx.clone(),
                )
                .await?,
            );
        }
        trace!("sending frame upstream");

        let outbound = self.outbound.as_mut().unwrap();
        let expected_size = messages.len();
        let results: Result<FuturesOrdered<oneshot::Receiver<Response>>> = messages
            .into_iter()
            .map(|m| {
                let (return_chan_tx, return_chan_rx) = oneshot::channel();
                outbound.send(m, return_chan_tx)?;

                Ok(return_chan_rx)
            })
            .collect();

        let mut responses = Vec::with_capacity(expected_size);
        let mut results = results?;

        loop {
            match timeout(Duration::from_secs(5), results.next()).await {
                Ok(Some(prelim)) => {
                    match prelim? {
                        Response {
                            response: Ok(message),
                            ..
                        } => {
                            if let Some(raw_bytes) = message.as_raw_bytes() {
                                if let Ok(Opcode::Error) =
                                    cassandra::raw_frame::get_opcode(raw_bytes)
                                {
                                    self.failed_requests.increment(1);
                                }
                            }
                            responses.push(message);
                        }
                        Response {
                            mut original,
                            response: Err(err),
                        } => {
                            // TODO: This is wrong: need to have a response for each incoming message
                            original.set_error(err.to_string());
                            responses.push(original);
                        }
                    };
                }
                Ok(None) => break,
                Err(_) => {
                    info!(
                        "timed out waiting for results got - {:?} expected - {:?}",
                        responses.len(),
                        expected_size
                    );
                    info!(
                        "timed out waiting for results - {:?} - {:?}",
                        responses, results
                    );
                }
            }
        }

        Ok(responses)
    }
}

#[async_trait]
impl Transform for CassandraSinkSingle {
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
