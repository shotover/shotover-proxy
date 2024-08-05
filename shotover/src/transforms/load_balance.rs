use super::{DownChainProtocol, TransformContextBuilder, TransformContextConfig, UpChainProtocol};
use crate::config::chain::TransformChainConfig;
use crate::message::Messages;
use crate::transforms::chain::{BufferedChain, TransformChainBuilder};
use crate::transforms::{Transform, TransformBuilder, TransformConfig, Wrapper};
use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct ConnectionBalanceAndPoolConfig {
    pub name: String,
    pub max_connections: usize,
    pub chain: TransformChainConfig,
}

const NAME: &str = "ConnectionBalanceAndPool";
#[typetag::serde(name = "ConnectionBalanceAndPool")]
#[async_trait(?Send)]
impl TransformConfig for ConnectionBalanceAndPoolConfig {
    async fn get_builder(
        &self,
        transform_context: TransformContextConfig,
    ) -> Result<Box<dyn TransformBuilder>> {
        let chain = Arc::new(self.chain.get_builder(transform_context).await?);

        Ok(Box::new(ConnectionBalanceAndPoolBuilder {
            max_connections: self.max_connections,
            all_connections: Arc::new(Mutex::new(Vec::with_capacity(self.max_connections))),
            chain_to_clone: chain,
        }))
    }

    fn up_chain_protocol(&self) -> UpChainProtocol {
        UpChainProtocol::Any
    }

    fn down_chain_protocol(&self) -> DownChainProtocol {
        DownChainProtocol::SameAsUpChain
    }
}

struct ConnectionBalanceAndPoolBuilder {
    max_connections: usize,
    all_connections: Arc<Mutex<Vec<BufferedChain>>>,
    chain_to_clone: Arc<TransformChainBuilder>,
}

impl TransformBuilder for ConnectionBalanceAndPoolBuilder {
    fn build(&self, transform_context: TransformContextBuilder) -> Box<dyn Transform> {
        Box::new(ConnectionBalanceAndPool {
            active_connection: None,
            max_connections: self.max_connections,
            all_connections: self.all_connections.clone(),
            chain_to_clone: self.chain_to_clone.clone(),
            transform_context,
        })
    }

    fn is_terminating(&self) -> bool {
        true
    }

    fn get_name(&self) -> &'static str {
        NAME
    }
}

/// Every cloned instance of ConnectionBalanceAndPool will use a new connection until `max_connections` clones are made.
/// Once this happens cloned instances will reuse connections from earlier clones.
struct ConnectionBalanceAndPool {
    active_connection: Option<BufferedChain>,
    max_connections: usize,
    all_connections: Arc<Mutex<Vec<BufferedChain>>>,
    chain_to_clone: Arc<TransformChainBuilder>,
    transform_context: TransformContextBuilder,
}

#[async_trait]
impl Transform for ConnectionBalanceAndPool {
    fn get_name(&self) -> &'static str {
        NAME
    }

    async fn transform<'a>(
        &'a mut self,
        requests_wrapper: &'a mut Wrapper<'a>,
    ) -> Result<Messages> {
        if self.active_connection.is_none() {
            let mut all_connections = self.all_connections.lock().await;
            if all_connections.len() < self.max_connections {
                let chain = self
                    .chain_to_clone
                    .build_buffered(5, self.transform_context.clone());
                self.active_connection = Some(chain.clone());
                all_connections.push(chain);
            } else {
                //take the first available existing change and grab its reference
                let top = all_connections.remove(0);
                self.active_connection = Some(top.clone());
                // put the chain at the back of the list
                all_connections.push(top);
            }
        }
        self.active_connection
            .as_mut()
            .unwrap()
            .process_request(requests_wrapper.take(), None)
            .await
    }
}
