use super::Transforms;
use crate::config::chain::TransformChainConfig;
use crate::message::Messages;
use crate::transforms::chain::{BufferedChain, TransformChainBuilder};
use crate::transforms::{Transform, TransformBuilder, TransformConfig, Wrapper};
use anyhow::Result;
use async_trait::async_trait;
use serde::Deserialize;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Deserialize, Debug)]
pub struct ConnectionBalanceAndPoolConfig {
    pub name: String,
    pub max_connections: usize,
    pub chain: TransformChainConfig,
}

#[typetag::deserialize(name = "ConnectionBalanceAndPool")]
#[async_trait(?Send)]
impl TransformConfig for ConnectionBalanceAndPoolConfig {
    async fn get_builder(&self, _chain_name: String) -> Result<Box<dyn TransformBuilder>> {
        let chain = Arc::new(self.chain.get_builder(self.name.clone()).await?);

        Ok(Box::new(ConnectionBalanceAndPoolBuilder {
            max_connections: self.max_connections,
            all_connections: Arc::new(Mutex::new(Vec::with_capacity(self.max_connections))),
            chain_to_clone: chain,
        }))
    }
}

#[derive(Debug)]
pub struct ConnectionBalanceAndPoolBuilder {
    pub max_connections: usize,
    pub all_connections: Arc<Mutex<Vec<BufferedChain>>>,
    pub chain_to_clone: Arc<TransformChainBuilder>,
}

impl TransformBuilder for ConnectionBalanceAndPoolBuilder {
    fn build(&self) -> Transforms {
        Transforms::PoolConnections(ConnectionBalanceAndPool {
            active_connection: None,
            max_connections: self.max_connections,
            all_connections: self.all_connections.clone(),
            chain_to_clone: self.chain_to_clone.clone(),
        })
    }

    fn is_terminating(&self) -> bool {
        true
    }

    fn get_name(&self) -> &'static str {
        "ConnectionBalanceAndPool"
    }
}

/// Every cloned instance of ConnectionBalanceAndPool will use a new connection until `max_connections` clones are made.
/// Once this happens cloned instances will reuse connections from earlier clones.
#[derive(Debug)]
pub struct ConnectionBalanceAndPool {
    pub active_connection: Option<BufferedChain>,
    pub max_connections: usize,
    pub all_connections: Arc<Mutex<Vec<BufferedChain>>>,
    pub chain_to_clone: Arc<TransformChainBuilder>,
}

#[async_trait]
impl Transform for ConnectionBalanceAndPool {
    async fn transform<'a>(&'a mut self, message_wrapper: Wrapper<'a>) -> Result<Messages> {
        if self.active_connection.is_none() {
            let mut all_connections = self.all_connections.lock().await;
            if all_connections.len() < self.max_connections {
                let chain = self.chain_to_clone.build_buffered(5);
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
            .process_request(message_wrapper, None)
            .await
    }
}

#[cfg(test)]
mod test {
    use crate::message::Messages;
    use crate::transforms::chain::TransformChainBuilder;
    use crate::transforms::debug::returner::{DebugReturner, Response};
    use crate::transforms::load_balance::ConnectionBalanceAndPoolBuilder;
    use crate::transforms::{Transforms, Wrapper};
    use std::sync::Arc;

    #[tokio::test(flavor = "multi_thread")]
    pub async fn test_balance() {
        let transform = Box::new(ConnectionBalanceAndPoolBuilder {
            max_connections: 3,
            all_connections: Arc::new(Default::default()),
            chain_to_clone: Arc::new(TransformChainBuilder::new(
                vec![Box::new(DebugReturner::new(Response::Message(
                    Messages::new(),
                )))],
                "child_test".to_string(),
            )),
        });

        let chain = TransformChainBuilder::new(vec![transform], "test".to_string());

        for _ in 0..90 {
            chain
                .build()
                .process_request(Wrapper::new(Messages::new()), "test_client".to_string())
                .await
                .unwrap();
        }

        match chain.chain[0].build() {
            Transforms::PoolConnections(p) => {
                let all_connections = p.all_connections.lock().await;
                assert_eq!(all_connections.len(), 3);
                for bc in all_connections.iter() {
                    assert_eq!(bc.count.load(std::sync::atomic::Ordering::Relaxed), 30);
                }
            }
            _ => panic!("whoops"),
        }
    }
}
