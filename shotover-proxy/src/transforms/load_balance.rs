use crate::config::topology::TopicHolder;
use crate::error::ChainResponse;
use crate::transforms::chain::{BufferedChain, TransformChain};
use crate::transforms::{
    build_chain_from_config, Transform, Transforms, TransformsConfig, Wrapper,
};
use anyhow::Result;
use async_trait::async_trait;
use serde::Deserialize;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Deserialize, Debug, Clone)]
pub struct ConnectionBalanceAndPoolConfig {
    pub name: String,
    pub max_connections: usize,
    pub chain: Vec<TransformsConfig>,
}

impl ConnectionBalanceAndPoolConfig {
    pub async fn get_transform(&self, topics: &TopicHolder) -> Result<Transforms> {
        let chain = build_chain_from_config(self.name.clone(), &self.chain, topics).await?;

        Ok(Transforms::PoolConnections(ConnectionBalanceAndPool {
            active_connection: None,
            max_connections: self.max_connections,
            all_connections: Arc::new(Mutex::new(Vec::with_capacity(self.max_connections))),
            chain_to_clone: chain,
        }))
    }
}

/// Every cloned instance of ConnectionBalanceAndPool will use a new connection until `max_connections` clones are made.
/// Once this happens cloned instances will reuse connections from earlier clones.
#[derive(Debug)]
pub struct ConnectionBalanceAndPool {
    pub active_connection: Option<BufferedChain>,
    pub max_connections: usize,
    pub all_connections: Arc<Mutex<Vec<BufferedChain>>>,
    pub chain_to_clone: TransformChain,
}

impl Clone for ConnectionBalanceAndPool {
    fn clone(&self) -> Self {
        ConnectionBalanceAndPool {
            active_connection: None,
            max_connections: self.max_connections,
            all_connections: self.all_connections.clone(),
            chain_to_clone: self.chain_to_clone.clone(),
        }
    }
}

#[async_trait]
impl Transform for ConnectionBalanceAndPool {
    async fn transform<'a>(&'a mut self, message_wrapper: Wrapper<'a>) -> ChainResponse {
        if self.active_connection.is_none() {
            let mut all_connections = self.all_connections.lock().await;
            if all_connections.len() < self.max_connections {
                let chain = self.chain_to_clone.clone().into_buffered_chain(5);
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

    fn is_terminating(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod test {
    use crate::message::Messages;
    use crate::transforms::chain::TransformChain;
    use crate::transforms::debug::returner::{DebugReturner, Response};
    use crate::transforms::load_balance::ConnectionBalanceAndPool;
    use crate::transforms::{Transforms, Wrapper};
    use anyhow::Result;
    use std::sync::Arc;

    #[tokio::test(flavor = "multi_thread")]
    pub async fn test_balance() -> Result<()> {
        let transform = Transforms::PoolConnections(ConnectionBalanceAndPool {
            active_connection: None,
            max_connections: 3,
            all_connections: Arc::new(Default::default()),
            chain_to_clone: TransformChain::new(
                vec![Transforms::DebugReturner(DebugReturner::new(
                    Response::Message(Messages::new()),
                ))],
                "child_test".to_string(),
            ),
        });

        let mut chain = TransformChain::new(vec![transform], "test".to_string());

        for _ in 0..90 {
            let r = chain
                .clone()
                .process_request(Wrapper::new(Messages::new()), "test_client".to_string())
                .await;
            assert!(r.is_ok());
        }

        match chain.chain.remove(0) {
            Transforms::PoolConnections(p) => {
                let all_connections = p.all_connections.lock().await;
                assert_eq!(all_connections.len(), 3);
                for bc in all_connections.iter() {
                    let count = bc.count.lock().await;
                    assert_eq!(*count, 30);
                }
            }
            _ => panic!("whoops"),
        }

        Ok(())
    }
}
