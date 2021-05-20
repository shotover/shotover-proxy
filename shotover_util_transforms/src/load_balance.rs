use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use shotover_transforms::TopicHolder;
use shotover_transforms::{ChainResponse, Messages, Transform, TransformsFromConfig, Wrapper};

use shotover_transforms::build_chain_from_config;
use shotover_transforms::chain::{BufferedChain, TransformChain};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct ConnectionBalanceAndPoolConfig {
    pub name: String,
    pub parallelism: usize,
    pub chain: Vec<Box<dyn TransformsFromConfig + Send + Sync>>,
}

#[typetag::serde]
#[async_trait]
impl TransformsFromConfig for ConnectionBalanceAndPoolConfig {
    async fn get_source(&self, topics: &TopicHolder) -> Result<Box<dyn Transform + Send + Sync>> {
        let configs = self.chain.clone();
        let chain =
            build_chain_from_config(self.name.clone(), self.chain.as_slice(), &topics).await?;

        Ok(Box::new(ConnectionBalanceAndPool {
            name: "PoolConnections",
            active_connection: None,
            parallelism: self.parallelism,
            other_connections: Arc::new(Mutex::new(Vec::with_capacity(self.parallelism))),
            chain_to_clone: configs,
        }))
    }
}

#[derive(Debug)]
pub struct ConnectionBalanceAndPool {
    pub name: &'static str,
    pub active_connection: Option<BufferedChain>,
    pub parallelism: usize,
    pub other_connections: Arc<Mutex<Vec<BufferedChain>>>,
    pub chain_to_clone: Vec<Box<dyn TransformsFromConfig + Send + Sync>>,
}

impl Clone for ConnectionBalanceAndPool {
    fn clone(&self) -> Self {
        ConnectionBalanceAndPool {
            name: self.name,
            active_connection: None,
            parallelism: self.parallelism,
            other_connections: self.other_connections.clone(),
            chain_to_clone: self.chain_to_clone.clone(),
        }
    }
}

#[async_trait]
impl Transform for ConnectionBalanceAndPool {
    async fn transform<'a>(&'a mut self, mut wrapped_messages: Wrapper<'a>) -> ChainResponse {
        if self.active_connection.is_none() {
            let mut guard = self.other_connections.lock().await;
            if guard.len() < self.parallelism {
                let chain = build_chain_from_config(
                    self.name.clone().to_string(),
                    self.chain_to_clone.clone().as_slice(),
                    &TopicHolder::new(),
                )
                .await?
                .build_buffered_chain(5);
                self.active_connection.replace(chain.clone());
                guard.push(chain);
            } else {
                //take the first available existing change and grab its reference
                let top = guard.remove(0);
                self.active_connection.replace(top.clone());
                // put the chain at the back of the list
                guard.push(top);
            }
        }
        if let Some(chain) = &mut self.active_connection {
            return chain
                .process_request(
                    wrapped_messages,
                    "Connection Balance and Pooler".to_string(),
                    None,
                )
                .await;
        }
        unreachable!()
    }

    fn get_name(&self) -> &'static str {
        self.name
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use anyhow::Result;

    use shotover_transforms::Messages;
    use shotover_transforms::TopicHolder;
    use shotover_transforms::Wrapper;

    use crate::transforms::chain::TransformChain;
    use crate::transforms::load_balance::ConnectionBalanceAndPool;
    use crate::transforms::test_transforms::ReturnerTransform;

    #[tokio::test(flavor = "multi_thread")]
    pub async fn test_balance() -> Result<()> {
        let transform = Box::new(ConnectionBalanceAndPool {
            name: "",
            active_connection: None,
            parallelism: 3,
            other_connections: Arc::new(Default::default()),
            chain_to_clone: vec![Box::new(Box::new(ReturnerTransform {
                message: Messages::new(),
                ok: true,
            }))],
        });

        let mut chain = TransformChain::new(vec![transform], "test".to_string());

        for _ in 0..90 {
            let r = chain
                .clone()
                .process_request(Wrapper::new(Messages::new()), "test_client".to_string())
                .await;
            assert_eq!(r.is_ok(), true);
        }

        match chain.chain.remove(0) {
            Box::new(p) => {
                let guard = p.other_connections.lock().await;
                assert_eq!(guard.len(), 3);
                for bc in guard.iter() {
                    let guard = bc.count.lock().await;
                    assert_eq!(*guard, 30);
                }
            }
            _ => panic!("whoops"),
        }

        Ok(())
    }
}
