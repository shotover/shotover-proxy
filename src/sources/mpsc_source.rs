use crate::transforms::chain::{Transform, ChainResponse, Wrapper, TransformChain};
use tokio::sync::mpsc::{Sender, Receiver, channel};

use async_trait::async_trait;
use crate::message::{Message, QueryResponse};
use tokio::task::JoinHandle;
use tokio::sync::mpsc::error::RecvError;
use tokio::runtime::Handle;
use crate::transforms::Transforms;
use crate::transforms::mpsc::{AsyncMpscForwarder, AsyncMpscTee};
use serde::{Serialize, Deserialize};
use crate::sources::{Sources, SourcesFromConfig};
use crate::config::topology::TopicHolder;
use crate::config::ConfigError;
use std::error::Error;


#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct AsyncMpscConfig {
    pub topic_name: String
}

#[async_trait]
impl SourcesFromConfig for AsyncMpscConfig {
    async fn get_source(&self, chain: &TransformChain, topics: &mut TopicHolder) -> Result<Sources, ConfigError> {
        if let Some(rx) = topics.get_rx(self.topic_name.clone()) {
            return Ok(Sources::Mpsc(AsyncMpsc::new(chain.clone(), rx)))
        }
        Err(ConfigError{})

    }
}

pub struct AsyncMpsc {
    pub name: &'static str,
    pub rx_handle: JoinHandle<Result<(), Box<dyn Error + Send + Sync>>>
}

impl AsyncMpsc {
    fn tee_loop(mut rx: Receiver<Message>, chain: TransformChain) -> JoinHandle<Result<(), Box<dyn Error + Send + Sync>>> {
        Handle::current().spawn(async move {
            loop {
                if let Some(m) = rx.recv().await {
                    let w: Wrapper = Wrapper::new(m.clone());
                    chain.process_request(w).await;
                }
            }
        })
    }

    pub fn new(chain: TransformChain, mut rx: Receiver<Message>) -> AsyncMpsc {
        return AsyncMpsc {
            name: "AsyncMpsc",
            rx_handle: AsyncMpsc::tee_loop(rx, chain)
        };
    }
    // pub fn new_old(chain: TransformChain) -> AsyncMpsc {
    //     let (tx, rx) = channel::<Message>(5);
    //     return AsyncMpsc {
    //         name: "AsyncMpsc",
    //         tx,
    //         rx_handle: AsyncMpsc::tee_loop(rx, chain)
    //     };
    // }

    // pub fn get_async_mpsc_forwarder_enum(&self) -> Transforms {
    //     Transforms::MPSCForwarder(self.get_async_mpsc_forwarder())
    // }
    //
    // pub fn get_async_mpsc_tee_enum(&self) -> Transforms {
    //     Transforms::MPSCTee(self.get_async_mpsc_tee())
    // }
    //
    // pub fn get_async_mpsc_forwarder(&self) -> AsyncMpscForwarder {
    //     AsyncMpscForwarder{
    //         name: "Forward",
    //         tx: self.tx.clone(),
    //     }
    // }
    //
    // pub fn get_async_mpsc_tee(&self) -> AsyncMpscTee {
    //     AsyncMpscTee{
    //         name: "Tee",
    //         tx: self.tx.clone(),
    //     }
    // }
}
