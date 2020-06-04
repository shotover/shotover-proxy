use crate::transforms::chain::{TransformChain, Wrapper};
use tokio::sync::mpsc::Receiver;

use crate::config::topology::TopicHolder;
use crate::config::ConfigError;
use crate::message::Message;
use crate::sources::{Sources, SourcesFromConfig};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use slog::info;
use slog::warn;
use slog::Logger;
use std::error::Error;
use tokio::runtime::Handle;
use tokio::task::JoinHandle;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct AsyncMpscConfig {
    pub topic_name: String,
}

#[async_trait]
impl SourcesFromConfig for AsyncMpscConfig {
    async fn get_source(
        &self,
        chain: &TransformChain,
        topics: &mut TopicHolder,
        logger: &Logger,
    ) -> Result<Sources, ConfigError> {
        if let Some(rx) = topics.get_rx(&self.topic_name) {
            return Ok(Sources::Mpsc(AsyncMpsc::new(
                chain.clone(),
                rx,
                &self.topic_name,
                logger,
            )));
        }
        Err(ConfigError::new(
            format!(
                "Could not find the topic {} in [{:#?}]",
                self.topic_name,
                topics.topics_rx.keys()
            )
            .as_str(),
        ))
    }
}

pub struct AsyncMpsc {
    pub name: &'static str,
    pub rx_handle: JoinHandle<Result<(), Box<dyn Error + Send + Sync>>>,
}

impl AsyncMpsc {
    fn tee_loop(
        mut rx: Receiver<Message>,
        chain: TransformChain,
        logger: Logger
    ) -> JoinHandle<Result<(), Box<dyn Error + Send + Sync>>> {
        Handle::current().spawn(async move {
            loop {
                if let Some(m) = rx.recv().await {
                    let w: Wrapper = Wrapper::new(m.clone());
                    if let Err(e) = chain.process_request(w).await {
                        warn!(logger, "Something went wrong {}", e)
                    }
                }
            }
        })
    }

    pub fn new(
        chain: TransformChain,
        rx: Receiver<Message>,
        name: &String,
        logger: &Logger,
    ) -> AsyncMpsc {
        info!(
            logger,
            "Starting MPSC source for the topic [{}] ",
            name.clone()
        );
        return AsyncMpsc {
            name: "AsyncMpsc",
            rx_handle: AsyncMpsc::tee_loop(rx, chain, logger.clone()),
        };
    }
}
