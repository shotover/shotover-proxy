use crate::transforms::chain::TransformChain;
use tokio::sync::mpsc::Receiver;

use crate::config::topology::{ChannelMessage, TopicHolder};
use crate::server::Shutdown;
use crate::sources::{Sources, SourcesFromConfig};
use crate::transforms::Wrapper;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::runtime::Handle;
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;
use tracing::info;
use tracing::warn;

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
        notify_shutdown: broadcast::Sender<()>,
        shutdown_complete_tx: mpsc::Sender<()>,
    ) -> Result<Vec<Sources>> {
        if let Some(rx) = topics.get_rx(&self.topic_name) {
            return Ok(vec![Sources::Mpsc(AsyncMpsc::new(
                chain.clone(),
                rx,
                &self.topic_name,
                Shutdown::new(notify_shutdown.subscribe()),
                shutdown_complete_tx,
            ))]);
        }
        Err(anyhow!(
            "Could not find the topic {} in [{:#?}]",
            self.topic_name,
            topics.topics_rx.keys()
        ))
    }
}

#[derive(Debug)]
pub struct AsyncMpsc {
    pub name: &'static str,
    pub rx_handle: JoinHandle<Result<()>>,
}

impl AsyncMpsc {
    pub fn new(
        mut chain: TransformChain,
        mut rx: Receiver<ChannelMessage>,
        name: &str,
        mut shutdown: Shutdown,
        shutdown_complete: mpsc::Sender<()>,
    ) -> AsyncMpsc {
        info!("Starting MPSC source for the topic [{}] ", name);
        let mut main_chain = chain.clone();
        let jh = Handle::current().spawn(async move {
            // This will go out of scope once we exit the loop below, indicating we are done and shutdown
            let _notifier = shutdown_complete.clone();
            while !shutdown.is_shutdown() {
                let channel_message = tokio::select! {
                    res = rx.recv() => {
                        match res {
                            Some(m) => m,
                            None => return Ok(())
                        }
                    },
                    _ = shutdown.recv() => {
                        return Ok(());
                    }
                };

                let ChannelMessage {
                    messages,
                    return_chan,
                    public_client,
                } = channel_message;

                let w: Wrapper = Wrapper::new(messages);
                match return_chan {
                    None => {
                        if let Err(e) = main_chain.process_request(w, "AsyncMpsc".to_string()).await
                        {
                            warn!("Something went wrong {}", e);
                        }
                    }
                    Some(tx) => {
                        if let Err(e) =
                            tx.send(main_chain.process_request(w, "AsyncMpsc".to_string()).await)
                        {
                            warn!("Something went wrong - couldn't return response {:?}", e);
                        }
                    }
                }
            }
            Ok(())
        });

        AsyncMpsc {
            name: "AsyncMpsc",
            rx_handle: jh,
        }
    }
}
