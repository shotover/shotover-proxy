use crate::transforms::chain::TransformChain;
use tokio::sync::mpsc::Receiver;

use crate::config::topology::{ChannelMessage, TopicHolder};
use crate::message::Message;
use crate::server::Shutdown;
use crate::sources::{Sources, SourcesFromConfig};
use crate::transforms::coalesce::CoalesceBehavior;
use crate::transforms::Wrapper;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::time::Instant;
use tokio::runtime::Handle;
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;
use tracing::info;
use tracing::warn;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct AsyncMpscConfig {
    pub topic_name: String,
    pub coalesce_behavior: Option<CoalesceBehavior>,
}

#[async_trait]
impl SourcesFromConfig for AsyncMpscConfig {
    async fn get_source(
        &self,
        chain: &TransformChain,
        topics: &mut TopicHolder,
        trigger_shutdown_on_drop_rx: broadcast::Sender<()>,
        shutdown_complete_tx: mpsc::Sender<()>,
    ) -> Result<Vec<Sources>> {
        if let Some(rx) = topics.get_rx(&self.topic_name) {
            let behavior = self
                .coalesce_behavior
                .clone()
                .unwrap_or(CoalesceBehavior::COUNT(10000));
            Ok(vec![Sources::Mpsc(AsyncMpsc::new(
                chain.clone(),
                rx,
                &self.topic_name,
                Shutdown::new(trigger_shutdown_on_drop_rx.subscribe()),
                shutdown_complete_tx,
                behavior.clone(),
            ))])
        } else {
            Err(anyhow!(
                "Could not find the topic {} in [{:#?}]",
                self.topic_name,
                topics.topics_rx.keys()
            ))
        }
    }
}

#[derive(Debug)]
pub struct AsyncMpsc {
    pub name: &'static str,
    pub rx_handle: JoinHandle<Result<()>>,
}

impl AsyncMpsc {
    pub fn new(
        chain: TransformChain,
        mut rx: Receiver<ChannelMessage>,
        name: &str,
        mut shutdown: Shutdown,
        shutdown_complete: mpsc::Sender<()>,
        max_behavior: CoalesceBehavior,
    ) -> AsyncMpsc {
        info!("Starting MPSC source for the topic [{}] ", name);
        let mut main_chain = chain.clone();
        let max_behavior = max_behavior.clone();
        let mut buffer: Vec<Message> = Vec::new();

        let jh = Handle::current().spawn(async move {
            // This will go out of scope once we exit the loop below, indicating we are done and shutdown
            let _notifier = shutdown_complete.clone();
            let mut last_write: Instant = Instant::now();
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
                    mut messages,
                    return_chan,
                } = channel_message;

                match return_chan {
                    None => {
                        buffer.append(&mut messages.messages);
                        if match max_behavior {
                            CoalesceBehavior::COUNT(c) => buffer.len() >= c,
                            CoalesceBehavior::WAIT_MS(w) => last_write.elapsed().as_millis() >= w,
                            CoalesceBehavior::COUNT_OR_WAIT(c, w) => {
                                last_write.elapsed().as_millis() >= w || buffer.len() >= c
                            }
                        } {
                            //this could be done in the if statement above, but for the moment lets keep the
                            //evaluation logic separate from the update
                            match max_behavior {
                                CoalesceBehavior::WAIT_MS(_)
                                | CoalesceBehavior::COUNT_OR_WAIT(_, _) => {
                                    last_write = Instant::now()
                                }
                                _ => {}
                            }
                            std::mem::swap(&mut buffer, &mut messages.messages);
                            let w: Wrapper = Wrapper::new(messages);
                            info!("Flushing {} commands", w.message.messages.len());

                            if let Err(e) =
                                main_chain.process_request(w, "AsyncMpsc".to_string()).await
                            {
                                warn!("Something went wrong {}", e);
                            }
                        }
                    }
                    Some(tx) => {
                        let w: Wrapper = Wrapper::new(messages);
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
