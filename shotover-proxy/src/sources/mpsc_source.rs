use crate::config::topology::{ChannelMessage, TopicHolder};
use crate::server::Shutdown;
use crate::sources::Sources;
use crate::transforms::chain::TransformChain;
use crate::transforms::Wrapper;
use anyhow::{anyhow, Result};
use serde::Deserialize;
use std::time::Instant;
use tokio::runtime::Handle;
use tokio::sync::mpsc::Receiver;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

#[derive(Deserialize, Debug, Clone)]
pub enum CoalesceBehavior {
    Count(usize),
    WaitMs(u128),
    CountOrWait(usize, u128),
}

#[derive(Deserialize, Debug, Clone)]
pub struct AsyncMpscConfig {
    pub topic_name: String,
    pub coalesce_behavior: Option<CoalesceBehavior>,
}

impl AsyncMpscConfig {
    pub async fn get_source(
        &self,
        chain: &TransformChain,
        topics: &mut TopicHolder,
        trigger_shutdown_on_drop_rx: watch::Receiver<bool>,
    ) -> Result<Vec<Sources>> {
        if let Some(rx) = topics.get_rx(&self.topic_name) {
            let behavior = self
                .coalesce_behavior
                .clone()
                .unwrap_or(CoalesceBehavior::Count(10000));
            Ok(vec![Sources::Mpsc(AsyncMpsc::new(
                chain.clone(),
                rx,
                &self.topic_name,
                Shutdown::new(trigger_shutdown_on_drop_rx),
                behavior,
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
        mut main_chain: TransformChain,
        mut rx: Receiver<ChannelMessage>,
        name: &str,
        mut shutdown: Shutdown,
        max_behavior: CoalesceBehavior,
    ) -> AsyncMpsc {
        info!("Starting MPSC source for the topic [{}] ", name);
        let mut buffer = Vec::new();

        let jh = Handle::current().spawn(async move {
            let mut last_write = Instant::now();
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
                        buffer.append(&mut messages);
                        if match max_behavior {
                            CoalesceBehavior::Count(c) => buffer.len() >= c,
                            CoalesceBehavior::WaitMs(w) => last_write.elapsed().as_millis() >= w,
                            CoalesceBehavior::CountOrWait(c, w) => {
                                last_write.elapsed().as_millis() >= w || buffer.len() >= c
                            }
                        } {
                            //this could be done in the if statement above, but for the moment lets keep the
                            //evaluation logic separate from the update
                            match max_behavior {
                                CoalesceBehavior::WaitMs(_)
                                | CoalesceBehavior::CountOrWait(_, _) => {
                                    last_write = Instant::now()
                                }
                                _ => {}
                            }
                            std::mem::swap(&mut buffer, &mut messages);
                            let w = Wrapper::new_with_chain_name(messages, main_chain.name.clone());
                            info!("Flushing {} commands", w.messages.len());

                            if let Err(e) =
                                main_chain.process_request(w, "AsyncMpsc".to_string()).await
                            {
                                warn!("Something went wrong {}", e);
                            }
                        }
                    }
                    Some(tx) => {
                        let w = Wrapper::new_with_chain_name(messages, main_chain.name.clone());
                        if let Err(e) =
                            tx.send(main_chain.process_request(w, "AsyncMpsc".to_string()).await)
                        {
                            warn!("Something went wrong - couldn't return response {:?}", e);
                        }
                    }
                }
            }
            match main_chain
                .process_request(
                    Wrapper::flush_with_chain_name(main_chain.name.clone()),
                    "".into(),
                )
                .await
            {
                Ok(_) => info!("source AsyncMpsc was shutdown"),
                Err(e) => error!(
                    "source AsyncMpsc encountered an error when flushing the chain for shutdown: {}",
                    e
                )
            }
            Ok(())
        });

        AsyncMpsc {
            name: "AsyncMpsc",
            rx_handle: jh,
        }
    }
}
