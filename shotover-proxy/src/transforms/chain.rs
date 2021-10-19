use crate::config::topology::ChannelMessage;
use crate::error::ChainResponse;
use crate::transforms::{Transforms, Wrapper};
use anyhow::{anyhow, Result};
use futures::TryFutureExt;

use itertools::Itertools;
use metrics::{counter, histogram};
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot::Receiver as OneReceiver;
use tokio::time::Duration;
use tokio::time::Instant;
use tracing::{debug, trace, warn};

type InnerChain = Vec<Transforms>;

//TODO explore running the transform chain on a LocalSet for better locality to a given OS thread
//Will also mean we can have `!Send` types  in our transform chain

/// A transform chain is a ordered list of transforms that a message will pass through.
/// Transform chains can be of arbitary complexity and a transform can even have its own set of child transform chains.
/// Transform chains are defined by the user in Shotover's configuration file and are linked to sources.
///
/// The transform chain is a vector of mutable references to the enum [Transforms] (which is an enum dispatch wrapper around the various transform types).
#[derive(Debug, Clone)]
pub struct TransformChain {
    name: String,
    pub chain: InnerChain,
}

#[derive(Debug, Clone)]
pub struct BufferedChain {
    pub original_chain: TransformChain,
    send_handle: Sender<ChannelMessage>,
    #[cfg(test)]
    pub count: std::sync::Arc<tokio::sync::Mutex<usize>>,
}

impl BufferedChain {
    pub async fn process_request(
        &mut self,
        wrapper: Wrapper<'_>,
        client_details: String,
        buffer_timeout_micros: Option<u64>,
    ) -> ChainResponse {
        self.process_request_with_receiver(wrapper, client_details, buffer_timeout_micros)
            .await?
            .await?
    }

    pub async fn process_request_with_receiver(
        &mut self,
        wrapper: Wrapper<'_>,
        _client_details: String,
        buffer_timeout_micros: Option<u64>,
    ) -> Result<OneReceiver<ChainResponse>> {
        let (one_tx, one_rx) = tokio::sync::oneshot::channel::<ChainResponse>();
        match buffer_timeout_micros {
            None => {
                self.send_handle
                    .send(ChannelMessage::new(wrapper.messages, one_tx))
                    .map_err(|e| anyhow!("Couldn't send message to wrapped chain {:?}", e))
                    .await?
            }
            Some(timeout) => {
                self.send_handle
                    .send_timeout(
                        ChannelMessage::new(wrapper.messages, one_tx),
                        Duration::from_micros(timeout),
                    )
                    .map_err(|e| anyhow!("Couldn't send message to wrapped chain {:?}", e))
                    .await?
            }
        }

        Ok(one_rx)
    }

    pub async fn process_request_no_return(
        &mut self,
        wrapper: Wrapper<'_>,
        _client_details: String,
        buffer_timeout_micros: Option<u64>,
    ) -> Result<()> {
        match buffer_timeout_micros {
            None => {
                self.send_handle
                    .send(ChannelMessage::new_with_no_return(wrapper.messages))
                    .map_err(|e| anyhow!("Couldn't send message to wrapped chain {:?}", e))
                    .await?
            }
            Some(timeout) => {
                self.send_handle
                    .send_timeout(
                        ChannelMessage::new_with_no_return(wrapper.messages),
                        Duration::from_micros(timeout),
                    )
                    .map_err(|e| anyhow!("Couldn't send message to wrapped chain {:?}", e))
                    .await?
            }
        }
        Ok(())
    }
}

impl TransformChain {
    pub fn into_buffered_chain(self, buffer_size: usize) -> BufferedChain {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<ChannelMessage>(buffer_size);

        #[cfg(test)]
        let count = std::sync::Arc::new(tokio::sync::Mutex::new(0_usize));
        #[cfg(test)]
        let count_clone = count.clone();

        // Even though we don't keep the join handle, this thread will wrap up once all corresponding senders have been dropped.

        let mut chain = self.clone();
        let _jh = tokio::spawn(async move {
            while let Some(ChannelMessage {
                return_chan,
                messages,
            }) = rx.recv().await
            {
                #[cfg(test)]
                {
                    let mut count = count.lock().await;
                    *count += 1;
                }

                let chain_response = chain
                    .process_request(
                        Wrapper::new_with_chain_name(messages, chain.name.clone()),
                        chain.name.clone(),
                    )
                    .await;

                if let Err(e) = &chain_response {
                    warn!("Internal error in buffered chain: {:?}", e);
                };

                match return_chan {
                    None => trace!("Ignoring response due to lack of return chan"),
                    Some(tx) => match tx.send(chain_response) {
                        Ok(_) => {}
                        Err(e) => trace!(
                            "Dropping response message {:?} as not needed by ConsistentScatter",
                            e
                        ),
                    },
                };
            }

            debug!("buffered chain processing thread exiting, stopping chain loop and dropping");
        });

        BufferedChain {
            send_handle: tx,
            #[cfg(test)]
            count: count_clone,
            original_chain: self,
        }
    }

    pub fn new_no_shared_state(transform_list: Vec<Transforms>, name: String) -> Self {
        TransformChain {
            name,
            chain: transform_list,
        }
    }

    #[allow(clippy::type_complexity)]
    pub fn new(transform_list: Vec<Transforms>, name: String) -> Self {
        TransformChain {
            name,
            chain: transform_list,
        }
    }

    pub fn validate(&self) -> Vec<String> {
        let len = self.chain.len();

        let mut errors = self
            .chain
            .iter()
            .enumerate()
            .map(|(i, transform)| {
                let position = (i + 1) % len;
                let mut errors = vec![];

                if position == 0 && !transform.is_terminating() {
                    errors.push(format!(
                        "  Non-terminating transform {:?} is last in chain",
                        transform.get_name()
                    ));
                } else if position != 0 && transform.is_terminating() {
                    errors.push(format!(
                        "  Terminating transform {:?} is not last in chain",
                        transform.get_name()
                    ));
                }

                errors.extend(
                    transform
                        .validate()
                        .iter()
                        .map(|x| format!("  {}", x))
                        .collect::<Vec<String>>(),
                );

                errors
            })
            .flatten()
            .collect::<Vec<String>>();

        if !errors.is_empty() {
            errors.insert(0, format!("{}:", self.name));
        }

        errors
    }

    pub fn get_inner_chain_refs(&mut self) -> Vec<&mut Transforms> {
        self.chain.iter_mut().collect_vec()
    }

    pub async fn process_request(
        &mut self,
        mut wrapper: Wrapper<'_>,
        client_details: String,
    ) -> ChainResponse {
        let start = Instant::now();
        let iter = self.chain.iter_mut().collect_vec();
        wrapper.reset(iter);

        let result = wrapper.call_next_transform().await;
        counter!("shotover_chain_total", 1, "chain" => self.name.clone());
        if result.is_err() {
            counter!("shotover_chain_failures", 1, "chain" => self.name.clone())
        }
        histogram!("shotover_chain_latency", start.elapsed(),  "chain" => self.name.clone(), "client_details" => client_details);
        result
    }
}
