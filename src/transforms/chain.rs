use crate::config::topology::ChannelMessage;
use crate::error::ChainResponse;
use crate::transforms::{Transforms, Wrapper};
use anyhow::{anyhow, Result};
use bytes::Bytes;
use futures::{FutureExt, TryFutureExt};

use itertools::Itertools;
use metrics::{counter, timing};
use std::sync::Arc;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::oneshot::Receiver as OneReceiver;
use tokio::sync::Mutex;
use tokio::time::Duration;
use tokio::time::Instant;
use tracing::{debug, info, trace, warn};

type InnerChain = Vec<Transforms>;

//TODO explore running the transform chain on a LocalSet for better locality to a given OS thread
//Will also mean we can have `!Send` types  in our transform chain

#[derive(Debug)]
pub struct TransformChain {
    name: String,
    pub chain: InnerChain,
}

impl Clone for TransformChain {
    fn clone(&self) -> Self {
        TransformChain::new(self.chain.clone(), self.name.clone())
    }
}

#[derive(Debug, Clone)]
pub struct BufferedChain {
    send_handle: Sender<ChannelMessage>,
    #[cfg(test)]
    pub count: Arc<Mutex<usize>>,
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
                    .send(ChannelMessage::new(wrapper.message, one_tx))
                    .map_err(|e| anyhow!("Couldn't send message to wrapped chain {:?}", e))
                    .await?
            }
            Some(timeout) => {
                self.send_handle
                    .send_timeout(
                        ChannelMessage::new(wrapper.message, one_tx),
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
                    .send(ChannelMessage::new_with_no_return(wrapper.message))
                    .map_err(|e| anyhow!("Couldn't send message to wrapped chain {:?}", e))
                    .await?
            }
            Some(timeout) => {
                self.send_handle
                    .send_timeout(
                        ChannelMessage::new_with_no_return(wrapper.message),
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
    pub fn build_buffered_chain(self, buffer_size: usize) -> BufferedChain {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<ChannelMessage>(buffer_size);

        // If this is not a test, this should get removed by the compiler
        let count_outer: Arc<Mutex<usize>> = Arc::new(Mutex::new(0_usize));
        let count = count_outer.clone();

        // Even though we don't keep the join handle, this thread will wrap up once all corresponding senders have been dropped.
        let _jh = tokio::spawn(async move {
            let mut chain = self;

            while let Some(ChannelMessage {
                return_chan,
                messages,
            }) = rx.recv().await
            {
                let name = chain.name.clone();
                if cfg!(test) {
                    let mut count = count.lock().await;
                    *count += 1;
                }

                let chain_response = chain.process_request(Wrapper::new(messages), name).await;

                if let Err(e) = &chain_response {
                    warn!("Internal error in buffered chain: {:?} - resetting", e);
                    chain = chain.clone();
                };

                match return_chan {
                    None => trace!("Ignoring response due to lack of return chan"),
                    Some(tx) => match tx.send(chain_response) {
                        Ok(_) => {}
                        Err(e) => trace!(
                            "Dropping response message {:?} as not needed by TunableConsistency",
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
            count: count_outer,
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
        let end = Instant::now();
        counter!("shotover_chain_total", 1, "chain" => self.name.clone());
        if result.is_err() {
            counter!("shotover_chain_failures", 1, "chain" => self.name.clone())
        }
        timing!("shotover_chain_latency", start, end, "chain" => self.name.clone(), "client_details" => client_details);
        result
    }
}
