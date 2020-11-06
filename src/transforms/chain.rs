use crate::config::topology::ChannelMessage;
use crate::error::ChainResponse;
use crate::transforms::{Transforms, Wrapper};
use anyhow::{anyhow, Result};
use bytes::Bytes;
use evmap::ReadHandleFactory;
use futures::{FutureExt, TryFutureExt};

use crate::message::{Messages, QueryResponse, Value};
use crate::protocols::RawFrame;
use itertools::Itertools;
use metrics::{counter, timing};
use std::sync::Arc;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::oneshot::Receiver as OneReceiver;
use tokio::sync::Mutex;
use tokio::time::timeout;
use tokio::time::Duration;
use tokio::time::Instant;
use tracing::{info, trace, warn};

type InnerChain = Vec<Transforms>;

//TODO explore running the transform chain on a LocalSet for better locality to a given OS thread
//Will also mean we can have `!Send` types  in our transform chain

#[derive(Debug)]
pub struct TransformChain {
    name: String,
    pub chain: InnerChain,
    global_map: Option<ReadHandleFactory<String, Bytes>>,
    global_updater: Option<Sender<(String, Bytes)>>,
    pub chain_local_map: Option<ReadHandleFactory<String, Bytes>>,
    pub chain_local_map_updater: Option<Sender<(String, Bytes)>>,
}

impl Clone for TransformChain {
    fn clone(&self) -> Self {
        TransformChain::new(
            self.chain.clone(),
            self.name.clone(),
            self.global_map.as_ref().unwrap().clone(),
            self.global_updater.as_ref().unwrap().clone(),
        )
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
    ) -> ChainResponse {
        self.process_request_with_receiver(wrapper, client_details)
            .await?
            .await?
    }

    pub async fn process_request_with_receiver(
        &mut self,
        wrapper: Wrapper<'_>,
        _client_details: String,
    ) -> Result<OneReceiver<ChainResponse>> {
        let (one_tx, one_rx) = tokio::sync::oneshot::channel::<ChainResponse>();
        self.send_handle
            .send(ChannelMessage::new(wrapper.message, one_tx))
            .map_err(|e| anyhow!("Couldn't send message to wrapped chain {:?}", e))
            .await?;
        Ok(one_rx)
    }
}

impl TransformChain {
    pub fn build_buffered_chain(
        self,
        buffer_size: usize,
        timeout_millis: Option<u64>,
    ) -> BufferedChain {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<ChannelMessage>(buffer_size);

        // If this is not a test, this should get removed by the compiler
        let count_outer: Arc<Mutex<usize>> = Arc::new(Mutex::new(0 as usize));
        let count = count_outer.clone();

        // Even though we don't keep the join handle, this thread will wrap up once all corresponding senders have been dropped.
        let _jh = tokio::spawn(async move {
            let mut chain = self;
            let mut last_message = None;
            let mut last_response = None;

            while let Some(ChannelMessage {
                return_chan,
                messages,
            }) = rx.recv().await
            {
                last_message = Some(messages.clone());
                let name = chain.name.clone();
                if cfg!(test) {
                    let mut count = count.lock().await;
                    *count += 1;
                }
                let future = async {
                    match timeout_millis {
                        None => Ok(chain.process_request(Wrapper::new(messages), name).await),
                        Some(timeout_ms) => {
                            timeout(
                                Duration::from_millis(timeout_ms),
                                chain.process_request(Wrapper::new(messages), name),
                            )
                            .await
                        }
                    }
                };

                match future.fuse().await {
                    Ok(chain_response) => {
                        match &chain_response {
                            Ok(m) => {
                                last_response = Some(m.clone());
                            }
                            Err(e) => {
                                warn!("Internal error in buffered chain: {:?} - resetting", e);
                                chain = chain.clone();
                            }
                        };
                        match return_chan {
                            None => trace!("Ignoring response due to lack of return chan"),
                            Some(tx) => {
                                match tx.send(chain_response) {
                                    Ok(_) => {}
                                    Err(e) => trace!(
                                        "Dropping response message {:?} as not needed by TunableConsistency",
                                        e
                                    )
                                }
                            },
                        }
                    }
                    Err(e) => {
                        info!("Upstream timeout, resetting chain {}", e);
                        chain = chain.clone();
                    }
                }
            }
            warn!("buffered chain processing thread exiting, stopping chain loop and dropping - last message {:?} - last response - {:?}", last_message, last_response);
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
            global_map: None,
            global_updater: None,
            chain_local_map: None,
            chain_local_map_updater: None,
        }
    }

    #[allow(clippy::type_complexity)]
    pub fn new(
        transform_list: Vec<Transforms>,
        name: String,
        global_map_handle: ReadHandleFactory<String, Bytes>,
        global_updater: Sender<(String, Bytes)>,
    ) -> Self {
        let (local_tx, mut local_rx): (Sender<(String, Bytes)>, Receiver<(String, Bytes)>) =
            channel::<(String, Bytes)>(1);
        let (rh, mut wh) = evmap::new::<String, Bytes>();
        wh.refresh();

        let _ = tokio::spawn(async move {
            // this loop will exit and the task will complete when all channel tx handles are dropped
            while let Some((k, v)) = local_rx.recv().await {
                wh.insert(k, v);
                wh.refresh();
            }
        });

        TransformChain {
            name,
            chain: transform_list,
            global_map: Some(global_map_handle),
            global_updater: Some(global_updater),
            chain_local_map: Some(rh.factory()),
            chain_local_map_updater: Some(local_tx),
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
