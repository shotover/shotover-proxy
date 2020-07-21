use crate::message::Message;
use crate::transforms::Transforms;
use async_trait::async_trait;
use metrics::timing;
use tokio::time::Instant;
use std::fmt::Display;
use serde::export::Formatter;
use crate::error::{ChainResponse, RequestError};
use anyhow::{anyhow};
use tracing::{instrument};
use bytes::{Bytes};
use evmap::{ReadHandleFactory};
use tokio::sync::mpsc::{channel, Sender, Receiver};


#[derive(Debug, Clone)]
struct QueryData {
    query: String,
}

//TODO change this to be generic to messages type
#[derive(Debug, Clone)]
pub struct Wrapper {
    pub message: Message,
    next_transform: usize,
    pub modified: bool,
    pub rnd: u32
}

impl Display for Wrapper {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        f.write_fmt(format_args!("{:#?}", self.message))
    }
}

impl Wrapper {
    pub fn new(m: Message) -> Self {
        Wrapper {
            message: m,
            next_transform: 0,
            modified: false,
            rnd: 0
        }
    }

    pub fn new_with_rnd(m: Message, rnd: u32) -> Self {
        Wrapper {
            message: m,
            next_transform: 0,
            modified: false,
            rnd
        }
    }

    pub fn reset(&mut self) {
        self.next_transform = 0;
    }
}

#[derive(Debug)]
struct ResponseData {
    response: Message,
}


//TODO change Transform to maintain the InnerChain internally so we don't have to expose this
pub type InnerChain = Vec<Transforms>;


//TODO explore running the transform chain on a LocalSet for better locality to a given OS thread
//Will also mean we can have `!Send` types  in our transform chain

#[async_trait]
pub trait Transform: Send + Sync {
    async fn transform(&self, mut qd: Wrapper, t: &TransformChain) -> ChainResponse;

    fn get_name(&self) -> &'static str;

    async fn instrument_transform(&self, qd: Wrapper, t: &TransformChain) -> ChainResponse {
        let start = Instant::now();
        let result = self.transform(qd, t).await;
        let end = Instant::now();
        timing!("", start, end, "transform" => self.get_name(), "" => "");
        return result;
    }

    async fn call_next_transform(
        &self,
        mut qd: Wrapper,
        transforms: &TransformChain,
    ) -> ChainResponse {
        let next = qd.next_transform;
        qd.next_transform += 1;
        return match transforms.chain.get(next) {
            Some(t) => t.instrument_transform(qd, transforms).await,
            None => {
                Err(anyhow!(RequestError::ChainProcessingError("No more transforms left in the chain".to_string())))
            },
        };
    }
}

// TODO: Remove thread safe requirements for transforms
// Currently the way this chain struct is built, requires that all Transforms are thread safe and
// implement with Sync

#[derive(Clone, Debug)]
pub struct TransformChain {
    name: String,
    chain: InnerChain,
    global_map: Option<ReadHandleFactory<String, Bytes>>,
    global_updater: Option<Sender<(String, Bytes)>>,
    chain_local_map: Option<ReadHandleFactory<String, Bytes>>,
    chain_local_map_updater: Option<Sender<(String, Bytes)>>
}

impl TransformChain {
    pub fn new_no_shared_state(transform_list: Vec<Transforms>, name: String) -> Self {
        return TransformChain {
            name,
            chain: transform_list,
            global_map: None,
            global_updater: None,
            chain_local_map: None,
            chain_local_map_updater: None
        };
    }

    pub fn new(transform_list: Vec<Transforms>, name: String, global_map_handle: ReadHandleFactory<String, Bytes>, global_updater: Sender<(String, Bytes)>) -> Self {
        let (local_tx, mut local_rx): (Sender<(String, Bytes)>, Receiver<(String, Bytes)>) = channel::<(String, Bytes)>(1);
        let (rh, mut wh) = evmap::new::<String, Bytes>();
        wh.refresh();

        let _ = tokio::spawn(async move {
            // this loop will exit and the task will complete when all channel tx handles are dropped
            while let Some((k,v)) = local_rx.recv().await {
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
            chain_local_map_updater: Some(local_tx)
        }
    }

    // #[instrument(skip(self))]
    pub async fn process_request(&self, mut wrapper: Wrapper) -> ChainResponse {
        // let span = trace_span!("processing request", );
        let start = Instant::now();
        let result = match self.chain.get(wrapper.next_transform) {
            Some(t) => {
                wrapper.next_transform += 1;
                t.instrument_transform(wrapper, &self).await
            }
            None => {
                return Err(anyhow!(RequestError::ChainProcessingError("No more transforms left in the chain".to_string())));
            },
        };
        let end = Instant::now();
        timing!("", start, end, "chain" => self.name.clone(), "" => "");
        return result;
    }
}
