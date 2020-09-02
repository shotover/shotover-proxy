use crate::error::ChainResponse;
use crate::transforms::{Transforms, Wrapper};
use bytes::Bytes;
use evmap::ReadHandleFactory;
use itertools::Itertools;
use metrics::{counter, timing};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::Instant;

type InnerChain = Vec<Transforms>;

//TODO explore running the transform chain on a LocalSet for better locality to a given OS thread
//Will also mean we can have `!Send` types  in our transform chain

// #[derive(Debug)]
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

impl TransformChain {
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
