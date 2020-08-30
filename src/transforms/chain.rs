use crate::error::{ChainResponse, RequestError};
use crate::transforms::{Transform, Transforms, Wrapper};
use anyhow::anyhow;
use bytes::Bytes;
use evmap::ReadHandleFactory;
use metrics::{counter, timing};
use mlua::Lua;
use std::sync::Arc;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::Mutex;
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
    pub lua_runtime: Arc<Mutex<mlua::Lua>>,
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
            lua_runtime: Arc::new(Mutex::new(Lua::new())),
        }
    }

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
            lua_runtime: Arc::new(Mutex::new(Lua::new())),
        }
    }

    pub async fn call_next_transform(&self, mut qd: Wrapper) -> ChainResponse {
        let current = qd.next_transform;
        qd.next_transform += 1;

        return match self.chain.get(current) {
            Some(t) => {
                let start = Instant::now();
                let result = t.transform(qd, self).await;
                let end = Instant::now();
                counter!("shotover_transform_total", 1, "transform" => t.get_name());
                if let Err(_) = &result {
                    counter!("shotover_transform_failures", 1, "transform" => t.get_name())
                }
                timing!("shotover_transform_latency", start, end, "transform" => t.get_name());
                result
            }
            None => Err(anyhow!(RequestError::ChainProcessingError(
                "No more transforms left in the chain".to_string()
            ))),
        };
    }

    pub async fn process_request(&self, wrapper: Wrapper, client_details: String) -> ChainResponse {
        let start = Instant::now();
        let result = self.call_next_transform(wrapper).await;
        let end = Instant::now();
        counter!("shotover_chain_total", 1, "chain" => self.name.clone());
        if let Err(_) = &result {
            counter!("shotover_chain_failures", 1, "chain" => self.name.clone())
        }
        timing!("shotover_chain_latency", start, end, "chain" => self.name.clone(), "client_details" => client_details);
        result
    }
}
