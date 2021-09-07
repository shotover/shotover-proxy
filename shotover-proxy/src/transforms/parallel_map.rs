use crate::config::topology::TopicHolder;
use crate::error::ChainResponse;
use crate::message::{Message, Messages};
use crate::transforms::chain::TransformChain;
use crate::transforms::{
    build_chain_from_config, Transform, Transforms, TransformsConfig, TransformsFromConfig, Wrapper,
};
use anyhow::Result;
use async_trait::async_trait;
use futures::stream::{FuturesOrdered, FuturesUnordered};
use futures::task::{Context, Poll};
use futures::Stream;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::pin::Pin;
use tokio_stream::StreamExt;

#[derive(Debug, Clone)]
pub struct ParallelMap {
    name: &'static str,
    chains: Vec<TransformChain>,
    ordered: bool,
}

enum UOFutures<T: Future> {
    Ordered(FuturesOrdered<T>),
    Unordered(FuturesUnordered<T>),
}

impl<T> UOFutures<T>
where
    T: Future,
{
    pub fn new(ordered: bool) -> Self {
        if ordered {
            Self::Ordered(FuturesOrdered::new())
        } else {
            Self::Unordered(FuturesUnordered::new())
        }
    }

    pub fn push(&mut self, future: T) {
        match self {
            UOFutures::Ordered(o) => o.push(future),
            UOFutures::Unordered(u) => u.push(future),
        }
    }
}

impl<T> Stream for UOFutures<T>
where
    T: Future,
{
    type Item = T::Output;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.get_mut() {
            UOFutures::Ordered(o) => Pin::new(o).poll_next(cx),
            UOFutures::Unordered(u) => Pin::new(u).poll_next(cx),
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct ParallelMapConfig {
    pub name: String,
    pub parallelism: u32,
    pub chain: Vec<TransformsConfig>,
    pub ordered_results: bool,
}

#[async_trait]
impl TransformsFromConfig for ParallelMapConfig {
    async fn get_source(&self, topics: &TopicHolder) -> Result<Transforms> {
        let chain = build_chain_from_config(self.name.clone(), &self.chain, topics).await?;

        Ok(Transforms::ParallelMap(ParallelMap {
            name: "SequentialMap",
            chains: std::iter::repeat(chain)
                .take(self.parallelism as usize)
                .collect_vec(),
            ordered: self.ordered_results,
        }))
    }
}

#[async_trait]
impl Transform for ParallelMap {
    async fn transform<'a>(&'a mut self, message_wrapper: Wrapper<'a>) -> ChainResponse {
        let mut results: Vec<Message> = Vec::with_capacity(message_wrapper.message.messages.len());
        let mut message_iter = message_wrapper.message.messages.into_iter();
        while message_iter.len() != 0 {
            let mut future = UOFutures::new(self.ordered);
            for chain in self.chains.iter_mut() {
                if let Some(message) = message_iter.next() {
                    future.push(chain.process_request(
                        Wrapper::new(Messages {
                            messages: vec![message],
                        }),
                        "Parallel".to_string(),
                    ));
                }
            }
            // We do this gnarly functional chain to unwrap each individual result and pop an error on the first one
            // then flatten it into one giant response.
            let mut temp: Vec<Message> = future
                .collect::<Vec<_>>()
                .await
                .into_iter()
                .collect::<anyhow::Result<Vec<Messages>>>()
                .into_iter()
                .flat_map(|ms| ms.into_iter().flat_map(|m| m.messages))
                .collect();
            results.append(&mut temp);
        }
        Ok(Messages { messages: results })
    }

    fn get_name(&self) -> &'static str {
        self.name
    }
}
