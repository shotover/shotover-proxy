use super::{DownChainProtocol, TransformContextBuilder, TransformContextConfig, UpChainProtocol};
use crate::config::chain::TransformChainConfig;
use crate::message::Messages;
use crate::transforms::chain::{TransformChain, TransformChainBuilder};
use crate::transforms::{Transform, TransformBuilder, TransformConfig, Wrapper};
use anyhow::Result;
use async_trait::async_trait;
use futures::stream::{FuturesOrdered, FuturesUnordered};
use futures::task::{Context, Poll};
use futures::Stream;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::pin::Pin;

struct ParallelMapBuilder {
    chains: Vec<TransformChainBuilder>,
    ordered: bool,
}

struct ParallelMap {
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
    fn new(ordered: bool) -> Self {
        if ordered {
            Self::Ordered(FuturesOrdered::new())
        } else {
            Self::Unordered(FuturesUnordered::new())
        }
    }

    fn push(&mut self, future: T) {
        match self {
            UOFutures::Ordered(o) => o.push_back(future),
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

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct ParallelMapConfig {
    pub parallelism: u32,
    pub chain: TransformChainConfig,
    pub ordered_results: bool,
}

const NAME: &str = "ParallelMap";
#[typetag::serde(name = "ParallelMap")]
#[async_trait(?Send)]
impl TransformConfig for ParallelMapConfig {
    async fn get_builder(
        &self,
        transform_context: TransformContextConfig,
    ) -> Result<Box<dyn TransformBuilder>> {
        let mut chains = vec![];
        for _ in 0..self.parallelism {
            let transform_context_config = TransformContextConfig {
                chain_name: "parallel_map_chain".into(),
                protocol: transform_context.protocol,
            };
            chains.push(self.chain.get_builder(transform_context_config).await?);
        }

        Ok(Box::new(ParallelMapBuilder {
            chains,
            ordered: self.ordered_results,
        }))
    }

    fn up_chain_protocol(&self) -> UpChainProtocol {
        UpChainProtocol::Any
    }

    fn down_chain_protocol(&self) -> DownChainProtocol {
        DownChainProtocol::Terminating
    }
}

#[async_trait]
impl Transform for ParallelMap {
    fn get_name(&self) -> &'static str {
        NAME
    }

    async fn transform<'a>(&'a mut self, requests_wrapper: Wrapper<'a>) -> Result<Messages> {
        let mut results = Vec::with_capacity(requests_wrapper.requests.len());
        let mut message_iter = requests_wrapper.requests.into_iter();
        while message_iter.len() != 0 {
            let mut future = UOFutures::new(self.ordered);
            for chain in self.chains.iter_mut() {
                if let Some(message) = message_iter.next() {
                    future.push(chain.process_request(Wrapper::new_with_addr(
                        vec![message],
                        requests_wrapper.local_addr,
                    )));
                }
            }
            // We do this gnarly functional chain to unwrap each individual result and pop an error on the first one
            // then flatten it into one giant response.
            results.extend(
                future
                    .collect::<Vec<_>>()
                    .await
                    .into_iter()
                    .collect::<anyhow::Result<Vec<Messages>>>()
                    .into_iter()
                    .flat_map(|ms| ms.into_iter().flatten()),
            );
        }
        Ok(results)
    }
}

impl TransformBuilder for ParallelMapBuilder {
    fn build(&self, transform_context: TransformContextBuilder) -> Box<dyn Transform> {
        Box::new(ParallelMap {
            chains: self
                .chains
                .iter()
                .map(|x| x.build(transform_context.clone()))
                .collect(),
            ordered: self.ordered,
        })
    }

    fn get_name(&self) -> &'static str {
        NAME
    }

    fn validate(&self) -> Vec<String> {
        let mut errors = self
            .chains
            .iter()
            .flat_map(|chain| {
                chain
                    .validate()
                    .iter()
                    .map(|x| format!("  {x}"))
                    .collect::<Vec<String>>()
            })
            .collect::<Vec<String>>();

        if !errors.is_empty() {
            errors.insert(0, format!("{}:", self.get_name()));
        }

        errors
    }

    fn is_terminating(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod parallel_map_tests {
    use crate::transforms::chain::TransformChainBuilder;
    use crate::transforms::debug::printer::DebugPrinter;
    use crate::transforms::null::NullSink;
    use crate::transforms::parallel_map::ParallelMapBuilder;
    use crate::transforms::TransformBuilder;
    use pretty_assertions::assert_eq;

    #[tokio::test]
    async fn test_validate_invalid_chain() {
        let chain_1 = TransformChainBuilder::new(
            vec![
                Box::<DebugPrinter>::default(),
                Box::<DebugPrinter>::default(),
                Box::<NullSink>::default(),
            ],
            "test-chain-1",
        );
        let chain_2 = TransformChainBuilder::new(vec![], "test-chain-2");

        let transform = ParallelMapBuilder {
            chains: vec![chain_1, chain_2],
            ordered: true,
        };

        assert_eq!(
            transform.validate(),
            vec![
                "ParallelMap:",
                "  test-chain-2 chain:",
                "    Chain cannot be empty"
            ]
        );
    }

    #[tokio::test]
    async fn test_validate_valid_chain() {
        let chain_1 = TransformChainBuilder::new(
            vec![
                Box::<DebugPrinter>::default(),
                Box::<DebugPrinter>::default(),
                Box::<NullSink>::default(),
            ],
            "test-chain-1",
        );
        let chain_2 = TransformChainBuilder::new(
            vec![
                Box::<DebugPrinter>::default(),
                Box::<DebugPrinter>::default(),
                Box::<NullSink>::default(),
            ],
            "test-chain-2",
        );

        let transform = ParallelMapBuilder {
            chains: vec![chain_1, chain_2],
            ordered: true,
        };

        assert_eq!(transform.validate(), Vec::<String>::new());
    }
}
