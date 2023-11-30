use crate::message::Messages;
use crate::transforms::{TransformBuilder, Transforms, Wrapper};
use anyhow::{anyhow, Result};
use derivative::Derivative;
use futures::TryFutureExt;
use metrics::{histogram, register_counter, register_histogram, Counter};
use std::net::SocketAddr;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{Duration, Instant};
use tracing::{debug, error, info, trace, Instrument};

type InnerChain = Vec<Transforms>;

#[derive(Debug)]
pub struct BufferedChainMessages {
    pub local_addr: SocketAddr,
    pub messages: Messages,
    pub flush: bool,
    pub return_chan: Option<oneshot::Sender<Result<Messages>>>,
}

impl BufferedChainMessages {
    pub fn new_with_no_return(m: Messages, local_addr: SocketAddr) -> Self {
        BufferedChainMessages {
            local_addr,
            messages: m,
            flush: false,
            return_chan: None,
        }
    }

    pub fn new(
        m: Messages,
        local_addr: SocketAddr,
        flush: bool,
        return_chan: oneshot::Sender<Result<Messages>>,
    ) -> Self {
        BufferedChainMessages {
            local_addr,
            messages: m,
            flush,
            return_chan: Some(return_chan),
        }
    }
}

//TODO explore running the transform chain on a LocalSet for better locality to a given OS thread
//Will also mean we can have `!Send` types  in our transform chain

/// A transform chain is a ordered list of transforms that a message will pass through.
/// Transform chains can be of arbitary complexity and a transform can even have its own set of child transform chains.
/// Transform chains are defined by the user in Shotover's configuration file and are linked to sources.
///
/// The transform chain is a vector of mutable references to the enum [Transforms] (which is an enum dispatch wrapper around the various transform types).
#[derive(Derivative)]
#[derivative(Debug)]
pub struct TransformChain {
    pub name: String,
    pub chain: InnerChain,

    #[derivative(Debug = "ignore")]
    chain_total: Counter,
    #[derivative(Debug = "ignore")]
    chain_failures: Counter,
}

#[derive(Debug, Clone)]
pub struct BufferedChain {
    send_handle: mpsc::Sender<BufferedChainMessages>,
    #[cfg(test)]
    pub count: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

impl BufferedChain {
    pub async fn process_request(
        &mut self,
        wrapper: Wrapper<'_>,
        buffer_timeout_micros: Option<u64>,
    ) -> Result<Messages> {
        self.process_request_with_receiver(wrapper, buffer_timeout_micros)
            .await?
            .await?
    }

    async fn process_request_with_receiver(
        &mut self,
        wrapper: Wrapper<'_>,
        buffer_timeout_micros: Option<u64>,
    ) -> Result<oneshot::Receiver<Result<Messages>>> {
        let (one_tx, one_rx) = oneshot::channel::<Result<Messages>>();
        match buffer_timeout_micros {
            None => {
                self.send_handle
                    .send(BufferedChainMessages::new(
                        wrapper.requests,
                        wrapper.local_addr,
                        wrapper.flush,
                        one_tx,
                    ))
                    .map_err(|e| anyhow!("Couldn't send message to wrapped chain {:?}", e))
                    .await?
            }
            Some(timeout) => {
                self.send_handle
                    .send_timeout(
                        BufferedChainMessages::new(
                            wrapper.requests,
                            wrapper.local_addr,
                            wrapper.flush,
                            one_tx,
                        ),
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
        buffer_timeout_micros: Option<u64>,
    ) -> Result<()> {
        if wrapper.flush {
            // To obey flush request we need to ensure messages have completed sending before returning.
            // In order to achieve that we need to use the regular process_request method.
            self.process_request(wrapper, buffer_timeout_micros).await?;
        } else {
            // When there is no flush we can return much earlier by not waiting for a response.
            match buffer_timeout_micros {
                None => {
                    self.send_handle
                        .send(BufferedChainMessages::new_with_no_return(
                            wrapper.requests,
                            wrapper.local_addr,
                        ))
                        .map_err(|e| anyhow!("Couldn't send message to wrapped chain {:?}", e))
                        .await?
                }
                Some(timeout) => {
                    self.send_handle
                        .send_timeout(
                            BufferedChainMessages::new_with_no_return(
                                wrapper.requests,
                                wrapper.local_addr,
                            ),
                            Duration::from_micros(timeout),
                        )
                        .map_err(|e| anyhow!("Couldn't send message to wrapped chain {:?}", e))
                        .await?
                }
            }
        }
        Ok(())
    }
}

impl TransformChain {
    pub async fn process_request(
        &mut self,
        mut wrapper: Wrapper<'_>,
        client_details: String,
    ) -> Result<Messages> {
        let start = Instant::now();
        wrapper.reset(&mut self.chain);

        let result = wrapper.call_next_transform().await;
        self.chain_total.increment(1);
        if result.is_err() {
            self.chain_failures.increment(1);
        }

        histogram!("shotover_chain_latency", start.elapsed(),  "chain" => self.name.clone(), "client_details" => client_details);
        result
    }

    pub async fn process_request_rev(
        &mut self,
        mut wrapper: Wrapper<'_>,
        client_details: String,
    ) -> Result<Messages> {
        let start = Instant::now();
        wrapper.reset_rev(&mut self.chain);

        let result = wrapper.call_next_transform_pushed().await;
        self.chain_total.increment(1);
        if result.is_err() {
            self.chain_failures.increment(1);
        }

        histogram!("shotover_chain_latency", start.elapsed(),  "chain" => self.name.clone(), "client_details" => client_details);
        result
    }
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct TransformChainBuilder {
    pub name: String,
    pub chain: Vec<Box<dyn TransformBuilder>>,

    #[derivative(Debug = "ignore")]
    chain_total: Counter,
    #[derivative(Debug = "ignore")]
    chain_failures: Counter,
}

impl TransformChainBuilder {
    pub fn new(chain: Vec<Box<dyn TransformBuilder>>, name: String) -> Self {
        for transform in &chain {
            register_counter!("shotover_transform_total", "transform" => transform.get_name());
            register_counter!("shotover_transform_failures", "transform" => transform.get_name());
            register_histogram!("shotover_transform_latency", "transform" => transform.get_name());
        }

        let chain_total = register_counter!("shotover_chain_total", "chain" => name.clone());
        let chain_failures = register_counter!("shotover_chain_failures", "chain" => name.clone());
        // Cant register shotover_chain_latency because a unique one is created for each client ip address

        TransformChainBuilder {
            name,
            chain,
            chain_total,
            chain_failures,
        }
    }

    pub fn validate(&self) -> Vec<String> {
        if self.chain.is_empty() {
            return vec![
                format!("{} chain:", self.name),
                "  Chain cannot be empty".to_string(),
            ];
        }

        let last_index = self.chain.len() - 1;

        let mut errors = self
            .chain
            .iter()
            .enumerate()
            .flat_map(|(i, transform)| {
                let mut errors = vec![];

                if i == last_index && !transform.is_terminating() {
                    errors.push(format!(
                        "  Non-terminating transform {:?} is last in chain. Last transform must be terminating.",
                        transform.get_name()
                    ));
                } else if i != last_index && transform.is_terminating() {
                    errors.push(format!(
                        "  Terminating transform {:?} is not last in chain. Terminating transform must be last in chain.",
                        transform.get_name()
                    ));
                }

                errors.extend(transform.validate().iter().map(|x| format!("  {x}")));

                errors
            })
            .collect::<Vec<String>>();

        if !errors.is_empty() {
            errors.insert(0, format!("{} chain:", self.name));
        }

        errors
    }

    pub fn build_buffered(&self, buffer_size: usize) -> BufferedChain {
        let (tx, mut rx) = mpsc::channel::<BufferedChainMessages>(buffer_size);

        #[cfg(test)]
        let count = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        #[cfg(test)]
        let count_clone = count.clone();

        // Even though we don't keep the join handle, this thread will wrap up once all corresponding senders have been dropped.

        let mut chain = self.build();
        let _jh = tokio::spawn(
            async move {
                while let Some(BufferedChainMessages {
                    local_addr,
                    return_chan,
                    messages,
                    flush,
                }) = rx.recv().await
                {
                    #[cfg(test)]
                    {
                        count_clone.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }

                    let mut wrapper = Wrapper::new_with_chain_name(messages, chain.name.clone(), local_addr);
                    wrapper.flush = flush;
                    let chain_response = chain.process_request(wrapper, chain.name.clone()).await;

                    if let Err(e) = &chain_response {
                        error!("Internal error in buffered chain: {e:?}");
                    };

                    match return_chan {
                        None => trace!("Ignoring response due to lack of return chan"),
                        Some(tx) => {
                            if let Err(message) = tx.send(chain_response) {
                                trace!("Failed to send response message over return chan. Message was: {message:?}");
                            }
                        }
                    };
                }

                debug!("buffered chain processing thread exiting, stopping chain loop and dropping");

                match chain
                    .process_request(
                        Wrapper::flush_with_chain_name(chain.name.clone()),
                        "".into(),
                    )
                    .await
                {
                    Ok(_) => info!("Buffered chain {} was shutdown", chain.name),
                    Err(e) => error!(
                        "Buffered chain {} encountered an error when flushing the chain for shutdown: {}",
                        chain.name, e
                    ),
                }
            }
            .in_current_span(),
        );

        BufferedChain {
            send_handle: tx,
            #[cfg(test)]
            count,
        }
    }

    /// Clone the chain while adding a producer for the pushed messages channel
    pub fn build(&self) -> TransformChain {
        let chain = self.chain.iter().map(|x| x.build()).collect();

        TransformChain {
            name: self.name.clone(),
            chain,
            chain_total: self.chain_total.clone(),
            chain_failures: self.chain_failures.clone(),
        }
    }

    /// Clone the chain while adding a producer for the pushed messages channel
    pub fn build_with_pushed_messages(
        &self,
        pushed_messages_tx: mpsc::UnboundedSender<Messages>,
    ) -> TransformChain {
        let chain = self
            .chain
            .iter()
            .map(|x| {
                let mut transform = x.build();
                transform.set_pushed_messages_tx(pushed_messages_tx.clone());
                transform
            })
            .collect();

        TransformChain {
            name: self.name.clone(),
            chain,
            chain_total: self.chain_total.clone(),
            chain_failures: self.chain_failures.clone(),
        }
    }
}

#[cfg(test)]
mod chain_tests {
    use crate::transforms::chain::TransformChainBuilder;
    use crate::transforms::debug::printer::DebugPrinter;
    use crate::transforms::null::NullSink;

    #[tokio::test]
    async fn test_validate_invalid_chain() {
        let chain = TransformChainBuilder::new(vec![], "test-chain".to_string());
        assert_eq!(
            chain.validate(),
            vec!["test-chain chain:", "  Chain cannot be empty"]
        );
    }

    #[tokio::test]
    async fn test_validate_valid_chain() {
        let chain = TransformChainBuilder::new(
            vec![
                Box::<DebugPrinter>::default(),
                Box::<DebugPrinter>::default(),
                Box::<NullSink>::default(),
            ],
            "test-chain".to_string(),
        );
        assert_eq!(chain.validate(), Vec::<String>::new());
    }
}
