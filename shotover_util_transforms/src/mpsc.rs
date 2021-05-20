// use anyhow::Result;
// use async_trait::async_trait;
// use itertools::Itertools;
// use metrics::counter;
// use serde::{Deserialize, Serialize};
// use tracing::trace;
//
// use shotover_transforms::TopicHolder;
// use shotover_transforms::{
//     ChainResponse, Message, Messages, QueryResponse, Transform, Value, Wrapper,
// };
// use shotover_transforms::{RawFrame, TransformsFromConfig};
//
// use crate::transforms::build_chain_from_config;
// use crate::transforms::chain::{BufferedChain, TransformChain};
// use crate::transforms::InternalTransform;
// use std::fmt::Debug;
//
// /*
// AsyncMPSC Tees and Forwarders should only be created from the AsyncMpsc struct,
// It's the thing that owns tx and rx handles :D
//  */
//
// #[derive(Debug)]
// pub struct Buffer {
//     pub name: &'static str,
//     pub tx: BufferedChain,
//     pub async_mode: bool,
//     pub buffer_size: usize,
//     pub chain_to_clone: TransformChain,
//     pub timeout: Option<u64>,
// }
//
// #[derive(Deserialize, Serialize, Debug, Clone)]
// pub struct BufferConfig {
//     pub async_mode: bool,
//     pub chain: Vec<Box<dyn TransformsFromConfig + Send + Sync>>,
//     pub buffer_size: Option<usize>,
//     pub timeout_micros: Option<u64>,
// }
//
// impl Clone for Buffer {
//     fn clone(&self) -> Self {
//         let chain = self.chain_to_clone.clone();
//         Buffer {
//             name: self.name.clone(),
//             tx: chain.build_buffered_chain(self.buffer_size),
//             async_mode: false,
//             buffer_size: self.buffer_size,
//             chain_to_clone: self.chain_to_clone.clone(),
//             timeout: self.timeout.clone(),
//         }
//     }
// }
//
// #[typetag::serde]
// #[async_trait]
// impl TransformsFromConfig for BufferConfig {
//     async fn get_source(&self, topics: &TopicHolder) -> Result<Box<dyn Transform + Send + Sync>> {
//         let chain = build_chain_from_config("forward".to_string(), &self.chain, &topics).await?;
//         let buffer = self.buffer_size.unwrap_or(5);
//         return Ok(Box::new(Buffer {
//             name: "forward",
//             tx: chain.clone().build_buffered_chain(buffer),
//             async_mode: self.async_mode,
//             buffer_size: buffer,
//             chain_to_clone: chain,
//             timeout: self.timeout_micros,
//         }));
//     }
// }
//
// #[async_trait]
// impl Transform for Buffer {
//     async fn transform<'a>(&'a mut self, mut wrapped_messages: Wrapper<'a>) -> ChainResponse {
//         return if self.async_mode {
//             let expected_responses = wrapped_messages.message.messages.len();
//             let buffer_result = self
//                 .tx
//                 .process_request_no_return(wrapped_messages, "Buffer".to_string(), self.timeout)
//                 .await;
//
//             match buffer_result {
//                 Ok(_) => {}
//                 Err(e) => {
//                     counter!("tee_dropped_messages", 1, "chain" => self.name);
//                     trace!("MPSC error {}", e);
//                 }
//             }
//
//             ChainResponse::Ok(Messages {
//                 messages: (0..expected_responses)
//                     .into_iter()
//                     .map(|_| Message::new_response(QueryResponse::empty(), true, RawFrame::NONE))
//                     .collect_vec(),
//             })
//         } else {
//             self.tx
//                 .process_request(wrapped_messages, "Buffer".to_string(), self.timeout)
//                 .await
//         };
//     }
//
//     fn get_name(&self) -> &'static str {
//         self.name
//     }
// }
//
// #[derive(Debug, Clone)]
// pub struct Tee {
//     pub name: &'static str,
//     pub tx: BufferedChain,
//     pub fail_chain: Option<BufferedChain>,
//     pub buffer_size: usize,
//     pub chain_to_clone: TransformChain,
//     pub behavior: ConsistencyBehavior,
//     pub timeout: Option<u64>,
// }
//
// #[derive(Deserialize, Serialize, Debug, Clone)]
// pub enum ConsistencyBehavior {
//     IGNORE,
//     FAIL,
//     LOG {
//         fail_chain: Vec<Box<dyn TransformsFromConfig + Send + Sync>>,
//     },
// }
//
// #[derive(Deserialize, Serialize, Debug, Clone)]
// pub struct TeeConfig {
//     pub behavior: Option<ConsistencyBehavior>,
//     pub timeout_micros: Option<u64>,
//     pub chain: Vec<Box<dyn TransformsFromConfig + Send + Sync>>,
//     pub buffer_size: Option<usize>,
// }
//
// #[typetag::serde]
// #[async_trait]
// impl TransformsFromConfig for TeeConfig {
//     async fn get_source(&self, topics: &TopicHolder) -> Result<Box<dyn Transform + Send + Sync>> {
//         let buffer_size = self.buffer_size.unwrap_or(5);
//         let fail_chain = if let Some(ConsistencyBehavior::LOG { fail_chain }) = &self.behavior {
//             Some(
//                 build_chain_from_config("fail_chain".to_string(), fail_chain.as_slice(), topics)
//                     .await?
//                     .build_buffered_chain(buffer_size),
//             )
//         } else {
//             None
//         };
//         let tee_chain =
//             build_chain_from_config("tee_chain".to_string(), &self.chain, topics).await?;
//
//         return Ok(Box::new(Tee {
//             name: "tee",
//             tx: tee_chain.clone().build_buffered_chain(buffer_size),
//             fail_chain,
//             buffer_size,
//             chain_to_clone: tee_chain,
//             behavior: self.behavior.clone().unwrap_or(ConsistencyBehavior::IGNORE),
//             timeout: self.timeout_micros,
//         }));
//     }
// }
//
// #[async_trait]
// impl Transform for Tee {
//     async fn transform<'a>(&'a mut self, mut wrapped_messages: Wrapper<'a>) -> ChainResponse {
//         // let m = wrapped_messages.message.clone();
//         return match self.behavior {
//             ConsistencyBehavior::IGNORE => {
//                 let (tee_result, chain_result) = tokio::join!(
//                     self.tx
//                         .process_request_no_return(wrapped_messages.clone(), "tee".to_string(), self.timeout),
//                     wrapped_messages.call_next_transform()
//                 );
//                 match tee_result {
//                     Ok(_) => {}
//                     Err(e) => {
//                         counter!("tee_dropped_messages", 1, "chain" => self.name);
//                         trace!("MPSC error {}", e);
//                     }
//                 }
//                 chain_result
//             }
//             ConsistencyBehavior::FAIL => {
//                 let (tee_result, chain_result) = tokio::join!(
//                     self.tx
//                         .process_request(wrapped_messages.clone(), "tee".to_string(), self.timeout),
//                     wrapped_messages.call_next_transform()
//                 );
//                 let tee_response = tee_result?;
//                 let chain_response = chain_result?;
//
//                 if !chain_response.eq(&tee_response) {
//                     Ok(Messages::new_single_response(
//                         QueryResponse::empty_with_error(Some(Value::Strings(
//                             "Shotover could not write to both topics via Tee - Behavior is to fail"
//                                 .to_string(),
//                         ))),
//                         true,
//                         RawFrame::NONE,
//                     ))
//                 } else {
//                     Ok(chain_response)
//                 }
//             }
//             ConsistencyBehavior::LOG { .. } => {
//                 let failed_message = wrapped_messages.clone();
//                 let (tee_result, chain_result) = tokio::join!(
//                     self.tx
//                         .process_request(wrapped_messages.clone(), "tee".to_string(), self.timeout),
//                     wrapped_messages.call_next_transform()
//                 );
//
//                 let tee_response = tee_result?;
//                 let chain_response = chain_result?;
//
//                 if chain_response.eq(&tee_response) {
//                     if let Some(topic) = &mut self.fail_chain {
//                         topic
//                             .process_request(failed_message, "tee".to_string(), None)
//                             .await?;
//                     }
//                 }
//
//                 Ok(chain_response)
//             }
//         };
//     }
//
//     fn get_name(&self) -> &'static str {
//         self.name
//     }
// }
