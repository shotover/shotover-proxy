use std::collections::HashMap;

use anyhow::Result;
use async_trait::async_trait;
use rdkafka::config::ClientConfig;
use rdkafka::producer::FutureProducer;
use serde::Deserialize;

use crate::error::ChainResponse;
use crate::frame::Frame;
use crate::message::Message;
use crate::transforms::{Transform, Transforms, Wrapper};

#[derive(Clone)]
pub struct KafkaSink {
    _producer: FutureProducer,
    pub topic: String,
}

#[derive(Deserialize, Debug, Clone)]
pub struct KafkaSinkConfig {
    #[serde(rename = "config_values")]
    pub keys: HashMap<String, String>,
    pub topic: String,
}

impl KafkaSinkConfig {
    pub async fn get_source(&self) -> Result<Transforms> {
        Ok(Transforms::KafkaSink(KafkaSink::new_from_config(
            &self.keys,
            self.topic.clone(),
        )))
    }
}

impl KafkaSink {
    pub fn new_from_config(config_map: &HashMap<String, String>, topic: String) -> KafkaSink {
        let mut config = ClientConfig::new();
        for (k, v) in config_map.iter() {
            config.set(k.as_str(), v.as_str());
        }
        KafkaSink {
            _producer: config.create().expect("Producer creation error"),
            topic,
        }
    }

    pub fn new() -> KafkaSink {
        KafkaSink {
            _producer: ClientConfig::new()
                .set("bootstrap.servers", "127.0.0.1:9092")
                .set("message.timeout.ms", "5000")
                .create()
                .expect("Producer creation error"),
            topic: "test_Topic".to_string(),
        }
    }
}

impl Default for KafkaSink {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Transform for KafkaSink {
    fn is_terminating(&self) -> bool {
        true
    }

    async fn transform<'a>(&'a mut self, message_wrapper: Wrapper<'a>) -> ChainResponse {
        let mut responses = vec![];
        for _message in message_wrapper.messages {
            // match message.details {
            //     MessageDetails::Query(qm) => {
            //         if let Some(ref key) = qm.get_namespaced_primary_key() {
            //             if let Some(values) = qm.query_values {
            //                 let message = serde_json::to_string(&values)?;
            //                 let a = FutureRecord::to(self.topic.as_str())
            //                     .payload(&message)
            //                     .key(&key);
            //                 self.producer
            //                     .send(a, Timeout::Never)
            //                     .await
            //                     .map_err(|(e, _o)| anyhow!("Couldn't send kafka message {}", e))?;
            //             }
            //         }
            //     }
            //     MessageDetails::Response(_) => {}
            //     MessageDetails::Unknown => {}
            // }

            responses.push(Message::from_frame(Frame::None));
        }
        Ok(responses)
    }
}
