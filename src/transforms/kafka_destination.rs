use crate::transforms::chain::{ChainResponse, RequestError, Transform, TransformChain, Wrapper};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde::{Deserialize, Serialize};

use crate::config::topology::TopicHolder;
use crate::config::ConfigError;
use crate::message::{Message, QueryResponse};
use crate::transforms::{Transforms, TransformsFromConfig};
use async_trait::async_trait;
use slog::Logger;
use std::collections::HashMap;

#[derive(Clone)]
pub struct KafkaDestination {
    producer: FutureProducer,
    logger: Logger,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct KafkaConfig {
    #[serde(rename = "config_values")]
    pub keys: HashMap<String, String>,
}

#[async_trait]
impl TransformsFromConfig for KafkaConfig {
    async fn get_source(
        &self,
        topics: &TopicHolder,
        logger: &Logger,
    ) -> Result<Transforms, ConfigError> {
        Ok(Transforms::KafkaDestination(
            KafkaDestination::new_from_config(&self.keys, logger),
        ))
    }
}

impl KafkaDestination {
    pub fn new_from_config(
        config_map: &HashMap<String, String>,
        logger: &Logger,
    ) -> KafkaDestination {
        let mut config = ClientConfig::new();
        for (k, v) in config_map.iter() {
            config.set(k.as_str(), v.as_str());
        }
        return KafkaDestination {
            producer: config.create().expect("Producer creation error"),
            logger: logger.clone(),
        };
    }

    pub fn new(logger: Logger) -> KafkaDestination {
        KafkaDestination {
            producer: ClientConfig::new()
                .set("bootstrap.servers", "127.0.0.1:9092")
                .set("message.timeout.ms", "5000")
                .create()
                .expect("Producer creation error"),
            logger,
        }
    }
}

#[async_trait]
impl Transform for KafkaDestination {
    async fn transform(&self, mut qd: Wrapper, t: &TransformChain) -> ChainResponse {
        if let Message::Query(qm) = qd.message {
            if let Some(ref key) = qm.get_namespaced_primary_key() {
                if let Some(values) = qm.query_values {
                    let message = serde_json::to_string(&values).map_err(|x| RequestError {})?;
                    let a = FutureRecord::to("test_topic").payload(&message).key(&key);
                    self.producer.send(a, 0);
                    return ChainResponse::Ok(Message::Response(QueryResponse::empty()));
                }
            }
        }
        return ChainResponse::Err(RequestError {});
    }

    fn get_name(&self) -> &'static str {
        "Kafka"
    }
}
