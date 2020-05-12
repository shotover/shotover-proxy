use crate::transforms::chain::{Transform, ChainResponse, Wrapper, TransformChain, RequestError};
use rdkafka::config::ClientConfig;
use rdkafka::message::OwnedHeaders;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::get_rdkafka_version;

use async_trait::async_trait;
use crate::message::{Message, QueryResponse};

#[derive(Clone)]
pub struct KafkaDestination {
    name: &'static str,
    producer: FutureProducer,
}

impl KafkaDestination {
    pub fn new() -> KafkaDestination {
        KafkaDestination{
            name: "Kafka",
            producer: ClientConfig::new()
                .set("bootstrap.servers", "127.0.0.1:9092")
                .set("message.timeout.ms", "5000")
                .create()
                .expect("Producer creation error")
        }
    }
}

#[async_trait]
impl<'a, 'c> Transform<'a, 'c> for KafkaDestination {
    async fn transform(&self, mut qd: Wrapper, t: & TransformChain<'a,'c>) -> ChainResponse<'c> {
        if let Message::Query(qm) = qd.message {
            if let Some(ref key) = qm.get_namespaced_primary_key() {
                if let Some(values) = qm.query_values {
                    let message = serde_json::to_string(&values).map_err(|x| RequestError{})?;
                    let a = FutureRecord::to("test_topic")
                        .payload(&message)
                        .key(&key);
                    self.producer.send(a, 0);
                    return ChainResponse::Ok(Message::Response(QueryResponse::empty()))
                }
            }
        }
        return ChainResponse::Err(RequestError{});
    }

    fn get_name(&self) -> &'static str {
        self.name
    }
}
