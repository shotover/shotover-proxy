// Allow direct usage of the APIs when the feature is enabled
pub use rdkafka;

use rdkafka::admin::AdminClient;
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord};

use rdkafka::Message;
use std::time::Duration;

pub struct KafkaConnectionBuilder {
    client: ClientConfig,
}

impl KafkaConnectionBuilder {
    pub fn new(address: &str) -> Self {
        let mut client = ClientConfig::new();
        client
            .set("bootstrap.servers", address)
            // internal driver debug logs are emitted to tokio tracing, assuming the appropriate filter is used by the tracing subscriber
            .set("debug", "all");
        KafkaConnectionBuilder { client }
    }

    pub fn use_sasl(mut self, user: &str, pass: &str) -> Self {
        self.client.set("sasl.mechanisms", "PLAIN");
        self.client.set("sasl.username", user);
        self.client.set("sasl.password", pass);
        self.client.set("security.protocol", "SASL_PLAINTEXT");
        self
    }

    pub async fn connect_producer(&self, acks: i32) -> KafkaProducer {
        KafkaProducer {
            producer: self
                .client
                .clone()
                .set("message.timeout.ms", "5000")
                .set("acks", &acks.to_string())
                .create()
                .unwrap(),
        }
    }

    pub async fn connect_consumer(&self, topic_name: &str) -> KafkaConsumer {
        let consumer: StreamConsumer = self
            .client
            .clone()
            .set("group.id", "some_group")
            .set("session.timeout.ms", "6000")
            .set("auto.offset.reset", "earliest")
            .set("enable.auto.commit", "false")
            .create()
            .unwrap();
        consumer.subscribe(&[topic_name]).unwrap();
        KafkaConsumer { consumer }
    }

    pub async fn connect_admin(&self) -> AdminClient<DefaultClientContext> {
        self.client.create().unwrap()
    }
}

pub struct KafkaProducer {
    producer: FutureProducer,
}

impl KafkaProducer {
    pub async fn assert_produce(&self, record: Record<'_>, expected_offset: Option<i64>) {
        let send = match record.key {
            Some(key) => self
                .producer
                .send_result(
                    FutureRecord::to(record.topic_name)
                        .payload(record.payload)
                        .key(key),
                )
                .unwrap(),
            None => self
                .producer
                .send_result(FutureRecord::<(), _>::to(record.topic_name).payload(record.payload))
                .unwrap(),
        };
        let delivery_status = tokio::time::timeout(Duration::from_secs(30), send)
            .await
            .expect("Timeout while receiving from producer")
            .unwrap()
            .unwrap();

        if let Some(offset) = expected_offset {
            assert_eq!(delivery_status.1, offset, "Unexpected offset");
        }
    }
}

pub struct Record<'a> {
    pub payload: &'a str,
    pub topic_name: &'a str,
    pub key: Option<&'a str>,
}

pub struct KafkaConsumer {
    consumer: StreamConsumer,
}

impl KafkaConsumer {
    pub async fn assert_consume(&self, response: ExpectedResponse<'_>) {
        let message = tokio::time::timeout(Duration::from_secs(30), self.consumer.recv())
            .await
            .expect("Timeout while receiving from consumer")
            .unwrap();
        let contents = message.payload_view::<str>().unwrap().unwrap();
        assert_eq!(response.message, contents);
        assert_eq!(
            response.key,
            message.key().map(|x| std::str::from_utf8(x).unwrap())
        );
        assert_eq!(response.topic_name, message.topic());
        assert_eq!(response.offset, message.offset());
    }
}

pub struct ExpectedResponse<'a> {
    pub message: &'a str,
    pub key: Option<&'a str>,
    pub topic_name: &'a str,
    pub offset: i64,
}
