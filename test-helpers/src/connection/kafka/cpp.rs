// Allow direct usage of the APIs when the feature is enabled
pub use rdkafka;

use super::{ExpectedResponse, Record};
use rdkafka::admin::AdminClient;
use rdkafka::admin::{
    AdminOptions, AlterConfig, NewPartitions, NewTopic, OwnedResourceSpecifier, ResourceSpecifier,
    TopicReplication,
};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::types::RDKafkaErrorCode;
use rdkafka::util::Timeout;
use rdkafka::Message;
use std::time::Duration;

pub struct KafkaConnectionBuilderCpp {
    client: ClientConfig,
}

impl KafkaConnectionBuilderCpp {
    pub fn new(address: &str) -> Self {
        let mut client = ClientConfig::new();
        client
            .set("bootstrap.servers", address)
            // internal driver debug logs are emitted to tokio tracing, assuming the appropriate filter is used by the tracing subscriber
            .set("debug", "all");
        KafkaConnectionBuilderCpp { client }
    }

    pub fn use_sasl(mut self, user: &str, pass: &str) -> Self {
        self.client.set("sasl.mechanisms", "PLAIN");
        self.client.set("sasl.username", user);
        self.client.set("sasl.password", pass);
        self.client.set("security.protocol", "SASL_PLAINTEXT");
        self
    }

    pub async fn connect_producer(&self, acks: i32) -> KafkaProducerCpp {
        KafkaProducerCpp {
            producer: self
                .client
                .clone()
                .set("message.timeout.ms", "5000")
                .set("acks", &acks.to_string())
                .create()
                .unwrap(),
        }
    }

    pub async fn connect_consumer(&self, topic_name: &str) -> KafkaConsumerCpp {
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
        KafkaConsumerCpp { consumer }
    }

    pub async fn connect_admin(&self) -> AdminClient<DefaultClientContext> {
        self.client.create().unwrap()
    }

    pub async fn admin_setup(&self) {
        let admin = self.connect_admin().await;
        admin
            .create_topics(
                &[
                    NewTopic {
                        name: "partitions1",
                        num_partitions: 1,
                        replication: TopicReplication::Fixed(1),
                        config: vec![],
                    },
                    NewTopic {
                        name: "paritions3",
                        num_partitions: 3,
                        replication: TopicReplication::Fixed(1),
                        config: vec![],
                    },
                    NewTopic {
                        name: "acks0",
                        num_partitions: 1,
                        replication: TopicReplication::Fixed(1),
                        config: vec![],
                    },
                    NewTopic {
                        name: "to_delete",
                        num_partitions: 1,
                        replication: TopicReplication::Fixed(1),
                        config: vec![],
                    },
                ],
                &AdminOptions::new()
                    .operation_timeout(Some(Timeout::After(Duration::from_secs(30)))),
            )
            .await
            .unwrap();

        let results = admin
            .create_partitions(
                &[NewPartitions {
                    // TODO: modify topic "foo" instead so that we can test our handling of that with interesting partiton + replication count
                    topic_name: "to_delete",
                    new_partition_count: 2,
                    assignment: None,
                }],
                &AdminOptions::new()
                    .operation_timeout(Some(Timeout::After(Duration::from_secs(30)))),
            )
            .await
            .unwrap();
        for result in results {
            let result = result.unwrap();
            assert_eq!(result, "to_delete")
        }

        let results = admin
            .describe_configs(
                // TODO: test ResourceSpecifier::Broker and ResourceSpecifier::Group as well.
                //       Will need to find a way to get a valid broker id and to create a group.
                &[ResourceSpecifier::Topic("to_delete")],
                &AdminOptions::new()
                    .operation_timeout(Some(Timeout::After(Duration::from_secs(30)))),
            )
            .await
            .unwrap();
        for result in results {
            let result = result.unwrap();
            assert_eq!(
                result.specifier,
                OwnedResourceSpecifier::Topic("to_delete".to_owned())
            );
        }

        let results = admin
            .alter_configs(
                &[AlterConfig {
                    specifier: ResourceSpecifier::Topic("to_delete"),
                    entries: [("foo", "bar")].into(),
                }],
                &AdminOptions::new()
                    .operation_timeout(Some(Timeout::After(Duration::from_secs(30)))),
            )
            .await
            .unwrap();
        for result in results {
            assert_eq!(
                result.unwrap(),
                OwnedResourceSpecifier::Topic("to_delete".to_owned())
            );
        }

        let results = admin
            .delete_topics(
                &["to_delete"],
                &AdminOptions::new()
                    .operation_timeout(Some(Timeout::After(Duration::from_secs(30)))),
            )
            .await
            .unwrap();
        for result in results {
            assert_eq!(result.unwrap(), "to_delete");
        }
    }

    pub async fn admin_cleanup(&self) {
        let admin = self.connect_admin().await;
        let results = admin
            // The cpp driver will lock up when running certain commands after a delete_groups if the delete_groups is targeted at a group that doesnt exist.
            // So just make sure to run it against a group that does exist.
            .delete_groups(
                &["some_group"],
                &AdminOptions::new()
                    .operation_timeout(Some(Timeout::After(Duration::from_secs(30)))),
            )
            .await
            .unwrap();
        for result in results {
            match result {
                Ok(result) => assert_eq!(result, "some_group"),
                Err(err) => assert_eq!(
                    err,
                    // Allow this error which can occur due to race condition in the test, but do not allow any other error types
                    ("some_group".to_owned(), RDKafkaErrorCode::NonEmptyGroup)
                ),
            }
        }
    }
}

pub struct KafkaProducerCpp {
    producer: FutureProducer,
}

impl KafkaProducerCpp {
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

pub struct KafkaConsumerCpp {
    consumer: StreamConsumer,
}

impl KafkaConsumerCpp {
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
