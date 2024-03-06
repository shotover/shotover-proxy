// Allow direct usage of the APIs when the feature is enabled
pub use rdkafka;

use super::{ExpectedResponse, NewPartition, Record};
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

    pub async fn connect_admin(&self) -> KafkaAdminCpp {
        let admin = self.client.create().unwrap();
        KafkaAdminCpp { admin }
    }

    // TODO: support for these admin operations needs to be added to the java driver wrapper and then this method can be deleted
    pub async fn admin_setup(&self) {
        let admin = self.connect_admin().await.admin;

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

    // TODO: support for these admin operations needs to be added to the java driver wrapper and then this method can be deleted
    pub async fn admin_cleanup(&self) {
        let admin = self.connect_admin().await.admin;
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

pub struct KafkaAdminCpp {
    // TODO: make private
    pub admin: AdminClient<DefaultClientContext>,
}

impl KafkaAdminCpp {
    pub async fn create_topics(&self, topics: &[super::NewTopic<'_>]) {
        let topics: Vec<_> = topics
            .iter()
            .map(|topic| NewTopic {
                name: topic.name,
                num_partitions: topic.num_partitions,
                replication: TopicReplication::Fixed(topic.replication_factor as i32),
                config: vec![],
            })
            .collect();
        self.admin
            .create_topics(
                &topics,
                &AdminOptions::new()
                    .operation_timeout(Some(Timeout::After(Duration::from_secs(30)))),
            )
            .await
            .unwrap();
    }

    pub async fn create_partitions(&self, partitions: &[NewPartition<'_>]) {
        let partitions: Vec<_> = partitions
            .iter()
            .map(|partition| NewPartitions {
                topic_name: partition.topic_name,
                new_partition_count: partition.new_partition_count as usize,
                assignment: None,
            })
            .collect();
        let results = self
            .admin
            .create_partitions(
                &partitions,
                &AdminOptions::new()
                    .operation_timeout(Some(Timeout::After(Duration::from_secs(30)))),
            )
            .await
            .unwrap();
        for result in results {
            let result = result.unwrap();
            assert_eq!(result, "to_delete")
        }
    }
}
