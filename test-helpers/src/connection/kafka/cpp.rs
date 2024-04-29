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
            .set("broker.address.family", "v4")
            // internal driver debug logs are emitted to tokio tracing, assuming the appropriate filter is used by the tracing subscriber
            .set("debug", "all");
        KafkaConnectionBuilderCpp { client }
    }

    pub fn use_sasl_scram(mut self, user: &str, pass: &str) -> Self {
        self.client.set("sasl.mechanisms", "SCRAM-SHA-256");
        self.client.set("sasl.username", user);
        self.client.set("sasl.password", pass);
        self.client.set("security.protocol", "SASL_PLAINTEXT");
        self
    }

    pub fn use_sasl_plain(mut self, user: &str, pass: &str) -> Self {
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
    pub async fn consume(&self) -> ExpectedResponse {
        let message = tokio::time::timeout(Duration::from_secs(30), self.consumer.recv())
            .await
            .expect("Timeout while receiving from consumer")
            .unwrap();
        ExpectedResponse {
            message: message.payload_view::<str>().unwrap().unwrap().to_owned(),
            key: message
                .key()
                .map(|x| String::from_utf8(x.to_vec()).unwrap()),
            topic_name: message.topic().to_owned(),
            offset: Some(message.offset()),
        }
    }
}

pub struct KafkaAdminCpp {
    admin: AdminClient<DefaultClientContext>,
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

        let mut results: Vec<_> = results.into_iter().map(|x| x.unwrap()).collect();
        for partition in partitions {
            if let Some(i) = results.iter().position(|x| x == partition.topic_name) {
                results.remove(i);
            } else {
                panic!("topic {} not in results", partition.topic_name)
            }
        }
        assert!(results.is_empty());
    }

    pub async fn describe_configs(&self, resources: &[super::ResourceSpecifier<'_>]) {
        let resources: Vec<_> = resources.iter().map(resource_specifier).collect();
        let results = self
            .admin
            .describe_configs(
                &resources,
                &AdminOptions::new()
                    .operation_timeout(Some(Timeout::After(Duration::from_secs(30)))),
            )
            .await
            .unwrap();

        let mut results: Vec<_> = results.into_iter().map(|x| x.unwrap()).collect();
        for resource in resources {
            if let Some(i) = results
                .iter()
                .position(|x| resource_specifier_ref(&x.specifier) == resource)
            {
                results.remove(i);
            } else {
                panic!("resource {:?} not in results", resource)
            }
        }
        assert!(results.is_empty());
    }

    pub async fn alter_configs(&self, alter_configs: &[super::AlterConfig<'_>]) {
        let alter_configs: Vec<_> = alter_configs
            .iter()
            .map(|alter_config| AlterConfig {
                specifier: resource_specifier(&alter_config.specifier),
                entries: alter_config
                    .entries
                    .iter()
                    .map(|entry| (entry.key.as_str(), entry.value.as_str()))
                    .collect(),
            })
            .collect();
        let results = self
            .admin
            .alter_configs(
                &alter_configs,
                &AdminOptions::new()
                    .operation_timeout(Some(Timeout::After(Duration::from_secs(30)))),
            )
            .await
            .unwrap();

        let mut results: Vec<_> = results.into_iter().map(|x| x.unwrap()).collect();
        for alter_config in alter_configs {
            if let Some(i) = results
                .iter()
                .position(|x| resource_specifier_ref(x) == alter_config.specifier)
            {
                results.remove(i);
            } else {
                panic!("resource {:?} not in results", alter_config.specifier)
            }
        }
        assert!(results.is_empty());
    }

    pub async fn delete_topics(&self, to_delete: &[&str]) {
        let results = self
            .admin
            .delete_topics(
                to_delete,
                &AdminOptions::new()
                    .operation_timeout(Some(Timeout::After(Duration::from_secs(30)))),
            )
            .await
            .unwrap();

        let mut results: Vec<_> = results.into_iter().map(|x| x.unwrap()).collect();
        for to_delete in to_delete {
            if let Some(i) = results.iter().position(|x| x == to_delete) {
                results.remove(i);
            } else {
                panic!("topic {} not in results", to_delete)
            }
        }
        assert!(results.is_empty());
    }
}

fn resource_specifier<'a>(specifier: &'a super::ResourceSpecifier<'a>) -> ResourceSpecifier<'a> {
    match specifier {
        super::ResourceSpecifier::Topic(topic) => ResourceSpecifier::Topic(topic),
    }
}

fn resource_specifier_ref(specifier: &OwnedResourceSpecifier) -> ResourceSpecifier {
    match specifier {
        OwnedResourceSpecifier::Topic(topic) => ResourceSpecifier::Topic(topic),
        OwnedResourceSpecifier::Group(group) => ResourceSpecifier::Group(group),
        OwnedResourceSpecifier::Broker(broker) => ResourceSpecifier::Broker(*broker),
    }
}
