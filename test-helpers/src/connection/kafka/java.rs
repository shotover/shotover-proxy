use super::{
    Acl, AclOperation, AclPermissionType, AlterConfig, ExpectedResponse, NewPartition, NewTopic,
    Record, ResourcePatternType, ResourceSpecifier, ResourceType, TopicDescription, TopicPartition,
};
use crate::connection::java::{Jvm, Value};
use anyhow::Result;
use pretty_assertions::assert_eq;
use std::collections::{HashMap, HashSet, VecDeque};

fn properties(jvm: &Jvm, props: &HashMap<String, String>) -> Value {
    let properties = jvm.construct("java.util.Properties", vec![]);
    for (key, value) in props.iter() {
        properties.call(
            "setProperty",
            vec![jvm.new_string(key), jvm.new_string(value)],
        );
    }
    properties
}

pub struct KafkaConnectionBuilderJava {
    jvm: Jvm,
    base_config: HashMap<String, String>,
}

impl KafkaConnectionBuilderJava {
    pub fn new(address: &str) -> Self {
        // specify maven dep for kafka-clients and all of its dependencies since j4rs does not support dependency resolution
        // The list of dependencies can be found here: https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.7.0/kafka-clients-3.7.0.pom
        // These are deployed to and loaded from a path like target/debug/jassets
        let jvm = Jvm::new(&[
            "org.apache.kafka:kafka-clients:3.7.0",
            "org.slf4j:slf4j-api:1.7.36",
            "org.slf4j:slf4j-simple:1.7.36",
        ]);

        let base_config = HashMap::from([("bootstrap.servers".to_owned(), address.to_owned())]);
        KafkaConnectionBuilderJava { jvm, base_config }
    }

    pub fn use_tls(mut self, certs: &str) -> Self {
        let conf = &mut self.base_config;
        conf.insert(
            "ssl.truststore.location".to_owned(),
            format!("{certs}/kafka.truststore.jks"),
        );
        conf.insert("ssl.truststore.password".to_owned(), "password".to_owned());
        conf.insert(
            "ssl.keystore.location".to_owned(),
            format!("{certs}/kafka.keystore.jks"),
        );
        conf.insert("ssl.keystore.password".to_owned(), "password".to_owned());
        conf.insert("security.protocol".to_owned(), "SSL".to_owned());

        self
    }

    pub fn use_sasl_scram(mut self, user: &str, pass: &str) -> Self {
        let conf = &mut self.base_config;
        conf.insert("sasl.mechanism".to_owned(), "SCRAM-SHA-256".to_owned());
        conf.insert("security.protocol".to_owned(), "SASL_PLAINTEXT".to_owned());
        conf.insert(
            "sasl.jaas.config".to_owned(),
            format!(r#"org.apache.kafka.common.security.scram.ScramLoginModule required username="{user}" password="{pass}";"#)
        );

        self
    }

    pub fn use_sasl_plain(mut self, user: &str, pass: &str) -> Self {
        let conf = &mut self.base_config;
        conf.insert("sasl.mechanism".to_owned(), "PLAIN".to_owned());
        conf.insert("security.protocol".to_owned(), "SASL_PLAINTEXT".to_owned());
        conf.insert(
            "sasl.jaas.config".to_owned(),
            format!(r#"org.apache.kafka.common.security.plain.PlainLoginModule required username="{user}" password="{pass}";"#)
        );

        self
    }

    pub async fn connect_producer(&self, acks: &str) -> KafkaProducerJava {
        let mut config = self.base_config.clone();
        config.insert("acks".to_owned(), acks.to_string());
        config.insert(
            "key.serializer".to_owned(),
            "org.apache.kafka.common.serialization.StringSerializer".to_owned(),
        );
        config.insert(
            "value.serializer".to_owned(),
            "org.apache.kafka.common.serialization.StringSerializer".to_owned(),
        );

        let properties = properties(&self.jvm, &config);
        let producer = self.jvm.construct(
            "org.apache.kafka.clients.producer.KafkaProducer",
            vec![properties],
        );

        let jvm = self.jvm.clone();
        KafkaProducerJava { jvm, producer }
    }

    pub async fn connect_consumer(&self, topic_name: &str, group: &str) -> KafkaConsumerJava {
        let mut config = self.base_config.clone();
        config.insert("group.id".to_owned(), group.to_owned());
        config.insert("session.timeout.ms".to_owned(), "6000".to_owned());
        config.insert("auto.offset.reset".to_owned(), "earliest".to_owned());
        config.insert("enable.auto.commit".to_owned(), "false".to_owned());
        config.insert(
            "key.deserializer".to_owned(),
            "org.apache.kafka.common.serialization.StringDeserializer".to_owned(),
        );
        config.insert(
            "value.deserializer".to_owned(),
            "org.apache.kafka.common.serialization.StringDeserializer".to_owned(),
        );

        let consumer = self.jvm.construct(
            "org.apache.kafka.clients.consumer.KafkaConsumer",
            vec![properties(&self.jvm, &config)],
        );
        consumer.call(
            "subscribe",
            vec![self
                .jvm
                .new_list("java.lang.String", vec![self.jvm.new_string(topic_name)])],
        );

        let jvm = self.jvm.clone();
        KafkaConsumerJava {
            jvm,
            consumer,
            waiting_records: VecDeque::new(),
        }
    }

    pub async fn connect_admin(&self) -> KafkaAdminJava {
        let admin = self.jvm.call_static(
            "org.apache.kafka.clients.admin.Admin",
            "create",
            vec![properties(&self.jvm, &self.base_config)],
        );
        let jvm = self.jvm.clone();
        KafkaAdminJava { jvm, admin }
    }
}
pub struct KafkaProducerJava {
    jvm: Jvm,
    producer: Value,
}

impl KafkaProducerJava {
    pub async fn assert_produce(&self, record: Record<'_>, expected_offset: Option<i64>) {
        let record = match record.key {
            Some(key) => self.jvm.construct(
                "org.apache.kafka.clients.producer.ProducerRecord",
                vec![
                    self.jvm.new_string(record.topic_name),
                    self.jvm.new_string(key),
                    self.jvm.new_string(record.payload),
                ],
            ),
            None => self.jvm.construct(
                "org.apache.kafka.clients.producer.ProducerRecord",
                vec![
                    self.jvm.new_string(record.topic_name),
                    self.jvm.new_string(record.payload),
                ],
            ),
        };
        let actual_offset: i64 = self
            .producer
            .call_async("send", vec![record])
            .await
            .cast("org.apache.kafka.clients.producer.RecordMetadata")
            .call("offset", vec![])
            .into_rust();

        if let Some(expected_offset) = expected_offset {
            assert_eq!(expected_offset, actual_offset);
        }
    }
}

pub struct KafkaConsumerJava {
    jvm: Jvm,
    consumer: Value,
    waiting_records: VecDeque<Value>,
}

impl KafkaConsumerJava {
    pub async fn consume(&mut self) -> ExpectedResponse {
        // This method asserts that we have consumed a single record from the broker.
        // Internally we may have actually received multiple records from the broker.
        // But that is hidden from the test by storing any extra messages for use in the next call to `consume`

        if self.waiting_records.is_empty() {
            self.fetch_from_broker();
        }

        self.pop_one_record()
    }

    fn fetch_from_broker(&mut self) {
        let timeout = self.jvm.call_static(
            "java.time.Duration",
            "ofSeconds",
            vec![self.jvm.new_long(30)],
        );

        let result = tokio::task::block_in_place(|| self.consumer.call("poll", vec![timeout]));

        for record in result.call("iterator", vec![]) {
            self.waiting_records
                .push_back(record.cast("org.apache.kafka.clients.consumer.ConsumerRecord"));
        }
    }

    fn pop_one_record(&mut self) -> ExpectedResponse {
        let record = self
            .waiting_records
            .pop_front()
            .expect("KafkaConsumer.poll timed out");

        let offset: i64 = record.call("offset", vec![]).into_rust();
        let topic_name: String = record.call("topic", vec![]).into_rust();
        let message: String = record.call("value", vec![]).into_rust();
        let key: Option<String> = record.call("key", vec![]).into_rust();

        ExpectedResponse {
            message,
            key,
            topic_name,
            offset: Some(offset),
        }
    }

    /// The offset to be committed should be lastProcessedMessageOffset + 1.
    pub fn commit(&self, offsets: &HashMap<TopicPartition, i64>) {
        let offsets_vec: Vec<(Value, Value)> = offsets
            .iter()
            .map(|(tp, offset)| {
                (
                    self.jvm.construct(
                        "org.apache.kafka.common.TopicPartition",
                        vec![
                            self.jvm.new_string(&tp.topic_name),
                            self.jvm.new_int(tp.partition),
                        ],
                    ),
                    self.jvm.construct(
                        "org.apache.kafka.clients.consumer.OffsetAndMetadata",
                        vec![self.jvm.new_long(*offset)],
                    ),
                )
            })
            .collect();
        let offsets_map = self.jvm.new_map(offsets_vec);

        tokio::task::block_in_place(|| self.consumer.call("commitSync", vec![offsets_map]));
    }

    pub fn committed_offsets(
        &self,
        partitions: HashSet<TopicPartition>,
    ) -> HashMap<TopicPartition, i64> {
        let mut offsets = HashMap::new();

        for tp in partitions {
            let topic_partition = self.jvm.construct(
                "org.apache.kafka.common.TopicPartition",
                vec![
                    self.jvm.new_string(&tp.topic_name),
                    self.jvm.new_int(tp.partition),
                ],
            );

            let timeout = self.jvm.call_static(
                "java.time.Duration",
                "ofSeconds",
                vec![self.jvm.new_long(30)],
            );

            let committed = tokio::task::block_in_place(|| {
                self.consumer
                    .call("committed", vec![topic_partition, timeout])
            });

            let offset: i64 = committed
                .cast("org.apache.kafka.clients.consumer.OffsetAndMetadata")
                .call("offset", vec![])
                .into_rust();
            offsets.insert(tp, offset);
        }
        offsets
    }
}

impl Drop for KafkaConsumerJava {
    fn drop(&mut self) {
        tokio::task::block_in_place(|| self.consumer.call("close", vec![]));
    }
}

pub struct KafkaAdminJava {
    jvm: Jvm,
    admin: Value,
}

impl KafkaAdminJava {
    pub async fn create_topics(&self, topics: &[NewTopic<'_>]) {
        self.create_topics_fallible(topics).await.unwrap();
    }

    pub async fn describe_topic(&self, topic_name: &str) -> Result<TopicDescription> {
        let topics = self
            .jvm
            .new_list("java.lang.String", vec![self.jvm.new_string(topic_name)]);

        self.admin
            .call("describeTopics", vec![topics])
            .call_async_fallible("allTopicNames", vec![])
            .await?;
        Ok(TopicDescription {})
    }

    pub async fn create_topics_fallible(&self, topics: &[NewTopic<'_>]) -> Result<()> {
        let topics: Vec<Value> = topics
            .iter()
            .map(|topic| {
                self.jvm.construct(
                    "org.apache.kafka.clients.admin.NewTopic",
                    vec![
                        self.jvm.new_string(topic.name),
                        self.jvm.new_int(topic.num_partitions),
                        self.jvm.new_short(topic.replication_factor),
                    ],
                )
            })
            .collect();
        let topics = self
            .jvm
            .new_list("org.apache.kafka.clients.admin.NewTopic", topics);

        self.admin
            .call("createTopics", vec![topics])
            .call_async_fallible("all", vec![])
            .await?;
        Ok(())
    }

    pub async fn delete_topics(&self, to_delete: &[&str]) {
        let to_delete: Vec<Value> = to_delete.iter().map(|x| self.jvm.new_string(x)).collect();
        let topics = self.jvm.new_list("java.lang.String", to_delete);

        self.admin
            .call("deleteTopics", vec![topics])
            .call_async("all", vec![])
            .await;
    }

    pub async fn create_partitions(&self, partitions: &[NewPartition<'_>]) {
        let partitions: Vec<(Value, Value)> = partitions
            .iter()
            .map(|partition| {
                (
                    self.jvm.new_string(partition.topic_name),
                    self.jvm.call_static(
                        "org.apache.kafka.clients.admin.NewPartitions",
                        "increaseTo",
                        vec![self.jvm.new_int(partition.new_partition_count)],
                    ),
                )
            })
            .collect();
        let partitions = self.jvm.new_map(partitions);

        self.admin
            .call("createPartitions", vec![partitions])
            .call_async("all", vec![])
            .await;
    }

    pub async fn describe_configs(&self, resources: &[ResourceSpecifier<'_>]) {
        let resource_type = self
            .jvm
            .class("org.apache.kafka.common.config.ConfigResource$Type");

        let resources: Vec<Value> = resources
            .iter()
            .map(|resource| {
                self.jvm.construct(
                    "org.apache.kafka.common.config.ConfigResource",
                    match resource {
                        ResourceSpecifier::Topic(topic) => {
                            vec![resource_type.field("TOPIC"), self.jvm.new_string(topic)]
                        }
                    },
                )
            })
            .collect();

        let resources = self
            .jvm
            .new_list("org.apache.kafka.common.config.ConfigResource", resources);

        self.admin
            .call("describeConfigs", vec![resources])
            .call_async("all", vec![])
            .await;
    }

    pub async fn alter_configs(&self, alter_configs: &[AlterConfig<'_>]) {
        let resource_type = self
            .jvm
            .class("org.apache.kafka.common.config.ConfigResource$Type");

        let alter_configs: Vec<_> = alter_configs
            .iter()
            .map(|alter_config| {
                let entries: Vec<Value> = alter_config
                    .entries
                    .iter()
                    .map(|entry| {
                        self.jvm.construct(
                            "org.apache.kafka.clients.admin.ConfigEntry",
                            vec![
                                self.jvm.new_string(&entry.key),
                                self.jvm.new_string(&entry.value),
                            ],
                        )
                    })
                    .collect();

                (
                    self.jvm.construct(
                        "org.apache.kafka.common.config.ConfigResource",
                        match &alter_config.specifier {
                            ResourceSpecifier::Topic(topic) => {
                                vec![resource_type.field("TOPIC"), self.jvm.new_string(topic)]
                            }
                        },
                    ),
                    self.jvm.construct(
                        "org.apache.kafka.clients.admin.Config",
                        vec![self
                            .jvm
                            .new_list("org.apache.kafka.clients.admin.ConfigEntry", entries)],
                    ),
                )
            })
            .collect();

        let alter_configs = self.jvm.new_map(alter_configs);

        self.admin
            .call("alterConfigs", vec![alter_configs])
            .call_async("all", vec![])
            .await;
    }

    pub async fn create_acls(&self, acls: Vec<Acl>) {
        let resource_type = self
            .jvm
            .class("org.apache.kafka.common.resource.ResourceType");
        let resource_pattern_type = self
            .jvm
            .class("org.apache.kafka.common.resource.PatternType");
        let acl_operation = self.jvm.class("org.apache.kafka.common.acl.AclOperation");
        let acl_permission_type = self
            .jvm
            .class("org.apache.kafka.common.acl.AclPermissionType");

        let acls: Vec<Value> = acls
            .iter()
            .map(|acl| {
                let resource_type_field = match acl.resource_type {
                    ResourceType::Cluster => "CLUSTER",
                    ResourceType::DelegationToken => "DELEGATION_TOKEN",
                    ResourceType::Group => "GROUP",
                    ResourceType::Topic => "TOPIC",
                    ResourceType::TransactionalId => "TRANSACTIONAL_ID",
                    ResourceType::User => "USER",
                };
                let resource_pattern_type_field = match acl.resource_pattern_type {
                    ResourcePatternType::Literal => "LITERAL",
                    ResourcePatternType::Prefixed => "PREFIXED",
                };
                let resource = self.jvm.construct(
                    "org.apache.kafka.common.resource.ResourcePattern",
                    vec![
                        resource_type.field(resource_type_field),
                        self.jvm.new_string(&acl.resource_name),
                        resource_pattern_type.field(resource_pattern_type_field),
                    ],
                );

                let acl_operation_field = match acl.operation {
                    AclOperation::All => "ALL",
                    AclOperation::Alter => "ALTER",
                    AclOperation::AlterConfigs => "ALTER_CONFIGS",
                    AclOperation::ClusterAction => "CLUSTER_ACTION",
                    AclOperation::Create => "CREATE",
                    AclOperation::CreateTokens => "CREATE_TOKENS",
                    AclOperation::Delete => "DELETE",
                    AclOperation::Describe => "DESCRIBE",
                    AclOperation::DescribeConfigs => "DESCRIBE_CONFIGS",
                    AclOperation::DescribeTokens => "DESCRIBE_TOKENS",
                    AclOperation::Read => "READ",
                    AclOperation::Write => "WRITE",
                };
                let acl_permission_type_field = match acl.permission_type {
                    AclPermissionType::Allow => "ALLOW",
                    AclPermissionType::Deny => "DENY",
                };
                let entry = self.jvm.construct(
                    "org.apache.kafka.common.acl.AccessControlEntry",
                    vec![
                        self.jvm.new_string(&acl.principal),
                        self.jvm.new_string(&acl.host),
                        acl_operation.field(acl_operation_field),
                        acl_permission_type.field(acl_permission_type_field),
                    ],
                );

                self.jvm.construct(
                    "org.apache.kafka.common.acl.AclBinding",
                    vec![resource, entry],
                )
            })
            .collect();

        let acls = self
            .jvm
            .new_list("org.apache.kafka.common.acl.AclBinding", acls);

        self.admin
            .call("createAcls", vec![acls])
            .call_async("all", vec![])
            .await;
    }
}
