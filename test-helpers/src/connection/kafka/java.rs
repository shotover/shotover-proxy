use super::{
    Acl, AclOperation, AclPermissionType, AlterConfig, ConsumerConfig, ExpectedResponse,
    ListOffsetsResultInfo, NewPartition, NewTopic, OffsetAndMetadata, OffsetSpec, ProduceResult,
    Record, ResourcePatternType, ResourceSpecifier, ResourceType, TopicDescription, TopicPartition,
    TopicPartitionInfo,
};
use crate::connection::java::{Jvm, Value};
use anyhow::Result;
use pretty_assertions::assert_eq;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    time::Duration,
};

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
        jvm.call_static(
            "java.lang.System",
            "setProperty",
            vec![
                jvm.new_string("org.slf4j.simpleLogger.defaultLogLevel"),
                jvm.new_string("WARN"),
            ],
        );

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

    pub async fn connect_producer(&self, acks: &str, linger_ms: u32) -> KafkaProducerJava {
        let mut config = self.base_config.clone();
        config.insert("acks".to_owned(), acks.to_string());
        config.insert("linger.ms".to_owned(), linger_ms.to_string());
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

    pub async fn connect_producer_with_transactions(
        &self,
        transaction_id: String,
    ) -> KafkaProducerJava {
        let mut config = self.base_config.clone();
        config.insert("acks".to_owned(), "all".to_owned());
        config.insert("linger.ms".to_owned(), "0".to_owned());
        config.insert("transactional.id".to_owned(), transaction_id);
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

        producer.call("initTransactions", vec![]);

        let jvm = self.jvm.clone();
        KafkaProducerJava { jvm, producer }
    }

    pub async fn connect_consumer(&self, consumer_config: ConsumerConfig) -> KafkaConsumerJava {
        let mut config = self.base_config.clone();
        config.insert("group.id".to_owned(), consumer_config.group);
        config.insert("session.timeout.ms".to_owned(), "6000".to_owned());
        config.insert("auto.offset.reset".to_owned(), "earliest".to_owned());
        config.insert("enable.auto.commit".to_owned(), "false".to_owned());
        config.insert(
            "isolation.level".to_owned(),
            consumer_config.isolation_level.as_str().to_owned(),
        );
        config.insert(
            "fetch.max.wait.ms".to_owned(),
            consumer_config.fetch_max_wait_ms.to_string(),
        );
        config.insert(
            "fetch.min.bytes".to_owned(),
            consumer_config.fetch_min_bytes.to_string(),
        );
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
            vec![self.jvm.new_list(
                "java.lang.String",
                consumer_config
                    .topic_names
                    .iter()
                    .map(|topic_name| self.jvm.new_string(topic_name))
                    .collect(),
            )],
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
    pub async fn assert_produce(
        &self,
        record: Record<'_>,
        expected_offset: Option<i64>,
    ) -> ProduceResult {
        let record = match record.key {
            Some(key) => self.jvm.construct(
                "org.apache.kafka.clients.producer.ProducerRecord",
                vec![
                    self.jvm.new_string(record.topic_name),
                    self.jvm.new_string(&key),
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
        let result = self
            .producer
            .call_async("send", vec![record])
            .await
            .cast("org.apache.kafka.clients.producer.RecordMetadata");

        let actual_offset: i64 = result.call("offset", vec![]).into_rust();
        let actual_partition: i32 = result.call("partition", vec![]).into_rust();

        if let Some(expected_offset) = expected_offset {
            assert_eq!(expected_offset, actual_offset);
        }

        ProduceResult {
            partition: actual_partition,
        }
    }

    pub fn begin_transaction(&self) {
        self.producer.call("beginTransaction", vec![]);
    }

    pub fn commit_transaction(&self) {
        self.producer.call("commitTransaction", vec![]);
    }

    pub fn abort_transaction(&self) {
        self.producer.call("abortTransaction", vec![]);
    }

    pub fn send_offsets_to_transaction(
        &self,
        consumer: &KafkaConsumerJava,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
    ) {
        let offsets = self.jvm.new_map(
            offsets
                .into_iter()
                .map(|(tp, offset_and_metadata)| {
                    (
                        create_topic_partition(&self.jvm, &tp),
                        self.jvm.construct(
                            "org.apache.kafka.clients.consumer.OffsetAndMetadata",
                            vec![self.jvm.new_long(offset_and_metadata.offset)],
                        ),
                    )
                })
                .collect(),
        );

        let consumer_group_id = consumer
            .consumer
            .call("groupMetadata", vec![])
            .call("groupId", vec![]);

        self.producer
            .call("sendOffsetsToTransaction", vec![offsets, consumer_group_id]);
    }
}

pub struct KafkaConsumerJava {
    jvm: Jvm,
    consumer: Value,
    waiting_records: VecDeque<Value>,
}

impl KafkaConsumerJava {
    pub async fn assert_no_consume_within_timeout(&mut self, timeout: Duration) {
        if !self.waiting_records.is_empty() {
            panic!(
                "There are pending records from the previous consume {:?}",
                self.waiting_records
            );
        }

        self.fetch_from_broker(timeout).await;

        if !self.waiting_records.is_empty() {
            panic!(
                "Records were received from this consume {:?}",
                self.waiting_records
            );
        }
    }
    pub async fn consume(&mut self, timeout: Duration) -> ExpectedResponse {
        // This method asserts that we have consumed a single record from the broker.
        // Internally we may have actually received multiple records from the broker.
        // But that is hidden from the test by storing any extra messages for use in the next call to `consume`

        if self.waiting_records.is_empty() {
            self.fetch_from_broker(timeout).await;
        }

        self.pop_one_record()
    }

    pub async fn fetch_from_broker(&mut self, timeout: Duration) {
        let instant = std::time::Instant::now();
        let mut finished = false;
        while !finished {
            let java_timeout = self.jvm.call_static(
                "java.time.Duration",
                "ofSeconds",
                vec![self.jvm.new_long(timeout.as_secs() as i64)],
            );
            // the poll method documentation claims that it will await the timeout if there are no records available.
            // but it will actually return immediately if it receives a transaction control record,
            // so we need to wrap it in our own timeout logic.
            let result =
                tokio::task::block_in_place(|| self.consumer.call("poll", vec![java_timeout]));

            for record in result.call("iterator", vec![]) {
                self.waiting_records
                    .push_back(record.cast("org.apache.kafka.clients.consumer.ConsumerRecord"));
                // finished because at least one record was received
                finished = true;
            }

            if instant.elapsed() > timeout {
                // finished because timeout elapsed
                finished = true;
            }
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
                    create_topic_partition(&self.jvm, tp),
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
            let topic_partition = create_topic_partition(&self.jvm, &tp);

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

    pub async fn describe_topics(&self, topic_names: &[&str]) -> Result<Vec<TopicDescription>> {
        let topics = self.jvm.new_list(
            "java.lang.String",
            topic_names
                .iter()
                .map(|topic| self.jvm.new_string(topic))
                .collect(),
        );

        let topic_names_to_info = self
            .admin
            .call("describeTopics", vec![topics])
            .call_async_fallible("allTopicNames", vec![])
            .await?;

        Ok(topic_names_to_info
            .call("values", vec![])
            .call("iterator", vec![])
            .into_iter()
            .map(|java_topic_description| {
                let java_topic_description =
                    java_topic_description.cast("org.apache.kafka.clients.admin.TopicDescription");
                TopicDescription {
                    topic_name: java_topic_description.call("name", vec![]).into_rust(),
                    partitions: java_topic_description
                        .call("partitions", vec![])
                        .call("iterator", vec![])
                        .into_iter()
                        .map(|_| TopicPartitionInfo {})
                        .collect(),
                }
            })
            .collect())
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

    pub async fn delete_groups(&self, to_delete: &[&str]) {
        let to_delete: Vec<Value> = to_delete.iter().map(|x| self.jvm.new_string(x)).collect();
        let topics = self.jvm.new_list("java.lang.String", to_delete);

        self.admin
            .call("deleteConsumerGroups", vec![topics])
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

    pub async fn list_offsets(
        &self,
        topic_partitions: HashMap<TopicPartition, OffsetSpec>,
    ) -> HashMap<TopicPartition, ListOffsetsResultInfo> {
        let offset_spec_class = "org.apache.kafka.clients.admin.OffsetSpec";
        let topic_partitions_java: Vec<_> = topic_partitions
            .iter()
            .map(|(topic_partition, offset_spec)| {
                (
                    create_topic_partition(&self.jvm, topic_partition),
                    match offset_spec {
                        OffsetSpec::Earliest => {
                            self.jvm.call_static(offset_spec_class, "earliest", vec![])
                        }
                        OffsetSpec::Latest => {
                            self.jvm.call_static(offset_spec_class, "latest", vec![])
                        }
                    },
                )
            })
            .collect();
        let topic_partitions_java = self.jvm.new_map(topic_partitions_java);

        let java_results = self
            .admin
            .call("listOffsets", vec![topic_partitions_java])
            .call_async("all", vec![])
            .await;

        let mut results = HashMap::new();
        for topic_partition in topic_partitions.into_keys() {
            let result = java_results
                .call(
                    "get",
                    vec![create_topic_partition(&self.jvm, &topic_partition)],
                )
                .cast("org.apache.kafka.clients.admin.ListOffsetsResult$ListOffsetsResultInfo");
            let offset: i32 = result.call("offset", vec![]).into_rust();
            results.insert(topic_partition, ListOffsetsResultInfo { offset });
        }
        results
    }

    pub async fn list_groups(&self) -> Vec<String> {
        let java_results = self
            .admin
            .call("listConsumerGroups", vec![])
            .call_async("all", vec![])
            .await;

        let mut results = vec![];
        for java_group in java_results.call("iterator", vec![]).into_iter() {
            results.push(
                java_group
                    .cast("org.apache.kafka.clients.admin.ConsumerGroupListing")
                    .call("groupId", vec![])
                    .into_rust(),
            )
        }
        results
    }

    pub async fn list_transactions(&self) -> Vec<String> {
        let java_results = self
            .admin
            .call("listTransactions", vec![])
            .call_async("all", vec![])
            .await;

        let mut results = vec![];
        for java_group in java_results.call("iterator", vec![]).into_iter() {
            results.push(
                java_group
                    .cast("org.apache.kafka.clients.admin.TransactionListing")
                    .call("transactionalId", vec![])
                    .into_rust(),
            )
        }
        results
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

fn create_topic_partition(jvm: &Jvm, tp: &TopicPartition) -> Value {
    jvm.construct(
        "org.apache.kafka.common.TopicPartition",
        vec![jvm.new_string(&tp.topic_name), jvm.new_int(tp.partition)],
    )
}
