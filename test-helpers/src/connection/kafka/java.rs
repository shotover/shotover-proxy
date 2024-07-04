use super::{
    Acl, AclOperation, AclPermissionType, AlterConfig, ExpectedResponse, NewPartition, NewTopic,
    Record, ResourcePatternType, ResourceSpecifier, ResourceType, TopicDescription,
};
use crate::connection::java::{Java, Value};
use anyhow::Result;
use j4rs::InvocationArg;
use pretty_assertions::assert_eq;
use std::collections::{HashMap, VecDeque};

fn properties(jvm: &Java, props: &HashMap<String, String>) -> Value {
    let properties = jvm.construct("java.util.Properties", vec![]);
    for (key, value) in props.iter() {
        properties.call(
            "setProperty",
            &[
                &InvocationArg::try_from(key).unwrap(),
                &InvocationArg::try_from(value).unwrap(),
            ],
        );
    }
    properties
}

pub struct KafkaConnectionBuilderJava {
    jvm: Java,
    base_config: HashMap<String, String>,
}

impl KafkaConnectionBuilderJava {
    pub fn new(address: &str) -> Self {
        // specify maven dep for kafka-clients and all of its dependencies since j4rs does not support dependency resolution
        // The list of dependencies can be found here: https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.7.0/kafka-clients-3.7.0.pom
        // These are deployed to and loaded from a path like target/debug/jassets
        let jvm = Java::new(&[
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

    pub async fn connect_producer(&self, acks: i32) -> KafkaProducerJava {
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

    pub async fn connect_consumer(&self, topic_name: &str) -> KafkaConsumerJava {
        let mut config = self.base_config.clone();
        config.insert("group.id".to_owned(), "some_group".to_owned());
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
            &[&self
                .jvm
                .new_list(
                    "java.lang.String",
                    &[InvocationArg::try_from(topic_name).unwrap()],
                )
                .into()],
        );

        let jvm = self.jvm.clone();
        KafkaConsumerJava {
            jvm,
            consumer,
            waiting_records: VecDeque::new(),
        }
    }

    pub async fn connect_admin(&self) -> KafkaAdminJava {
        let properties = properties(&self.jvm, &self.base_config);
        let admin = self.jvm.invoke_static(
            "org.apache.kafka.clients.admin.Admin",
            "create",
            &[&properties.into()],
        );
        let jvm = self.jvm.clone();
        KafkaAdminJava { jvm, admin }
    }
}
pub struct KafkaProducerJava {
    jvm: Java,
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
            .call_async("send", &[record.into()])
            .await
            .cast("org.apache.kafka.clients.producer.RecordMetadata")
            .call("offset", &[])
            .into_rust();

        if let Some(expected_offset) = expected_offset {
            assert_eq!(expected_offset, actual_offset);
        }
    }
}

pub struct KafkaConsumerJava {
    jvm: Java,
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
        let timeout = self.jvm.invoke_static(
            "java.time.Duration",
            "ofSeconds",
            &[&self.jvm.new_long(30_i64).instance.into()],
        );

        let result = tokio::task::block_in_place(|| self.consumer.call("poll", &[&timeout.into()]));

        for record in result.call("iterator", &[]) {
            self.waiting_records
                .push_back(record.cast("org.apache.kafka.clients.consumer.ConsumerRecord"));
        }
    }

    fn pop_one_record(&mut self) -> ExpectedResponse {
        let record = self
            .waiting_records
            .pop_front()
            .expect("KafkaConsumer.poll timed out");

        let offset: i64 = record.call("offset", &[]).into_rust();
        let topic_name: String = record.call("topic", &[]).into_rust();
        let message: String = record.call("value", &[]).into_rust();
        let key: Option<String> = record.call("key", &[]).into_rust();

        ExpectedResponse {
            message,
            key,
            topic_name,
            offset: Some(offset),
        }
    }
}

impl Drop for KafkaConsumerJava {
    fn drop(&mut self) {
        tokio::task::block_in_place(|| self.consumer.call("close", &[]));
    }
}

pub struct KafkaAdminJava {
    jvm: Java,
    admin: Value,
}

impl KafkaAdminJava {
    pub async fn create_topics(&self, topics: &[NewTopic<'_>]) {
        self.create_topics_fallible(topics).await.unwrap();
    }

    pub async fn describe_topic(&self, topic_name: &str) -> Result<TopicDescription> {
        let topics = self.jvm.new_list(
            "java.lang.String",
            &[InvocationArg::try_from(topic_name).unwrap()],
        );

        self.admin
            .call("describeTopics", &[&topics.into()])
            .call_async_fallible("allTopicNames", &[])
            .await?;
        Ok(TopicDescription {})
    }

    pub async fn create_topics_fallible(&self, topics: &[NewTopic<'_>]) -> Result<()> {
        let topics: Vec<InvocationArg> = topics
            .iter()
            .map(|topic| {
                self.jvm
                    .construct(
                        "org.apache.kafka.clients.admin.NewTopic",
                        vec![
                            self.jvm.new_string(topic.name),
                            self.jvm.new_int(topic.num_partitions),
                            self.jvm.new_short(topic.replication_factor),
                        ],
                    )
                    .into()
            })
            .collect();
        let topics = self
            .jvm
            .new_list("org.apache.kafka.clients.admin.NewTopic", &topics);

        self.admin
            .call("createTopics", &[&topics.into()])
            .call_async_fallible("all", &[])
            .await?;
        Ok(())
    }

    pub async fn delete_topics(&self, to_delete: &[&str]) {
        let to_delete: Vec<InvocationArg> = to_delete
            .iter()
            .map(|x| InvocationArg::try_from(*x).unwrap())
            .collect();
        let topics = self.jvm.new_list("java.lang.String", &to_delete);

        self.admin
            .call("deleteTopics", &[&topics.into()])
            .call_async("all", &[])
            .await;
    }

    pub async fn create_partitions(&self, partitions: &[NewPartition<'_>]) {
        // TODO: fix this
        let partitions: Vec<(Value, Value)> = partitions
            .iter()
            .map(|partition| {
                (
                    self.jvm.new_string(partition.topic_name),
                    self.jvm.invoke_static(
                        "org.apache.kafka.clients.admin.NewPartitions",
                        "increaseTo",
                        &[&InvocationArg::try_from(partition.new_partition_count)
                            .unwrap()
                            .into_primitive()
                            .unwrap()],
                    ),
                )
            })
            .collect();
        let partitions = self.jvm.new_map(partitions);

        self.admin
            .call("createPartitions", &[&partitions.into()])
            .call_async("all", &[])
            .await;
    }

    pub async fn describe_configs(&self, resources: &[ResourceSpecifier<'_>]) {
        let resource_type = self
            .jvm
            .static_class("org.apache.kafka.common.config.ConfigResource$Type");

        let resources: Vec<InvocationArg> = resources
            .iter()
            .map(|resource| {
                self.jvm
                    .construct(
                        "org.apache.kafka.common.config.ConfigResource",
                        match resource {
                            ResourceSpecifier::Topic(topic) => {
                                vec![resource_type.field("TOPIC"), self.jvm.new_string(topic)]
                            }
                        },
                    )
                    .into()
            })
            .collect();

        let resources = self
            .jvm
            .new_list("org.apache.kafka.common.config.ConfigResource", &resources);

        self.admin
            .call("describeConfigs", &[&resources.into()])
            .call_async("all", &[])
            .await;
    }

    pub async fn alter_configs(&self, alter_configs: &[AlterConfig<'_>]) {
        let resource_type = self
            .jvm
            .static_class("org.apache.kafka.common.config.ConfigResource$Type");

        let alter_configs: Vec<_> = alter_configs
            .iter()
            .map(|alter_config| {
                let entries: Vec<InvocationArg> = alter_config
                    .entries
                    .iter()
                    .map(|entry| {
                        self.jvm
                            .construct(
                                "org.apache.kafka.clients.admin.ConfigEntry",
                                vec![
                                    self.jvm.new_string(&entry.key),
                                    self.jvm.new_string(&entry.value),
                                ],
                            )
                            .into()
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
                            .new_list("org.apache.kafka.clients.admin.ConfigEntry", &entries)],
                    ),
                )
            })
            .collect();

        let alter_configs = self.jvm.new_map(alter_configs);

        self.admin
            .call("alterConfigs", &[&alter_configs.into()])
            .call_async("all", &[])
            .await;
    }

    pub async fn create_acls(&self, acls: Vec<Acl>) {
        let resource_type = self
            .jvm
            .static_class("org.apache.kafka.common.resource.ResourceType");
        let resource_pattern_type = self
            .jvm
            .static_class("org.apache.kafka.common.resource.PatternType");
        let acl_operation = self
            .jvm
            .static_class("org.apache.kafka.common.acl.AclOperation");
        let acl_permission_type = self
            .jvm
            .static_class("org.apache.kafka.common.acl.AclPermissionType");

        let acls: Vec<InvocationArg> = acls
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

                self.jvm
                    .construct(
                        "org.apache.kafka.common.acl.AclBinding",
                        vec![resource, entry],
                    )
                    .into()
            })
            .collect();

        let acls = self
            .jvm
            .new_list("org.apache.kafka.common.acl.AclBinding", &acls);

        self.admin
            .call("createAcls", &[&acls.into()])
            .call_async("all", &[])
            .await;
    }
}
