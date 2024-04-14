use super::{AlterConfig, ExpectedResponse, NewPartition, NewTopic, Record, ResourceSpecifier};
use j4rs::{errors::J4RsError, Instance, InvocationArg, Jvm, JvmBuilder, MavenArtifact};
use std::{
    collections::{HashMap, VecDeque},
    rc::Rc,
};

fn properties(jvm: &Jvm, props: &HashMap<String, String>) -> Instance {
    let properties = jvm
        .create_instance("java.util.Properties", InvocationArg::empty())
        .unwrap();
    for (key, value) in props.iter() {
        jvm.invoke(
            &properties,
            "setProperty",
            &[
                InvocationArg::try_from(key).unwrap(),
                InvocationArg::try_from(value).unwrap(),
            ],
        )
        .unwrap();
    }
    properties
}

pub struct KafkaConnectionBuilderJava {
    jvm: Rc<Jvm>,
    base_config: HashMap<String, String>,
}

impl KafkaConnectionBuilderJava {
    pub fn new(address: &str) -> Self {
        let jvm = Rc::new(JvmBuilder::new().build().unwrap());

        // specify maven dep for kafka-clients and all of its dependencies since j4rs does not support dependency resolution
        // The list of dependencies can be found here: https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.7.0/kafka-clients-3.7.0.pom6
        // These are deployed to and loaded from a path like target/debug/jassets
        jvm.deploy_artifact(&MavenArtifact::from("org.apache.kafka:kafka-clients:3.7.0"))
            .unwrap();
        jvm.deploy_artifact(&MavenArtifact::from("org.slf4j:slf4j-api:1.7.36"))
            .unwrap();
        jvm.deploy_artifact(&MavenArtifact::from("org.slf4j:slf4j-simple:1.7.36"))
            .unwrap();

        let base_config = HashMap::from([("bootstrap.servers".to_owned(), address.to_owned())]);
        KafkaConnectionBuilderJava { jvm, base_config }
    }

    pub fn use_sasl(mut self, user: &str, pass: &str) -> Self {
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
        let producer = self
            .jvm
            .create_instance(
                "org.apache.kafka.clients.producer.KafkaProducer",
                &[&properties.into()],
            )
            .unwrap();

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

        let properties = properties(&self.jvm, &config);
        let consumer = self
            .jvm
            .create_instance(
                "org.apache.kafka.clients.consumer.KafkaConsumer",
                &[&properties.into()],
            )
            .unwrap();
        self.jvm
            .invoke(
                &consumer,
                "subscribe",
                &[&self
                    .jvm
                    .java_list("java.lang.String", vec![topic_name])
                    .unwrap()
                    .into()],
            )
            .unwrap();

        let jvm = self.jvm.clone();
        KafkaConsumerJava {
            jvm,
            consumer,
            waiting_records: VecDeque::new(),
        }
    }

    pub async fn connect_admin(&self) -> KafkaAdminJava {
        let properties = properties(&self.jvm, &self.base_config);
        let admin = self
            .jvm
            .invoke_static(
                "org.apache.kafka.clients.admin.Admin",
                "create",
                &[&properties.into()],
            )
            .unwrap();
        let jvm = self.jvm.clone();
        KafkaAdminJava { jvm, admin }
    }
}
pub struct KafkaProducerJava {
    jvm: Rc<Jvm>,
    producer: Instance,
}

impl KafkaProducerJava {
    pub async fn assert_produce(&self, record: Record<'_>, expected_offset: Option<i64>) {
        let record = match record.key {
            Some(key) => self
                .jvm
                .create_instance(
                    "org.apache.kafka.clients.producer.ProducerRecord",
                    &[
                        InvocationArg::try_from(record.topic_name).unwrap(),
                        InvocationArg::try_from(key).unwrap(),
                        InvocationArg::try_from(record.payload).unwrap(),
                    ],
                )
                .unwrap(),
            None => self
                .jvm
                .create_instance(
                    "org.apache.kafka.clients.producer.ProducerRecord",
                    &[
                        InvocationArg::try_from(record.topic_name).unwrap(),
                        InvocationArg::try_from(record.payload).unwrap(),
                    ],
                )
                .unwrap(),
        };
        let result = self
            .jvm
            .invoke_async(&self.producer, "send", &[record.into()])
            .await
            .unwrap();
        let actual_offset: i64 = self
            .jvm
            .chain(&result)
            .unwrap()
            .cast("org.apache.kafka.clients.producer.RecordMetadata")
            .unwrap()
            .invoke("offset", &[])
            .unwrap()
            .to_rust()
            .unwrap();

        if let Some(expected_offset) = expected_offset {
            assert_eq!(expected_offset, actual_offset);
        }
    }
}

pub struct KafkaConsumerJava {
    jvm: Rc<Jvm>,
    consumer: Instance,
    waiting_records: VecDeque<Instance>,
}

impl KafkaConsumerJava {
    pub async fn assert_consume(&mut self, expected_response: ExpectedResponse<'_>) {
        // This method asserts that we have consumed a single record from the broker.
        // Internally we may have actually received multiple records from the broker.
        // But that is hidden from the test by storing any extra messages for use in the next call to `assert_consume`

        if self.waiting_records.is_empty() {
            self.fetch_from_broker();
        }

        self.process_one_fetched_record(expected_response);
    }

    fn fetch_from_broker(&mut self) {
        let timeout = self
            .jvm
            .invoke_static(
                "java.time.Duration",
                "ofSeconds",
                &[InvocationArg::try_from(30_i64)
                    .unwrap()
                    .into_primitive()
                    .unwrap()],
            )
            .unwrap();

        let result = tokio::task::block_in_place(|| {
            self.jvm
                .invoke(&self.consumer, "poll", &[&timeout.into()])
                .unwrap()
        });

        let iterator = JavaIterator::new(
            self.jvm
                .invoke(&result, "iterator", InvocationArg::empty())
                .unwrap(),
        );
        while let Some(record) = iterator.next(&self.jvm) {
            let record = self
                .jvm
                .cast(&record, "org.apache.kafka.clients.consumer.ConsumerRecord")
                .unwrap();
            self.waiting_records.push_front(record);
        }
    }

    fn process_one_fetched_record(&mut self, expected_response: ExpectedResponse<'_>) {
        let record = self
            .waiting_records
            .pop_back()
            .expect("KafkaConsumer.poll timed out");

        let offset: i64 = self
            .jvm
            .chain(&record)
            .unwrap()
            .invoke("offset", &[])
            .unwrap()
            .to_rust()
            .unwrap();
        assert_eq!(expected_response.offset, offset);

        let topic: String = self
            .jvm
            .chain(&record)
            .unwrap()
            .invoke("topic", &[])
            .unwrap()
            .to_rust()
            .unwrap();
        assert_eq!(expected_response.topic_name, topic);

        let value: String = self
            .jvm
            .chain(&record)
            .unwrap()
            .invoke("value", &[])
            .unwrap()
            .to_rust()
            .unwrap();
        assert_eq!(expected_response.message, value);

        let key: Option<String> = self
            .jvm
            .chain(&record)
            .unwrap()
            .invoke("key", &[])
            .unwrap()
            .to_rust()
            .unwrap();
        assert_eq!(expected_response.key, key.as_deref());
    }
}

impl Drop for KafkaConsumerJava {
    fn drop(&mut self) {
        tokio::task::block_in_place(|| {
            self.jvm
                .invoke(&self.consumer, "close", InvocationArg::empty())
                .unwrap()
        });
    }
}

pub struct KafkaAdminJava {
    jvm: Rc<Jvm>,
    admin: Instance,
}

impl KafkaAdminJava {
    pub async fn create_topics(&self, topics: &[NewTopic<'_>]) {
        let topics: Vec<_> = topics
            .iter()
            .map(|topic| {
                self.jvm.create_instance(
                    "org.apache.kafka.clients.admin.NewTopic",
                    &[
                        &topic.name.try_into().unwrap(),
                        &InvocationArg::try_from(topic.num_partitions)
                            .unwrap()
                            .into_primitive()
                            .unwrap(),
                        &InvocationArg::try_from(topic.replication_factor)
                            .unwrap()
                            .into_primitive()
                            .unwrap(),
                    ],
                )
            })
            .collect();
        let topics = self
            .jvm
            .java_list("org.apache.kafka.clients.admin.NewTopic", topics)
            .unwrap();

        let result = self
            .jvm
            .invoke(&self.admin, "createTopics", &[&topics.into()])
            .unwrap();
        self.jvm.invoke_async(&result, "all", &[]).await.unwrap();
    }

    pub async fn delete_topics(&self, to_delete: &[&str]) {
        let topics = self
            .jvm
            .java_list("java.lang.String", to_delete.to_vec())
            .unwrap();

        let result = self
            .jvm
            .invoke(&self.admin, "deleteTopics", &[&topics.into()])
            .unwrap();
        self.jvm.invoke_async(&result, "all", &[]).await.unwrap();
    }

    pub async fn create_partitions(&self, partitions: &[NewPartition<'_>]) {
        let partitions: HashMap<_, _> = partitions
            .iter()
            .map(|partition| {
                (
                    partition.topic_name,
                    self.jvm.invoke_static(
                        "org.apache.kafka.clients.admin.NewPartitions",
                        "increaseTo",
                        &[InvocationArg::try_from(partition.new_partition_count)
                            .unwrap()
                            .into_primitive()
                            .unwrap()],
                    ),
                )
            })
            .collect();
        let partitions = self
            .jvm
            .java_map(
                "java.lang.String",
                "org.apache.kafka.clients.admin.NewTopic",
                partitions,
            )
            .unwrap();

        let result = self
            .jvm
            .invoke(&self.admin, "createPartitions", &[&partitions.into()])
            .unwrap();
        self.jvm.invoke_async(&result, "all", &[]).await.unwrap();
    }

    pub async fn describe_configs(&self, resources: &[ResourceSpecifier<'_>]) {
        let resource_type = self
            .jvm
            .static_class("org.apache.kafka.common.config.ConfigResource$Type")
            .unwrap();

        let resources: Vec<_> = resources
            .iter()
            .map(|resource| {
                self.jvm.create_instance(
                    "org.apache.kafka.common.config.ConfigResource",
                    &match resource {
                        ResourceSpecifier::Topic(topic) => [
                            self.jvm.field(&resource_type, "TOPIC").unwrap().into(),
                            InvocationArg::try_from(*topic).unwrap(),
                        ],
                    },
                )
            })
            .collect();

        let resources = self
            .jvm
            .java_list("org.apache.kafka.common.config.ConfigResource", resources)
            .unwrap();

        let result = self
            .jvm
            .invoke(&self.admin, "describeConfigs", &[&resources.into()])
            .unwrap();
        self.jvm.invoke_async(&result, "all", &[]).await.unwrap();
    }

    pub async fn alter_configs(&self, alter_configs: &[AlterConfig<'_>]) {
        let resource_type = self
            .jvm
            .static_class("org.apache.kafka.common.config.ConfigResource$Type")
            .unwrap();

        let alter_configs: Vec<_> = alter_configs
            .iter()
            .map(|alter_config| {
                let entries: Vec<Result<Instance, J4RsError>> = alter_config
                    .entries
                    .iter()
                    .map(|entry| {
                        self.jvm.create_instance(
                            "org.apache.kafka.clients.admin.ConfigEntry",
                            &[
                                InvocationArg::try_from(entry.key.as_str()).unwrap(),
                                InvocationArg::try_from(entry.value.as_str()).unwrap(),
                            ],
                        )
                    })
                    .collect();
                (
                    self.jvm
                        .create_instance(
                            "org.apache.kafka.common.config.ConfigResource",
                            &match &alter_config.specifier {
                                ResourceSpecifier::Topic(topic) => [
                                    self.jvm.field(&resource_type, "TOPIC").unwrap().into(),
                                    InvocationArg::try_from(*topic).unwrap(),
                                ],
                            },
                        )
                        .unwrap(),
                    self.jvm
                        .create_instance(
                            "org.apache.kafka.clients.admin.Config",
                            &[&self
                                .jvm
                                .java_list("org.apache.kafka.clients.admin.ConfigEntry", entries)
                                .unwrap()
                                .into()],
                        )
                        .unwrap(),
                )
            })
            .collect();

        let alter_configs = self.java_map(alter_configs);

        let result = self
            .jvm
            .invoke(&self.admin, "alterConfigs", &[&alter_configs.into()])
            .unwrap();
        self.jvm
            .invoke_async(&result, "all", InvocationArg::empty())
            .await
            .unwrap();
    }

    fn java_map(&self, key_values: Vec<(Instance, Instance)>) -> Instance {
        let map = self
            .jvm
            .create_instance("java.util.HashMap", InvocationArg::empty())
            .unwrap();
        for (k, v) in key_values {
            self.jvm
                .invoke(&map, "put", &[&k.into(), &v.into()])
                .unwrap();
        }
        map
    }
}

struct JavaIterator(Instance);

impl JavaIterator {
    fn new(iterator_instance: Instance) -> Self {
        JavaIterator(iterator_instance)
    }

    fn next(&self, jvm: &Jvm) -> Option<Instance> {
        let has_next: bool = jvm
            .to_rust(
                jvm.invoke(&self.0, "hasNext", InvocationArg::empty())
                    .unwrap(),
            )
            .unwrap();
        if has_next {
            Some(jvm.invoke(&self.0, "next", InvocationArg::empty()).unwrap())
        } else {
            None
        }
    }
}
