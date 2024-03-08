use super::{AlterConfig, ExpectedResponse, NewPartition, NewTopic, Record, ResourceSpecifier};
use j4rs::{errors::J4RsError, Instance, InvocationArg, Jvm, JvmBuilder, MavenArtifact};
use std::{collections::HashMap, rc::Rc};

fn properties(jvm: &Jvm, props: &HashMap<String, String>) -> Instance {
    let properties = jvm.create_instance("java.util.Properties", &[]).unwrap();
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
                &[properties.into()],
            )
            .unwrap();
        KafkaProducerJava {
            _producer: producer,
        }
    }

    pub async fn connect_consumer(&self, _topic_name: &str) -> KafkaConsumerJava {
        KafkaConsumerJava {}
    }

    pub async fn connect_admin(&self) -> KafkaAdminJava {
        let properties = properties(&self.jvm, &self.base_config);
        let admin = self
            .jvm
            .invoke_static(
                "org.apache.kafka.clients.admin.Admin",
                "create",
                &[properties.into()],
            )
            .unwrap();
        let jvm = self.jvm.clone();
        KafkaAdminJava { jvm, admin }
    }
}
pub struct KafkaProducerJava {
    _producer: Instance,
}

impl KafkaProducerJava {
    pub async fn assert_produce(&self, _record: Record<'_>, _expected_offset: Option<i64>) {
        tracing::error!("Unimplemented assert");
    }
}

pub struct KafkaConsumerJava {}

impl KafkaConsumerJava {
    pub async fn assert_consume(&self, _response: ExpectedResponse<'_>) {
        tracing::error!("Unimplemented assert");
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
                        topic.name.try_into().unwrap(),
                        self.jvm
                            .invoke_static(
                                "java.util.Optional",
                                "of",
                                &[InvocationArg::try_from(topic.num_partitions).unwrap()],
                            )
                            .unwrap()
                            .into(),
                        self.jvm
                            .invoke_static(
                                "java.util.Optional",
                                "of",
                                &[InvocationArg::try_from(topic.replication_factor).unwrap()],
                            )
                            .unwrap()
                            .into(),
                        // TODO: can simplify to this once https://github.com/astonbitecode/j4rs/issues/91 is resolved
                        // InvocationArg::try_from(topic.num_partitions)
                        //     .unwrap()
                        //     .into_primitive()
                        //     .unwrap(),
                        // InvocationArg::try_from(topic.replication_factor)
                        //     .unwrap()
                        //     .into_primitive()
                        //     .unwrap(),
                    ],
                )
            })
            .collect();
        let topics = self
            .jvm
            .java_list("org.apache.kafka.clients.admin.NewTopic", topics)
            .unwrap();

        self.jvm
            .chain(&self.admin)
            .unwrap()
            .invoke("createTopics", &[topics.into()])
            .unwrap()
            .invoke("all", &[])
            .unwrap()
            .invoke("get", &[])
            .unwrap()
            .collect();
    }

    pub async fn delete_topics(&self, to_delete: &[&str]) {
        let topics = self
            .jvm
            .java_list("java.lang.String", to_delete.to_vec())
            .unwrap();

        self.jvm
            .chain(&self.admin)
            .unwrap()
            .invoke("deleteTopics", &[topics.into()])
            .unwrap()
            .invoke("all", &[])
            .unwrap()
            .invoke("get", &[])
            .unwrap()
            .collect();
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

        self.jvm
            .chain(&self.admin)
            .unwrap()
            .invoke("createPartitions", &[partitions.into()])
            .unwrap()
            .invoke("all", &[])
            .unwrap()
            .invoke("get", &[])
            .unwrap()
            .collect();
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

        self.jvm
            .chain(&self.admin)
            .unwrap()
            .invoke("describeConfigs", &[resources.into()])
            .unwrap()
            .invoke("all", &[])
            .unwrap()
            .invoke("get", &[])
            .unwrap()
            .collect();
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
                            &[self
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

        self.jvm
            .chain(&self.admin)
            .unwrap()
            .invoke("alterConfigs", &[alter_configs.into()])
            .unwrap()
            .invoke("all", &[])
            .unwrap()
            .invoke("get", &[])
            .unwrap()
            .collect();
    }

    fn java_map(&self, key_values: Vec<(Instance, Instance)>) -> Instance {
        let map = self.jvm.create_instance("java.util.HashMap", &[]).unwrap();
        for (k, v) in key_values {
            self.jvm.invoke(&map, "put", &[k.into(), v.into()]).unwrap();
        }
        map
    }
}
