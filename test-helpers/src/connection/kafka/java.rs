use super::{ExpectedResponse, Record};
use j4rs::{Instance, InvocationArg, Jvm, JvmBuilder, MavenArtifact};
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

    pub fn use_sasl(self, _user: &str, _pass: &str) -> Self {
        tracing::error!("Unimplemented test case");
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

    pub async fn connect_admin(&self) -> Instance {
        let properties = properties(&self.jvm, &self.base_config);
        self.jvm
            .invoke_static(
                "org.apache.kafka.clients.admin.Admin",
                "create",
                &[properties.into()],
            )
            .unwrap()
    }

    pub async fn admin_setup(&self) {
        let admin = self.connect_admin().await;
        create_topics(
            &self.jvm,
            &admin,
            &[
                NewTopic {
                    name: "partitions1",
                    num_partitions: 1,
                    replication_factor: 1,
                },
                NewTopic {
                    name: "paritions3",
                    num_partitions: 3,
                    replication_factor: 1,
                },
                NewTopic {
                    name: "acks0",
                    num_partitions: 1,
                    replication_factor: 1,
                },
                NewTopic {
                    name: "to_delete",
                    num_partitions: 1,
                    replication_factor: 1,
                },
            ],
        )
        .await;
    }

    pub async fn admin_cleanup(&self) {
        self.connect_admin().await;
    }
}

struct NewTopic<'a> {
    name: &'a str,
    num_partitions: i32,
    replication_factor: i16,
}

async fn create_topics(jvm: &Jvm, admin_client: &Instance, topics: &[NewTopic<'_>]) {
    let topics: Vec<_> = topics
        .iter()
        .map(|topic| {
            jvm.create_instance(
                "org.apache.kafka.clients.admin.NewTopic",
                &[
                    topic.name.try_into().unwrap(),
                    jvm.invoke_static(
                        "java.util.Optional",
                        "of",
                        &[InvocationArg::try_from(topic.num_partitions).unwrap()],
                    )
                    .unwrap()
                    .into(),
                    jvm.invoke_static(
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
    let topics = jvm
        .java_list("org.apache.kafka.clients.admin.NewTopic", topics)
        .unwrap();

    jvm.invoke(admin_client, "createTopics", &[topics.into()])
        .unwrap();
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
