use pretty_assertions::assert_eq;

#[cfg(feature = "kafka-cpp-driver-tests")]
pub mod cpp;
pub mod java;

use anyhow::Result;
#[cfg(feature = "kafka-cpp-driver-tests")]
use cpp::*;
use java::*;

#[derive(Clone, Copy)]
pub enum KafkaDriver {
    #[cfg(feature = "kafka-cpp-driver-tests")]
    Cpp,
    Java,
}

pub enum KafkaConnectionBuilder {
    #[cfg(feature = "kafka-cpp-driver-tests")]
    Cpp(KafkaConnectionBuilderCpp),
    Java(KafkaConnectionBuilderJava),
}

impl KafkaConnectionBuilder {
    pub fn new(driver: KafkaDriver, address: &str) -> Self {
        match driver {
            #[cfg(feature = "kafka-cpp-driver-tests")]
            KafkaDriver::Cpp => Self::Cpp(KafkaConnectionBuilderCpp::new(address)),
            KafkaDriver::Java => Self::Java(KafkaConnectionBuilderJava::new(address)),
        }
    }

    pub fn use_tls(self, certs: &str) -> Self {
        match self {
            #[cfg(feature = "kafka-cpp-driver-tests")]
            Self::Cpp(_) => todo!("TLS not implemented for cpp driver"),
            Self::Java(java) => Self::Java(java.use_tls(certs)),
        }
    }

    pub fn use_sasl_scram(self, user: &str, pass: &str) -> Self {
        match self {
            #[cfg(feature = "kafka-cpp-driver-tests")]
            Self::Cpp(cpp) => Self::Cpp(cpp.use_sasl_scram(user, pass)),
            Self::Java(java) => Self::Java(java.use_sasl_scram(user, pass)),
        }
    }

    pub fn use_sasl_plain(self, user: &str, pass: &str) -> Self {
        match self {
            #[cfg(feature = "kafka-cpp-driver-tests")]
            Self::Cpp(cpp) => Self::Cpp(cpp.use_sasl_plain(user, pass)),
            Self::Java(java) => Self::Java(java.use_sasl_plain(user, pass)),
        }
    }

    pub async fn connect_producer(&self, acks: i32) -> KafkaProducer {
        match self {
            #[cfg(feature = "kafka-cpp-driver-tests")]
            Self::Cpp(cpp) => KafkaProducer::Cpp(cpp.connect_producer(acks).await),
            Self::Java(java) => KafkaProducer::Java(java.connect_producer(acks).await),
        }
    }

    pub async fn connect_consumer(&self, topic_name: &str) -> KafkaConsumer {
        match self {
            #[cfg(feature = "kafka-cpp-driver-tests")]
            Self::Cpp(cpp) => KafkaConsumer::Cpp(cpp.connect_consumer(topic_name).await),
            Self::Java(java) => KafkaConsumer::Java(java.connect_consumer(topic_name).await),
        }
    }

    pub async fn connect_admin(&self) -> KafkaAdmin {
        match self {
            #[cfg(feature = "kafka-cpp-driver-tests")]
            Self::Cpp(cpp) => KafkaAdmin::Cpp(cpp.connect_admin().await),
            Self::Java(java) => KafkaAdmin::Java(java.connect_admin().await),
        }
    }

    pub async fn assert_admin_error(&self) -> anyhow::Error {
        let admin = self.connect_admin().await;

        admin
            .create_topics_fallible(&[NewTopic {
                name: "partitions1",
                num_partitions: 1,
                replication_factor: 1,
            }])
            .await
            .unwrap_err()
    }

    pub async fn admin_cleanup(&self) {
        match self {
            #[cfg(feature = "kafka-cpp-driver-tests")]
            Self::Cpp(cpp) => cpp.admin_cleanup().await,
            Self::Java(_) => {}
        }
    }
}

pub enum KafkaProducer {
    #[cfg(feature = "kafka-cpp-driver-tests")]
    Cpp(KafkaProducerCpp),
    Java(KafkaProducerJava),
}

impl KafkaProducer {
    pub async fn assert_produce(&self, record: Record<'_>, expected_offset: Option<i64>) {
        match self {
            #[cfg(feature = "kafka-cpp-driver-tests")]
            Self::Cpp(cpp) => cpp.assert_produce(record, expected_offset).await,
            Self::Java(java) => java.assert_produce(record, expected_offset).await,
        }
    }
}

pub struct Record<'a> {
    pub payload: &'a str,
    pub topic_name: &'a str,
    pub key: Option<&'a str>,
}

pub enum KafkaConsumer {
    #[cfg(feature = "kafka-cpp-driver-tests")]
    Cpp(KafkaConsumerCpp),
    Java(KafkaConsumerJava),
}

impl KafkaConsumer {
    pub async fn assert_consume(&mut self, expected_response: ExpectedResponse) {
        let response = match self {
            #[cfg(feature = "kafka-cpp-driver-tests")]
            Self::Cpp(cpp) => cpp.consume().await,
            Self::Java(java) => java.consume().await,
        };
        assert_eq!(expected_response.message, response.message);
        assert_eq!(expected_response.key, response.key);
        assert_eq!(expected_response.topic_name, response.topic_name);
        assert_eq!(expected_response.offset, response.offset);
    }

    pub async fn assert_consume_in_any_order(&mut self, expected_responses: Vec<ExpectedResponse>) {
        let mut responses = vec![];
        while responses.len() < expected_responses.len() {
            match self {
                #[cfg(feature = "kafka-cpp-driver-tests")]
                Self::Cpp(cpp) => responses.push(cpp.consume().await),
                Self::Java(java) => responses.push(java.consume().await),
            }
        }
        let full_responses = responses.clone();
        let full_expected_responses = expected_responses.clone();

        for expected_response in expected_responses {
            match responses.iter().enumerate().find(|(_, x)| **x == expected_response) {
                Some((i, _)) => {
                    responses.remove(i);
                }
                None => panic!("An expected response was not found in the actual responses\nExpected responses:{full_expected_responses:#?}\nActual responses:{full_responses:#?}"),
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct ExpectedResponse {
    pub message: String,
    pub key: Option<String>,
    pub topic_name: String,
    /// Responses will always have this set to Some.
    /// The test case can set this to None to ignore the value of the actual response.
    pub offset: Option<i64>,
}

impl PartialEq for ExpectedResponse {
    fn eq(&self, other: &Self) -> bool {
        self.message == other.message
            && self.key == other.key
            && self.topic_name == other.topic_name
            && match (self.offset, other.offset) {
                (None, None) => true,
                (None, Some(_)) => true,
                (Some(_), None) => true,
                (Some(a), Some(b)) => a == b,
            }
    }
}

pub enum KafkaAdmin {
    #[cfg(feature = "kafka-cpp-driver-tests")]
    Cpp(KafkaAdminCpp),
    Java(KafkaAdminJava),
}

impl KafkaAdmin {
    pub async fn create_topics_fallible(&self, topics: &[NewTopic<'_>]) -> Result<()> {
        match self {
            #[cfg(feature = "kafka-cpp-driver-tests")]
            KafkaAdmin::Cpp(cpp) => cpp.create_topics_fallible(topics).await,
            KafkaAdmin::Java(java) => java.create_topics_fallible(topics).await,
        }
    }

    pub async fn create_topics(&self, topics: &[NewTopic<'_>]) {
        match self {
            #[cfg(feature = "kafka-cpp-driver-tests")]
            KafkaAdmin::Cpp(cpp) => cpp.create_topics(topics).await,
            KafkaAdmin::Java(java) => java.create_topics(topics).await,
        }
    }

    pub async fn describe_topic(&self, topic_name: &str) -> Result<TopicDescription> {
        match self {
            #[cfg(feature = "kafka-cpp-driver-tests")]
            KafkaAdmin::Cpp(_) => unimplemented!(),
            KafkaAdmin::Java(java) => java.describe_topic(topic_name).await,
        }
    }

    pub async fn delete_topics(&self, to_delete: &[&str]) {
        match self {
            #[cfg(feature = "kafka-cpp-driver-tests")]
            Self::Cpp(cpp) => cpp.delete_topics(to_delete).await,
            Self::Java(java) => java.delete_topics(to_delete).await,
        }
    }

    pub async fn create_partitions(&self, partitions: &[NewPartition<'_>]) {
        match self {
            #[cfg(feature = "kafka-cpp-driver-tests")]
            KafkaAdmin::Cpp(cpp) => cpp.create_partitions(partitions).await,
            KafkaAdmin::Java(java) => java.create_partitions(partitions).await,
        }
    }

    pub async fn describe_configs(&self, resources: &[ResourceSpecifier<'_>]) {
        match self {
            #[cfg(feature = "kafka-cpp-driver-tests")]
            KafkaAdmin::Cpp(cpp) => cpp.describe_configs(resources).await,
            KafkaAdmin::Java(java) => java.describe_configs(resources).await,
        }
    }

    pub async fn alter_configs(&self, alter_configs: &[AlterConfig<'_>]) {
        match self {
            #[cfg(feature = "kafka-cpp-driver-tests")]
            KafkaAdmin::Cpp(cpp) => cpp.alter_configs(alter_configs).await,
            KafkaAdmin::Java(java) => java.alter_configs(alter_configs).await,
        }
    }

    pub async fn create_acls(&self, acls: Vec<Acl>) {
        match self {
            #[cfg(feature = "kafka-cpp-driver-tests")]
            KafkaAdmin::Cpp(_) => unimplemented!("CPP driver does not support creating ACL's"),
            KafkaAdmin::Java(java) => java.create_acls(acls).await,
        }
    }
}

pub struct NewTopic<'a> {
    pub name: &'a str,
    pub num_partitions: i32,
    pub replication_factor: i16,
}

pub struct NewPartition<'a> {
    pub topic_name: &'a str,
    pub new_partition_count: i32,
}

pub enum ResourceSpecifier<'a> {
    Topic(&'a str),
}

pub struct AlterConfig<'a> {
    pub specifier: ResourceSpecifier<'a>,
    pub entries: &'a [ConfigEntry],
}

pub struct ConfigEntry {
    pub key: String,
    pub value: String,
}

pub struct Acl {
    pub resource_type: ResourceType,
    pub resource_name: String,
    pub resource_pattern_type: ResourcePatternType,
    pub principal: String,
    pub host: String,
    pub operation: AclOperation,
    pub permission_type: AclPermissionType,
}

/// https://docs.confluent.io/platform/current/clients/javadocs/javadoc/org/apache/kafka/common/resource/ResourceType.html
pub enum ResourceType {
    Cluster,
    DelegationToken,
    Group,
    Topic,
    TransactionalId,
    User,
}

/// https://docs.confluent.io/platform/current/clients/javadocs/javadoc/org/apache/kafka/common/resource/PatternType.html
pub enum ResourcePatternType {
    Literal,
    Prefixed,
}

/// https://docs.confluent.io/platform/current/clients/javadocs/javadoc/org/apache/kafka/common/acl/AclOperation.html
pub enum AclOperation {
    All,
    Alter,
    AlterConfigs,
    ClusterAction,
    Create,
    CreateTokens,
    Delete,
    Describe,
    DescribeConfigs,
    DescribeTokens,
    Read,
    Write,
}

/// https://docs.confluent.io/platform/current/clients/javadocs/javadoc/org/apache/kafka/common/acl/AclPermissionType.html
pub enum AclPermissionType {
    Allow,
    Deny,
}

#[derive(Debug)]
pub struct TopicDescription {
    // None of our tests actually make use of the contents of TopicDescription,
    // instead they just check if the describe succeeded or failed,
    // so this is intentionally left empty for now
}
