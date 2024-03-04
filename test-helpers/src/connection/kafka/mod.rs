#[cfg(feature = "rdkafka-driver-tests")]
pub mod cpp;
pub mod java;

#[cfg(feature = "rdkafka-driver-tests")]
use cpp::*;
use java::*;

pub enum KafkaDriver {
    #[cfg(feature = "rdkafka-driver-tests")]
    Cpp,
    Java,
}

pub enum KafkaConnectionBuilder {
    #[cfg(feature = "rdkafka-driver-tests")]
    Cpp(KafkaConnectionBuilderCpp),
    Java(KafkaConnectionBuilderJava),
}

impl KafkaConnectionBuilder {
    pub fn new(driver: KafkaDriver, address: &str) -> Self {
        match driver {
            #[cfg(feature = "rdkafka-driver-tests")]
            KafkaDriver::Cpp => Self::Cpp(KafkaConnectionBuilderCpp::new(address)),
            KafkaDriver::Java => Self::Java(KafkaConnectionBuilderJava::new(address)),
        }
    }

    pub fn use_sasl(self, user: &str, pass: &str) -> Self {
        match self {
            #[cfg(feature = "rdkafka-driver-tests")]
            Self::Cpp(cpp) => Self::Cpp(cpp.use_sasl(user, pass)),
            Self::Java(java) => Self::Java(java.use_sasl(user, pass)),
        }
    }

    pub async fn connect_producer(&self, acks: i32) -> KafkaProducer {
        match self {
            #[cfg(feature = "rdkafka-driver-tests")]
            Self::Cpp(cpp) => KafkaProducer::Cpp(cpp.connect_producer(acks).await),
            Self::Java(java) => KafkaProducer::Java(java.connect_producer(acks).await),
        }
    }

    pub async fn connect_consumer(&self, topic_name: &str) -> KafkaConsumer {
        match self {
            #[cfg(feature = "rdkafka-driver-tests")]
            Self::Cpp(cpp) => KafkaConsumer::Cpp(cpp.connect_consumer(topic_name).await),
            Self::Java(java) => KafkaConsumer::Java(java.connect_consumer(topic_name).await),
        }
    }

    pub async fn admin_setup(&self) {
        match self {
            #[cfg(feature = "rdkafka-driver-tests")]
            Self::Cpp(cpp) => cpp.admin_setup().await,
            Self::Java(java) => java.admin_setup().await,
        }
    }

    pub async fn admin_cleanup(&self) {
        match self {
            #[cfg(feature = "rdkafka-driver-tests")]
            Self::Cpp(cpp) => cpp.admin_cleanup().await,
            Self::Java(java) => java.admin_cleanup().await,
        }
    }
}

pub enum KafkaProducer {
    #[cfg(feature = "rdkafka-driver-tests")]
    Cpp(KafkaProducerCpp),
    Java(KafkaProducerJava),
}

impl KafkaProducer {
    pub async fn assert_produce(&self, record: Record<'_>, expected_offset: Option<i64>) {
        match self {
            #[cfg(feature = "rdkafka-driver-tests")]
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
    #[cfg(feature = "rdkafka-driver-tests")]
    Cpp(KafkaConsumerCpp),
    Java(KafkaConsumerJava),
}

impl KafkaConsumer {
    pub async fn assert_consume(&self, response: ExpectedResponse<'_>) {
        match self {
            #[cfg(feature = "rdkafka-driver-tests")]
            Self::Cpp(cpp) => cpp.assert_consume(response).await,
            Self::Java(java) => java.assert_consume(response).await,
        }
    }
}

pub struct ExpectedResponse<'a> {
    pub message: &'a str,
    pub key: Option<&'a str>,
    pub topic_name: &'a str,
    pub offset: i64,
}
