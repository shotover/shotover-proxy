use crate::sources::cassandra::CassandraSource;
use crate::sources::kafka::KafkaSource;
use crate::sources::opensearch::OpenSearchSource;
use crate::sources::redis::RedisSource;
use serde::Deserialize;
use tokio::task::JoinHandle;

pub mod cassandra;
pub mod kafka;
pub mod opensearch;
pub mod redis;

#[derive(Deserialize, Debug, Clone, Copy)]
#[serde(deny_unknown_fields)]
pub enum Transport {
    Tcp,
    WebSocket,
}

#[derive(Debug)]
pub enum Source {
    Cassandra(CassandraSource),
    Redis(RedisSource),
    Kafka(KafkaSource),
    OpenSearch(OpenSearchSource),
}

impl Source {
    pub fn into_join_handle(self) -> JoinHandle<()> {
        match self {
            Source::Cassandra(c) => c.join_handle,
            Source::Redis(r) => r.join_handle,
            Source::Kafka(r) => r.join_handle,
            Source::OpenSearch(o) => o.join_handle,
        }
    }
}
