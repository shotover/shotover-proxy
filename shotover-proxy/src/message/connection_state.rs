#[derive(Default, PartialEq, Clone, Debug)]
pub struct CassandraConnectionState {
    pub used_keyspace: Option<String>,
}

#[derive(PartialEq, Clone, Debug)]
pub enum ConnectionState {
    Cassandra(CassandraConnectionState),
    Redis,
    Unknown,
}
