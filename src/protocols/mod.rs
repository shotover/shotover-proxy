use cassandra_proto::frame::Frame;

pub mod cassandra_protocol2;
pub mod redis_codec;
use redis_protocol::prelude::Frame as Rframe;
use sqlparser::ast::DateTimeField;
use sqlparser::ast::Value;

use serde::{Deserialize, Serialize};

#[derive(Eq, PartialEq, Debug, Clone, Hash, Serialize, Deserialize)]
pub enum RawFrame {
    CASSANDRA(Frame),
    Redis(Rframe),
    NONE,
}
