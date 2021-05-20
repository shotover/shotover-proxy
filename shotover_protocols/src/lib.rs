use std::collections::HashMap;

use anyhow::Result;
use bytes::Bytes;
use redis_protocol::types::Frame;

use shotover_transforms::{MessageDetails, QueryMessage, QueryResponse, QueryType, Value};

pub mod cassandra_protocol2;
pub mod redis_codec;
pub extern crate redis_protocol;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
