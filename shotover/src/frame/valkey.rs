use crate::frame::ValkeyFrame;
use crate::message::QueryType;

#[inline]
pub fn valkey_query_type(frame: &ValkeyFrame) -> QueryType {
    if let ValkeyFrame::Array(frames) = frame {
        if let Some(ValkeyFrame::BulkString(bytes)) = frames.first() {
            return match bytes.to_ascii_uppercase().as_slice() {
                b"APPEND" | b"BITCOUNT" | b"STRLEN" | b"GET" | b"GETRANGE" | b"MGET"
                | b"LRANGE" | b"LINDEX" | b"LLEN" | b"SCARD" | b"SISMEMBER" | b"SMEMBERS"
                | b"SUNION" | b"SINTER" | b"ZCARD" | b"ZCOUNT" | b"ZRANGE" | b"ZRANK"
                | b"ZSCORE" | b"ZRANGEBYSCORE" | b"HGET" | b"HGETALL" | b"HEXISTS" | b"HKEYS"
                | b"HLEN" | b"HSTRLEN" | b"HVALS" | b"PFCOUNT" => QueryType::Read,
                _ => QueryType::Write,
            };
        }
    }
    QueryType::Write
}

pub fn valkey_query_name(frame: &ValkeyFrame) -> Option<String> {
    if let ValkeyFrame::Array(array) = frame {
        if let Some(ValkeyFrame::BulkString(v)) = array.first() {
            let upper_bytes = v.to_ascii_uppercase();
            match String::from_utf8(upper_bytes) {
                Ok(query_type) => {
                    return Some(query_type);
                }
                Err(err) => {
                    tracing::error!("Failed to convert valkey bulkstring to string, err: {err:?}")
                }
            }
        }
    }
    None
}
