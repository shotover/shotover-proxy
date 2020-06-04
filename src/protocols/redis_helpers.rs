use redis_protocol::prelude::Frame;
use crate::message::{Value, QueryType, RawMessage};
use crate::message::{Message, QueryMessage};
use crate::protocols::cassandra_protocol2::RawFrame;
use std::collections::HashMap;

fn handle_redis_string(string: String, _frame: Frame) -> Message {
    return Message::Query(QueryMessage {
        original: RawFrame::NONE,
        query_string: string,
        namespace: vec![],
        primary_key: Default::default(),
        query_values: None,
        projection: None,
        query_type: QueryType::Read,
        ast: None,
    });
}

pub fn process_redis_frame(frame: Frame) -> Message {
    return match &frame {
        Frame::SimpleString(s) => { handle_redis_string(s.clone(), frame) }
        Frame::BulkString(bs) => { handle_redis_string(String::from_utf8(bs.clone()).unwrap_or("invalid utf-8".to_string()), frame) }
        Frame::Array(_) => {
            match frame.parse_as_pubsub(){
                Ok((channel, message, kind)) => {
                    let mut map: HashMap<String, Value> = HashMap::new();
                    map.insert(channel.clone(), Value::Strings(message.clone()));
                    Message::Query(QueryMessage {
                        original: RawFrame::Redis(Frame::Array(vec![Frame::SimpleString(channel.clone()), Frame::SimpleString(message.clone()), Frame::SimpleString(kind.clone())])),
                        query_string: "".to_string(),
                        namespace: vec![channel],
                        primary_key: Default::default(),
                        query_values: Some(map),
                        projection: None,
                        query_type: QueryType::PubSubMessage,
                        ast: None,
                    })
                },
                Err(frame) => {Message::Bypass(RawMessage { original: RawFrame::Redis(frame)})},
            }
        }
        Frame::Moved(m) => { handle_redis_string(m.clone(), frame) }
        Frame::Ask(a) => { handle_redis_string(a.clone(), frame) }
        _ => {
            Message::Bypass(RawMessage {
                original: RawFrame::Redis(frame)
            })
        }
    };
}