use async_trait::async_trait;
use crate::transforms::chain::{Transform, ChainResponse, Wrapper, TransformChain};
use tokio_util::codec::Framed;
use tokio::net::TcpStream;
use crate::cassandra_protocol2::CassandraCodec2;
use futures::{SinkExt, FutureExt};
use crate::message::Message::Query;
use crate::cassandra_protocol::RawFrame::CASSANDRA;
use tokio::stream::StreamExt;
use crate::message::{Message, QueryResponse, QueryMessage, RawMessage};
use std::cell::RefCell;
use std::borrow::BorrowMut;
use std::sync::Arc;
use tokio::sync::Mutex;
use crate::transforms::chain::RequestError;
use crate::cassandra_protocol::RawFrame;
use cassandra_proto::frame::Frame;


#[derive(Debug)]
pub struct CodecDestination {
    name: &'static str,
    outbound: Arc<Mutex<Framed<TcpStream, CassandraCodec2>>>
}

impl CodecDestination {
    pub fn new(outbound: Arc<Mutex<Framed<TcpStream, CassandraCodec2>>>) -> CodecDestination {
        CodecDestination{
            outbound,
            name: "CodecDestination",
        }
    }
}

/*
TODO:
it may be worthwhile putting the inbound and outbound tcp streams behind a
multi-consumer, single producer threadsafe queue
*/

impl<'a, 'c> CodecDestination {
    async fn send_frame(&self, frame: Frame, matching_query: Option<QueryMessage>) -> ChainResponse<'c> {
        println!("      C -> S {:?}", frame.opcode);
        if let Ok(mut outbound) = self.outbound.try_lock() {
            outbound.send(frame).await;
            if let Some(o) = outbound.next().fuse().await {
                if let Ok(resp) = o {
                    println!("      S -> C {:?}", resp.opcode);
                    return ChainResponse::Ok(Message::Bypass(RawMessage{
                        original: RawFrame::CASSANDRA(resp),
                    }));
                }
            }
        }
        return ChainResponse::Err(RequestError{});
    }
}


#[async_trait]
impl<'a, 'c> Transform<'a, 'c> for CodecDestination {
    async fn transform(&self, mut qd: Wrapper, t: & TransformChain<'a,'c>) -> ChainResponse<'c> {
        let return_query = qd.message.clone();
        match qd.message {
            Message::Bypass(rm) => {
                if let CASSANDRA(frame) = rm.original {
                    return self.send_frame(frame, None).await;
                }
            },
            Message::Query(qm) => {
                let return_query = qm.clone();
                if let CASSANDRA(frame) = qm.original {
                    return self.send_frame(frame, Some(return_query)).await;
                }
            },
            Message::Response(_) => {},
        }
        return ChainResponse::Err(RequestError{});
    }

    fn get_name(&self) -> &'static str {
        self.name
    }
}