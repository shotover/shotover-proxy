use async_trait::async_trait;
use crate::transforms::chain::{Transform, ChainResponse, Wrapper, TransformChain};
use tokio_util::codec::Framed;
use tokio::net::TcpStream;
use crate::cassandra_protocol::{CassandraCodec, RawFrame};
use futures::{SinkExt, FutureExt};
use crate::message::Message::Query;
use crate::cassandra_protocol::RawFrame::CASSANDRA;
use tokio::stream::StreamExt;
use crate::message::{Message, QueryResponse};
use std::cell::RefCell;
use std::borrow::BorrowMut;
use std::sync::Arc;
use tokio::sync::Mutex;
use crate::transforms::chain::RequestError;


#[derive(Debug)]
pub struct CodecDestination {
    name: &'static str,
    outbound: Arc<Mutex<Framed<TcpStream, CassandraCodec>>>
}

impl CodecDestination {
    pub fn new(outbound: Arc<Mutex<Framed<TcpStream, CassandraCodec>>>) -> CodecDestination {
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


#[async_trait]
impl<'a, 'c> Transform<'a, 'c> for CodecDestination {
    async fn transform(&self, mut qd: Wrapper, t: & TransformChain<'a,'c>) -> ChainResponse<'c> {
        let return_query = qd.message.clone();
        if let Query(qm) = qd.message {
            let return_query = qm.clone();
            if let CASSANDRA(frame) = qm.original {
                if let Ok(mut outbound) = self.outbound.try_lock() {
                    outbound.send(frame).await;
                    if let Some(o) = outbound.next().fuse().await {
                        if let Ok(resp) = o {
                            return ChainResponse::Ok(Message::Response(QueryResponse{
                                matching_query: Some(return_query),
                                original: RawFrame::CASSANDRA(resp),
                                result: None,
                                error: None
                            }));
                        }
                    }
                }
            }
        }
        return ChainResponse::Err(RequestError{});
    }

    fn get_name(&self) -> &'static str {
        self.name
    }
}