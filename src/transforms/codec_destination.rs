use async_trait::async_trait;
use crate::transforms::chain::{Transform, ChainResponse, Wrapper, TransformChain};
use tokio_util::codec::Framed;
use tokio::net::TcpStream;
use tokio::task::spawn_blocking;
use serde::Deserialize;

use crate::protocols::cassandra_protocol2::CassandraCodec2;
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
use tokio::runtime::Handle;
use tokio::task;


#[derive(Debug, Deserialize)]
#[serde(from = "CodecConfiguration")]
pub struct CodecDestination {
    name: &'static str,
    address: String,
    outbound: Arc<Mutex<Framed<TcpStream, CassandraCodec2>>>
}

#[derive(Deserialize)]
pub struct CodecConfiguration {
    #[serde(rename = "remote_address")]
    pub address: String
}

impl Clone for CodecDestination {
    fn clone(&self) -> Self {
        let f = self.address.clone();
        CodecDestination::new_from_config_sync(f)
    }
}

impl From<CodecConfiguration> for CodecDestination {
    fn from(c: CodecConfiguration) -> Self {
        Handle::current().block_on(CodecDestination::new_from_config(c.address))
    }
}


impl CodecDestination {
    pub async fn new_from_config(address: String) -> CodecDestination {
        let outbound_stream = TcpStream::connect(address.clone()).await.unwrap();
        let outbound_framed_codec = Framed::new(outbound_stream, CassandraCodec2::new());
        let protected_outbound =  Arc::new(Mutex::new(outbound_framed_codec));
        CodecDestination::new(protected_outbound, address.clone())
    }

    pub fn new_from_config_sync(address: String) -> CodecDestination {
        task::block_in_place(|| {
            Handle::current().block_on(tokio::spawn(async {
                CodecDestination::new_from_config(address).await
            }))
        }).unwrap()
    }


    pub fn new(outbound: Arc<Mutex<Framed<TcpStream, CassandraCodec2>>>, address: String) -> CodecDestination {
        CodecDestination {
            address,
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

impl CodecDestination {
    async fn send_frame(&self, frame: Frame, matching_query: Option<QueryMessage>) -> ChainResponse {
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
impl Transform for CodecDestination {
    async fn transform(&self, mut qd: Wrapper, t: & TransformChain) -> ChainResponse {
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