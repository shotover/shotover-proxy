#![warn(rust_2018_idioms)]
#![recursion_limit="256"]

use tokio::stream::{ StreamExt};
use tokio_util::codec::{Framed};

use futures::FutureExt;
use futures::SinkExt;

use std::env;
use std::error::Error;

use rust_practice::cassandra_protocol::{CassandraFrame, MessageType, Direction, RawFrame};
use rust_practice::transforms::chain::{Transform, TransformChain, Wrapper, ChainResponse};
use rust_practice::message::{QueryType, Message, QueryMessage, QueryResponse, Value, RawMessage};
use rust_practice::message::Message::{Query, Response, Bypass};
use rust_practice::cassandra_protocol::RawFrame::CASSANDRA;

use tokio::net::{TcpListener, TcpStream};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::collections::HashMap;
use sqlparser::ast::{SetExpr, TableFactor, Value as SQLValue, Expr, Statement, BinaryOperator};
use sqlparser::ast::Statement::{Insert, Update, Delete};
use sqlparser::ast::Expr::{Identifier, BinaryOp};
use std::borrow::{Borrow, BorrowMut};
use chrono::DateTime;
use std::str::FromStr;
use futures::executor::block_on;
use rust_practice::transforms::codec_destination::CodecDestination;
use tokio::sync::Mutex;
use std::sync::Arc;
use cassandra_proto::frame::{Frame, Opcode};
use cassandra_proto::frame::frame_response::ResponseBody;
use rust_practice::protocols::cassandra_protocol2::CassandraCodec2;
use rust_practice::transforms::noop::NoOp;
use rust_practice::transforms::printer::Printer;
use rust_practice::transforms::query::QueryTypeFilter;
use rust_practice::transforms::forward::Forward;
use rust_practice::transforms::redis_cache::SimpleRedisCache;
use rust_practice::protocols::cassandra_helper::process_cassandra_frame;
use rust_practice::transforms::mpsc::{AsyncMpsc, AsyncMpscTee};
use std::sync::mpsc::Receiver;

struct Config {

}

#[tokio::main(core_threads = 4)]
async fn main() -> Result<(), Box<dyn Error>> {
    let listen_addr = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:9043".to_string());
    let server_addr = env::args()
        .nth(2)
        .unwrap_or_else(|| "127.0.0.1:9042".to_string());

    println!("Listening on: {}", listen_addr);
    println!("Proxying to: {}", server_addr);

    //TODO: Setup MPSC receiver threads here.
    //TODO: move all transform setups to outside of the tokio loop
    // unimplemented!();

    let mut listener = TcpListener::bind(listen_addr).await?;
    // let t_list:Vec<&'static dyn Transform> = ;
    // TransformChain::new(t_list, "test")
    let mut topic = AsyncMpsc::new();

    while let Ok((inbound, _)) = listener.accept().await {
        println!("Connection received from {:?}", inbound.peer_addr());

        let messages = Framed::new(inbound, CassandraCodec2::new());
        let outbound_stream = TcpStream::connect(server_addr.clone()).await?;
        let outbound_framed_codec = Framed::new(outbound_stream, CassandraCodec2::new());

        let transfer = transfer(messages, outbound_framed_codec, topic.get_async_mpsc_tee()).map(|r| {
            if let Err(e) = r {
                println!("Failed to transfer; error={}", e);
            }
        });

        tokio::spawn(transfer);
    }

    Ok(())
}


// TODO we should allow users to build and define their own topology of transforms/tasks to perform on a single request
// however for the poc we'll just decode the C* frame to a common format, run through a lua script then write to C* and REDIS
async fn process_message<'a, 'c>(mut frame: Wrapper, transforms: &'c TransformChain<'a, 'c>) -> ChainResponse<'c> {
    return transforms.process_request(frame).await
}

async fn transfer<'a>(
    mut inbound: Framed<TcpStream, CassandraCodec2>,
    mut outbound: Framed<TcpStream, CassandraCodec2>,
    topic_map: AsyncMpscTee
) -> Result<(), Box<dyn Error>> {
    let noop_transformer = NoOp::new();
    let printer_transform = Printer::new();
    let query_transform = QueryTypeFilter::new(vec![QueryType::Write]);
    let forward = Forward::new();
    let client = redis::Client::open("redis://127.0.0.1/")?;
    let mut con = client.get_multiplexed_tokio_connection().await?;

    let response = redis::cmd("PING").query_async(&mut con).await?;
    println!("REDIS: {:?}", response);

    let protected_outbound = Arc::new(Mutex::new(outbound));
    let redis_cache = SimpleRedisCache::new(con);
    let cassandra_dest = CodecDestination::new(protected_outbound);
    let mut cassandra_ks: HashMap<String, Vec<String>> = HashMap::new();
    // cassandra_ks.insert("system.local".to_string(), vec!["key".to_string()]);
    cassandra_ks.insert("test.simple".to_string(), vec!["pk".to_string()]);
    cassandra_ks.insert("test.clustering".to_string(), vec!["pk".to_string(), "clustering".to_string()]);


    let chain = TransformChain::new(vec![&noop_transformer, &topic_map, &redis_cache, &cassandra_dest], "test");
    // Holy snappers this is terrible - seperate out inbound and outbound loops
    // We should probably have a seperate thread for inbound and outbound, but this will probably do. Also not sure on select behavior.
    loop {
        if let Some(result) = inbound.next().fuse().await {
            match result {
                Ok(message) => {
                    // Current logic assumes that if a message is to be forward upstream, expect something
                    // If the message is going somewhere else, and the original (modified or otherwise) shouldn't be
                    // fowarded, then we should not do anything
                    // If we don't want to forward upstream, then process message should return a response and we'll just
                    // return it to the client instead of forwarding.
                    // This could be something like a spoofed success.
                    // TODO: Decouple C* frame processing / codec enabled sockets from the main tokio thread loop
                    let mut frame = Wrapper::new(process_cassandra_frame(message, &cassandra_ks));
                    let pm = process_message(frame, &chain).await;
                    if let Ok(modified_message) = pm {
                        match modified_message {
                            Query(query)    =>  {
                                // We now forward queries to the server via the chain
                            },
                            Response(resp)  =>  {
                                if let CASSANDRA(f) = resp.original {
                                    inbound.send(f).await?
                                } else {
                                    let c_frame: Frame = CassandraCodec2::build_cassandra_response_frame(resp);
                                    inbound.send(c_frame).await?
                                }
                            },
                            Bypass(resp)  =>  {
                                if let CASSANDRA(f) = resp.original {
                                    inbound.send(f).await?
                                }
                            }
                        }
                    }
                    // Process message decided to drop the message so do nothing (warning this may cause client timeouts)
                }
                Err(e) => {
                    println!("uh oh! {}", e)
                }
            }
        }
    }
}
