#![warn(rust_2018_idioms)]
#![recursion_limit="256"]

use tokio::prelude::*;
use tokio::stream::{ StreamExt};
use tokio_util::codec::{Framed};

use futures::FutureExt;
use futures::SinkExt;
use futures::{pin_mut, select};

use std::env;
use std::error::Error;

use rust_practice::cassandra_protocol::{CassandraCodec, CassandraFrame, MessageType, Direction, RawFrame};
use rust_practice::transforms::chain::{Transform, TransformChain, Wrapper, ChainResponse};
use rust_practice::transforms::{NoOp, Printer, QueryTypeFilter, Forward};
use rust_practice::message::{QueryType, Message, QueryMessage, QueryResponse};
use rust_practice::message::Message::{Query, Response};
use rust_practice::cassandra_protocol::RawFrame::CASSANDRA;

use tokio::net::{TcpListener, TcpStream};


struct Config {

}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let listen_addr = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:9043".to_string());
    let server_addr = env::args()
        .nth(2)
        .unwrap_or_else(|| "127.0.0.1:9042".to_string());

    println!("Listening on: {}", listen_addr);
    println!("Proxying to: {}", server_addr);

    let mut listener = TcpListener::bind(listen_addr).await?;

    while let Ok((inbound, _)) = listener.accept().await {
        let messages = Framed::new(inbound, CassandraCodec::new());
        let outbound_stream = TcpStream::connect(server_addr.clone()).await?;
        let outbound_framed_codec = Framed::new(outbound_stream, CassandraCodec::new());
        println!("Connection received");

        let transfer = transfer(messages, outbound_framed_codec).map(|r| {
            if let Err(e) = r {
                println!("Failed to transfer; error={}", e);
            }
        });

        tokio::spawn(transfer);
    }

    Ok(())
}

fn process_cassandra_frame(mut frame: CassandraFrame) -> Message {
    if frame.header.direction == Direction::Request {
        match frame.get_query() {
            Some(q) => {
                Message::Query(QueryMessage{
                    original: RawFrame::CASSANDRA(frame),
                    query_string: format!("{:?}", q.query_string),
                    query_type: QueryType::Read
                })
            },
            None => {
                Message::Query(QueryMessage{
                    original: RawFrame::CASSANDRA(frame),
                    query_string: "".to_string(),
                    query_type: QueryType::Read
                })
            }
        }
    } else {
        Message::Response(QueryResponse{
            original: RawFrame::CASSANDRA(frame),
            result: None,
            error: None
        })
    }
}


// TODO we should allow users to build and define their own topology of transforms/tasks to perform on a single request
// however for the poc we'll just decode the C* frame to a common format, run through a lua script then write to C* and REDIS
fn process_message(frame: CassandraFrame, transforms: & TransformChain) -> ChainResponse {
    let mut frame = Wrapper::new(process_cassandra_frame(frame));
    transforms.process_request(&mut frame)
}


//async fn build_transform_future(transform: Transform, message: Message) -> Message {
//    transform.transform(message);
//}

async fn transfer<'a>(
    mut inbound: Framed<TcpStream, CassandraCodec>,
    mut outbound: Framed<TcpStream, CassandraCodec>,
) -> Result<(), Box<dyn Error>> {
    let noop_transformer = NoOp::new();
    let printer_transform = Printer::new();
    let query_transform = QueryTypeFilter::new(vec![QueryType::Write]);
    let forward = Forward::new();

    let chain = TransformChain::new(vec![&noop_transformer, &printer_transform, &query_transform, &forward], "test");
    // Holy snappers this is terrible - seperate out inbound and outbound loops
    // We should probably have a seperate thread for inbound and outbound, but this will probably do. Also not sure on select behavior.
    loop {
        select! {
            i = inbound.next().fuse() => {
                if let Some(result) = i {
                    match result {
                        Ok(message) => {
                            // Current logic assumes that if a message is to be forward upstream, expect something
                            // If the message is going somewhere else, and the original (modified or otherwise) shouldn't be
                            // fowarded, then we should not do anything
                            // If we don't want to forward upstream, then process message should return a response and we'll just
                            // return it to the client instead of forwarding.
                            // This could be something like a spoofed success.
                            let pm = process_message(message, &chain);
                            println!("{:?}", pm);
                            if let Ok(modified_message) = pm {
                                match modified_message {
                                    Query(query)    =>  {
                                        //Unwrap c* frame
                                        if let CASSANDRA(f) = query.original {
                                            outbound.send(f).await?
                                        }
                                    },
                                    Response(resp)  =>  {
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
            },

            // TODO - allow us to filter responses from the server.
            o = outbound.next().fuse() => {
                if let Some(result) = o {
                    match result {
                        Ok(message) => {
                            // let modified_message = process_message(message, &topology);
                            inbound.send(message).await?;
                        }
                        Err(e) => {
                            println!("uh oh! {}", e)
                        }
                    }
                }
            },
        };
    }
}
