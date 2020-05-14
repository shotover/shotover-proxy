use crate::transforms::chain::{Transform, ChainResponse, Wrapper, TransformChain};
use tokio::sync::mpsc::{Sender, Receiver, channel};
use tokio::stream::{ StreamExt};

use tokio_util::codec::{Framed};

use futures::FutureExt;

use async_trait::async_trait;
use crate::message::{Message, QueryResponse};
use tokio::task::JoinHandle;
use tokio::runtime::Handle;
use tokio::net::{TcpListener, TcpStream};
use crate::protocols::cassandra_protocol2::CassandraCodec2;
use std::error::Error;
use crate::cassandra_protocol::RawFrame::CASSANDRA;
use cassandra_proto::frame::Frame;
use futures::SinkExt;
use crate::protocols::cassandra_helper::process_cassandra_frame;
use std::collections::HashMap;

pub struct CassandraSource {
    pub name: &'static str,
    pub join_handle: JoinHandle<Result<(), Box<dyn Error + Send + Sync>>>,
    pub listen_addr: String,
    // pub cassandra_ks: HashMap<String, Vec<String>>,
}


impl CassandraSource {
    fn listen_loop(chain: TransformChain, listen_addr: String, cassandra_ks: HashMap<String, Vec<String>>) -> JoinHandle<Result<(), Box<dyn Error + Send + Sync>>> {
        Handle::current().spawn(async move {
            let mut listener = TcpListener::bind(listen_addr).await.unwrap();
            loop {
                while let Ok((inbound, _)) = listener.accept().await {
                    println!("Connection received from {:?}", inbound.peer_addr());

                    let messages = Framed::new(inbound, CassandraCodec2::new());

                    let transfer = CassandraSource::transfer(messages, chain.clone(), cassandra_ks.clone()).map(|r| {
                        if let Err(e) = r {
                            println!("Failed to transfer; error={}", e);
                        }
                    });

                    tokio::spawn(transfer);
                }
            }
        })
    }


    //"127.0.0.1:9043
    pub fn new(chain: TransformChain, listen_addr: String, cassandra_ks: HashMap<String, Vec<String>>) -> CassandraSource {
        CassandraSource {
            name: "Cassandra",
            join_handle: CassandraSource::listen_loop(chain, listen_addr, cassandra_ks),
            listen_addr: "".to_string()
        }
    }

    async fn process_message(mut frame: Wrapper, transforms: &TransformChain) -> ChainResponse {
        return transforms.process_request(frame).await
    }

    async fn transfer(
        mut inbound: Framed<TcpStream, CassandraCodec2>,
        chain: TransformChain,
        mut cassandra_ks: HashMap<String, Vec<String>>,
    ) -> Result<(), Box<dyn Error>> {
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
                        let pm = CassandraSource::process_message(frame, &chain).await;
                        if let Ok(modified_message) = pm {
                            match modified_message {
                                Message::Query(query)    =>  {
                                    // We now forward queries to the server via the chain
                                },
                                Message::Response(resp)  =>  {
                                    if let CASSANDRA(f) = resp.original {
                                        inbound.send(f).await?
                                    } else {
                                        let c_frame: Frame = CassandraCodec2::build_cassandra_response_frame(resp);
                                        inbound.send(c_frame).await?
                                    }
                                },
                                Message::Bypass(resp)  =>  {
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
}