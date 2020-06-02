use crate::transforms::chain::{ChainResponse, TransformChain, Wrapper};
use tokio::stream::StreamExt;

use tokio_util::codec::Framed;

use futures::FutureExt;

use crate::config::topology::TopicHolder;
use crate::config::ConfigError;
use crate::message::Message;
use crate::protocols::cassandra_helper::process_cassandra_frame;
use crate::protocols::cassandra_protocol2::CassandraCodec2;
use crate::protocols::cassandra_protocol2::RawFrame::CASSANDRA;
use crate::sources::{Sources, SourcesFromConfig};
use async_trait::async_trait;
use cassandra_proto::frame::Frame;
use futures::SinkExt;
use serde::{Deserialize, Serialize};
use slog::error;
use slog::info;
use slog::warn;
use slog::Logger;
use std::collections::HashMap;
use std::error::Error;
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::Handle;
use tokio::task::JoinHandle;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct CassandraConfig {
    pub listen_addr: String,
    pub cassandra_ks: HashMap<String, Vec<String>>,
}

#[async_trait]
impl SourcesFromConfig for CassandraConfig {
    async fn get_source(
        &self,
        chain: &TransformChain,
        topics: &mut TopicHolder,
        logger: &Logger,
    ) -> Result<Sources, ConfigError> {
        Ok(Sources::Cassandra(CassandraSource::new(
            chain,
            self.listen_addr.clone(),
            self.cassandra_ks.clone(),
            logger,
        )))
    }
}

pub struct CassandraSource {
    pub name: &'static str,
    pub join_handle: JoinHandle<Result<(), Box<dyn Error + Send + Sync>>>,
    pub listen_addr: String,
    logger: Logger,
    // pub cassandra_ks: HashMap<String, Vec<String>>,
}

impl CassandraSource {
    fn listen_loop(
        chain: TransformChain,
        listen_addr: String,
        cassandra_ks: HashMap<String, Vec<String>>,
        logger_p: &Logger,
    ) -> JoinHandle<Result<(), Box<dyn Error + Send + Sync>>> {
        let logger = logger_p.clone();
        Handle::current().spawn(async move {
            let mut listener = TcpListener::bind(listen_addr.clone()).await.unwrap();
            info!(logger, "Starting Cassandra source on [{}]", listen_addr);
            loop {
                while let Ok((inbound, _)) = listener.accept().await {
                    info!(logger, "Connection received from {:?}", inbound.peer_addr());

                    let messages = Framed::new(inbound, CassandraCodec2::new());
                    let e_logger = logger.clone();

                    let transfer = CassandraSource::transfer(
                        messages,
                        chain.clone(),
                        cassandra_ks.clone(),
                        logger.clone(),
                    )
                    .map( move |r| {
                        if let Err(e) = r {
                            warn!(e_logger, "Oh oh {}", e);
                            //TODO I don't actually think we really get an error back
                        }
                    });

                    tokio::spawn(transfer);
                }
            }
        })
    }

    //"127.0.0.1:9043
    pub fn new(
        chain: &TransformChain,
        listen_addr: String,
        cassandra_ks: HashMap<String, Vec<String>>,
        logger: &Logger,
    ) -> CassandraSource {
        CassandraSource {
            name: "Cassandra",
            join_handle: CassandraSource::listen_loop(
                chain.clone(),
                listen_addr.clone(),
                cassandra_ks,
                logger,
            ),
            listen_addr: listen_addr.clone(),
            logger: logger.clone(),
        }
    }

    async fn process_message(frame: Wrapper, transforms: &TransformChain) -> ChainResponse {
        return transforms.process_request(frame).await;
    }

    async fn transfer(
        mut inbound: Framed<TcpStream, CassandraCodec2>,
        chain: TransformChain,
        cassandra_ks: HashMap<String, Vec<String>>,
        logger: Logger,
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
                        let frame = Wrapper::new(process_cassandra_frame(message, &cassandra_ks));
                        let pm = CassandraSource::process_message(frame, &chain).await;
                        if let Ok(modified_message) = pm {
                            match modified_message {
                                Message::Query(query) => {
                                    // We now forward queries to the server via the chain
                                }
                                Message::Response(resp) => {
                                    if let CASSANDRA(f) = resp.original {
                                        inbound.send(f).await?
                                    } else {
                                        let c_frame: Frame =
                                            CassandraCodec2::build_cassandra_response_frame(resp);
                                        inbound.send(c_frame).await?
                                    }
                                }
                                Message::Bypass(resp) => {
                                    if let CASSANDRA(f) = resp.original {
                                        inbound.send(f).await?
                                    }
                                }
                            }
                        }
                        // Process message decided to drop the message so do nothing (warning this may cause client timeouts)
                    }
                    Err(e) => {
                        error!(logger, "Error handling message in Cassandra source: {}", e);
                    }
                }
            }
        }
    }
}
