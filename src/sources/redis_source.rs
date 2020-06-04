use crate::transforms::chain::{ChainResponse, TransformChain, Wrapper};
use tokio::stream::StreamExt;

use tokio_util::codec::Framed;

use futures::FutureExt;

use crate::message::Message;
use crate::protocols::cassandra_protocol2::RawFrame::{Redis};
use futures::SinkExt;
use slog::error;
use slog::info;
use slog::warn;
use slog::Logger;
use std::error::Error;
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::Handle;
use tokio::task::JoinHandle;
use crate::protocols::redis_codec::RedisCodec;
use crate::protocols::redis_helpers::process_redis_frame;
use redis_protocol::prelude::Frame;


pub struct RedisSource {
    pub name: &'static str,
    pub join_handle: JoinHandle<Result<(), Box<dyn Error + Send + Sync>>>,
    pub listen_addr: String,
}

impl RedisSource {
    fn listen_loop(
        chain: TransformChain,
        listen_addr: String,
        logger_p: &Logger,
    ) -> JoinHandle<Result<(), Box<dyn Error + Send + Sync>>> {
        let logger = logger_p.clone();
        Handle::current().spawn(async move {
            let mut listener = TcpListener::bind(listen_addr.clone()).await.unwrap();
            info!(logger, "Starting Redis source on [{}]", listen_addr);
            loop {
                while let Ok((inbound, _)) = listener.accept().await {
                    info!(logger, "Connection received from {:?}", inbound.peer_addr());

                    let messages = Framed::new(inbound, RedisCodec::new(logger.clone()));
                    let e_logger = logger.clone();

                    let transfer = RedisSource::transfer(
                        messages,
                        chain.clone(),
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
        logger: &Logger,
    ) -> RedisSource {
        RedisSource {
            name: "Redis",
            join_handle: RedisSource::listen_loop(
                chain.clone(),
                listen_addr.clone(),
                logger,
            ),
            listen_addr: listen_addr.clone(),
        }
    }

    async fn process_message(frame: Wrapper, transforms: &TransformChain) -> ChainResponse {
        return transforms.process_request(frame).await;
    }

    async fn transfer(
        mut inbound: Framed<TcpStream, RedisCodec>,
        chain: TransformChain,
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
                        let frame = Wrapper::new(process_redis_frame(message));
                        let pm = RedisSource::process_message(frame, &chain).await;
                        if let Ok(modified_message) = pm {
                            match modified_message {
                                Message::Query(_) => {
                                    // We now forward queries to the server via the chain
                                }
                                Message::Response(resp) => {
                                    if let Redis(f) = resp.original {
                                        inbound.send(f).await?
                                    } else {
                                        let r_frame: Frame =
                                            RedisCodec::build_redis_response_frame(resp);
                                        inbound.send(r_frame).await?
                                    }
                                }
                                Message::Bypass(resp) => {
                                    if let Redis(f) = resp.original {
                                        inbound.send(f).await?
                                    }
                                }
                            }
                        }
                        // Process message decided to drop the message so do nothing (warning this may cause client timeouts)
                    }
                    Err(e) => {
                        error!(logger, "Error handling message in Redis source: {}", e);
                    }
                }
            }
        }
    }
}
