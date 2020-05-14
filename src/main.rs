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

use std::collections::HashMap;
use rust_practice::transforms::codec_destination::{CodecDestination};
use rust_practice::transforms::Transforms;
use rust_practice::transforms::mpsc::{AsyncMpsc};
use rust_practice::transforms::cassandra_source::CassandraSource;
use rust_practice::transforms::kafka_destination::KafkaDestination;


#[tokio::main(core_threads = 4)]
async fn main() -> Result<(), Box<dyn Error>> {

    //TODO: turn all this into a config object or something
    //Build config for Topic Source and chain
    let kafka_config: HashMap<String, String> =
        [("bootstrap.servers", "127.0.0.1:9092"),
        ("message.timeout.ms", "5000")].iter()
            .map(|(x,y)| (String::from(*x), String::from(*y)))
            .collect();

    let t = Transforms::KafkaDestination(KafkaDestination::new_from_config(&kafka_config));
    let mpsc_chain = TransformChain::new(vec![t], "kafka_log");
    let mpsc = AsyncMpsc::new(mpsc_chain);

    // Build config for C* Source and chain
    let listen_addr = env::args()
    .nth(1)
    .unwrap_or_else(|| "127.0.0.1:9043".to_string());

    let server_addr = env::args()
    .nth(2)
    .unwrap_or_else(|| "127.0.0.1:9042".to_string());


    // Build the C* chain, include the Tee to the other chain... this is the main way in which we
    // will build async topologies

    let cstar_dest = CodecDestination::get_transform_enum_from_config(server_addr).await;
    let mpsc_tee = mpsc.get_async_mpsc_tee_enum();
    let chain = TransformChain::new(vec![mpsc_tee, cstar_dest], "test");

    let mut cassandra_ks: HashMap<String, Vec<String>> = HashMap::new();
    cassandra_ks.insert("system.local".to_string(), vec!["key".to_string()]);
    cassandra_ks.insert("test.simple".to_string(), vec!["pk".to_string()]);
    cassandra_ks.insert("test.clustering".to_string(), vec!["pk".to_string(), "clustering".to_string()]);

    let source = CassandraSource::new(chain, listen_addr, cassandra_ks);

    //TODO: probably a better way to handle various join handles / threads
    let _ = tokio::join!(mpsc.rx_handle, source.join_handle);
    Ok(())
}