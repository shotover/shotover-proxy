#![warn(rust_2018_idioms)]
#![recursion_limit="256"]

use std::env;
use std::error::Error;

use rust_practice::transforms::chain::{TransformChain};
use futures::future::join_all;

use std::collections::HashMap;
use rust_practice::transforms::codec_destination::{CodecDestination, CodecConfiguration};
use rust_practice::transforms::{Transforms, TransformsConfig};
use rust_practice::transforms::kafka_destination::{KafkaDestination, KafkaConfig};
use rust_practice::sources::mpsc_source::{AsyncMpsc, AsyncMpscConfig};
use rust_practice::sources::cassandra_source::{CassandraSource, CassandraConfig};
use rust_practice::transforms::mpsc::AsyncMpscTeeConfig;
use rust_practice::sources::{SourcesConfig, Sources};
use rust_practice::config::topology::Topology;
use tokio::task::JoinHandle;
use rust_practice::config::ConfigError;
use std::future::Future;


#[tokio::main(core_threads = 4)]
async fn main() -> Result<(), Box<dyn Error>> {

    //TODO: turn all this into a config object or something
    let kafka_transform_config_obj = TransformsConfig::KafkaDestination(KafkaConfig {
        keys: [("bootstrap.servers", "127.0.0.1:9092"),
            ("message.timeout.ms", "5000")].iter()
            .map(|(x,y)| (String::from(*x), String::from(*y)))
            .collect(),
    });

    let listen_addr = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:9043".to_string());

    let server_addr = env::args()
        .nth(2)
        .unwrap_or_else(|| "127.0.0.1:9042".to_string());

    let codec_config = TransformsConfig::CodecDestination(CodecConfiguration {
        address: server_addr,
    });

    let mut cassandra_ks: HashMap<String, Vec<String>> = HashMap::new();
    cassandra_ks.insert("system.local".to_string(), vec!["key".to_string()]);
    cassandra_ks.insert("test.simple".to_string(), vec!["pk".to_string()]);
    cassandra_ks.insert("test.clustering".to_string(), vec!["pk".to_string(), "clustering".to_string()]);

    let mpsc_config = SourcesConfig::Mpsc(AsyncMpscConfig{
        topic_name: String::from("testtopic")
    });

    let cassandra_source = SourcesConfig::Cassandra(CassandraConfig{
        listen_addr,
        cassandra_ks
    });

    let tee_conf = TransformsConfig::MPSCTee(AsyncMpscTeeConfig{
        topic_name: String::from("testtopic")
    });

    let mut sources: HashMap<String, SourcesConfig> = HashMap::new();
    sources.insert(String::from("cassandra_prod"), cassandra_source);
    sources.insert(String::from("mpsc_chan"), mpsc_config);

    let mut chain_config: HashMap<String, Vec<TransformsConfig>> = HashMap::new();
    chain_config.insert(String::from("main_chain"), vec![tee_conf, codec_config]);
    chain_config.insert(String::from("async_chain"), vec![kafka_transform_config_obj]);
    let named_topics: Vec<String> = vec![String::from("testtopic")];
    let mut source_to_chain_mapping: HashMap<String, String> = HashMap::new();
    source_to_chain_mapping.insert(String::from("cassandra_prod"), String::from("main_chain"));
    source_to_chain_mapping.insert(String::from("mpsc_chan"), String::from("async_chain"));

    let topology = Topology {
        sources,
        chain_config,
        named_topics,
        source_to_chain_mapping
    };

    if let Ok(sources) = topology.run_chains().await {
        //TODO: probably a better way to handle various join handles / threads
        for s in sources {
            let _ = match s {
                Sources::Cassandra(c) => {tokio::join!(c.join_handle)},
                Sources::Mpsc(m) => {tokio::join!(m.rx_handle)},
            };
        }
    }
    Ok(())
}
// }
//
// #[tokio::main(core_threads = 4)]
// async fn main() -> Result<(), Box<dyn Error>> {
//
//     //TODO: turn all this into a config object or something
//     //Build config for Topic Source and chain
//     let kafka_config: HashMap<String, String> =
//         [("bootstrap.servers", "127.0.0.1:9092"),
//         ("message.timeout.ms", "5000")].iter()
//             .map(|(x,y)| (String::from(*x), String::from(*y)))
//             .collect();
//
//     let t = Transforms::KafkaDestination(KafkaDestination::new_from_config(&kafka_config));
//     let mpsc_chain = TransformChain::new(vec![t], String::from("kafka_log"));
//     // let mpsc = AsyncMpsc::new(mpsc_chain);
//
//     // Build config for C* Source and chain
//     let listen_addr = env::args()
//     .nth(1)
//     .unwrap_or_else(|| "127.0.0.1:9043".to_string());
//
//     let server_addr = env::args()
//     .nth(2)
//     .unwrap_or_else(|| "127.0.0.1:9042".to_string());
//
//
//     // Build the C* chain, include the Tee to the other chain... this is the main way in which we
//     // will build async topologies
//
//     let cstar_dest = CodecDestination::get_transform_enum_from_config(server_addr).await;
//     // let mpsc_tee = mpsc.get_async_mpsc_tee_enum();
//     let chain = TransformChain::new(vec![cstar_dest], String::from("test"));
//
//     let mut cassandra_ks: HashMap<String, Vec<String>> = HashMap::new();
//     cassandra_ks.insert("system.local".to_string(), vec!["key".to_string()]);
//     cassandra_ks.insert("test.simple".to_string(), vec!["pk".to_string()]);
//     cassandra_ks.insert("test.clustering".to_string(), vec!["pk".to_string(), "clustering".to_string()]);
//
//     let source = CassandraSource::new(chain, listen_addr, cassandra_ks);
//
//     //TODO: probably a better way to handle various join handles / threads
//     let _ = tokio::join!(source.join_handle);
//     Ok(())
// }