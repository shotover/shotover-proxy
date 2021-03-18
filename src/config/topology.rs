use crate::error::ChainResponse;
use crate::message::Messages;
use crate::sources::cassandra_source::CassandraConfig;
use crate::sources::{Sources, SourcesConfig};
use crate::transforms::cassandra::cassandra_codec_destination::CodecConfiguration;
use crate::transforms::chain::TransformChain;
use crate::transforms::kafka_destination::KafkaConfig;
use crate::transforms::mpsc::TeeConfig;
use crate::transforms::{build_chain_from_config, TransformsConfig};
use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::oneshot::Sender as OneSender;
use tokio::sync::{broadcast, mpsc};
use tracing::info;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Topology {
    pub sources: HashMap<String, SourcesConfig>,
    pub chain_config: HashMap<String, Vec<TransformsConfig>>,
    pub named_topics: HashMap<String, usize>,
    pub source_to_chain_mapping: HashMap<String, String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct TopologyConfig {
    pub sources: HashMap<String, SourcesConfig>,
    pub chain_config: HashMap<String, Vec<TransformsConfig>>,
    pub named_topics: Option<HashMap<String, Option<usize>>>,
    pub source_to_chain_mapping: HashMap<String, String>,
}

#[derive(Debug)]
pub struct ChannelMessage {
    pub messages: Messages,
    pub return_chan: Option<OneSender<ChainResponse>>,
}

impl ChannelMessage {
    pub fn new_with_no_return(m: Messages) -> Self {
        ChannelMessage {
            messages: m,
            return_chan: None,
        }
    }

    pub fn new(m: Messages, return_chan: OneSender<ChainResponse>) -> Self {
        ChannelMessage {
            messages: m,
            return_chan: Some(return_chan),
        }
    }
}

pub struct TopicHolder {
    pub topics_rx: HashMap<String, Receiver<ChannelMessage>>,
    pub topics_tx: HashMap<String, Sender<ChannelMessage>>,
}

impl TopicHolder {
    pub fn get_rx(&mut self, name: &str) -> Option<Receiver<ChannelMessage>> {
        let rx = self.topics_rx.remove(name)?;
        Some(rx)
    }

    pub fn get_tx(&self, name: &str) -> Option<Sender<ChannelMessage>> {
        let tx = self.topics_tx.get(name)?;
        Some(tx.clone())
    }
}

impl Topology {
    pub fn new_from_yaml(yaml_contents: String) -> Topology {
        let config: TopologyConfig = serde_yaml::from_str(&yaml_contents)
            .map_err(|e| anyhow!(e))
            .unwrap();
        Topology::topology_from_config(config)
    }

    fn build_topics(&self) -> TopicHolder {
        let mut topics_rx: HashMap<String, Receiver<ChannelMessage>> = HashMap::new();
        let mut topics_tx: HashMap<String, Sender<ChannelMessage>> = HashMap::new();
        for (name, size) in &self.named_topics {
            let (tx, rx) = channel::<ChannelMessage>(*size);
            topics_rx.insert(name.clone(), rx);
            topics_tx.insert(name.clone(), tx);
        }
        TopicHolder {
            topics_rx,
            topics_tx,
        }
    }

    async fn build_chains(&self, topics: &TopicHolder) -> Result<HashMap<String, TransformChain>> {
        let mut temp: HashMap<String, TransformChain> = HashMap::new();
        for (key, value) in self.chain_config.clone() {
            temp.insert(
                key.clone(),
                build_chain_from_config(key, &value, &topics).await?,
            );
        }
        Ok(temp)
    }

    #[allow(clippy::type_complexity)]
    pub async fn run_chains(&self) -> Result<(Vec<Sources>, Receiver<()>)> {
        let mut topics = self.build_topics();
        info!("Loaded topics {:?}", topics.topics_tx.keys());

        let mut sources_list: Vec<Sources> = Vec::new();

        let (notify_shutdown, _) = broadcast::channel(1);
        let (shutdown_complete_tx, shutdown_complete_rx) = mpsc::channel(1);

        let chains = self.build_chains(&topics).await?;
        info!("Loaded chains {:?}", chains.keys());

        for (source_name, chain_name) in &self.source_to_chain_mapping {
            if let Some(source_config) = self.sources.get(source_name.as_str()) {
                if let Some(chain) = chains.get(chain_name.as_str()) {
                    sources_list.append(
                        &mut source_config
                            .get_source(
                                chain,
                                &mut topics,
                                notify_shutdown.clone(),
                                shutdown_complete_tx.clone(),
                            )
                            .await?,
                    );
                } else {
                    return Err(anyhow!("Could not find the [{}] chain from \
                    the source to chain mapping definition [{:?}] in list of configured chains [{:?}].",
                                                        chain_name.as_str(),
                                                        &self.source_to_chain_mapping.values().cloned().collect::<Vec<_>>(),
                                                        chains.keys().cloned().collect::<Vec<_>>()));
                }
            } else {
                return Err(anyhow!("Could not find the [{}] source from \
                    the source to chain mapping definition [{:?}] in list of configured sources [{:?}].",
                                                    source_name.as_str(),
                                                    &self.source_to_chain_mapping.keys().cloned().collect::<Vec<_>>(),
                                                    self.sources.keys().cloned().collect::<Vec<_>>()));
            }
        }
        info!(
            "Loaded sources [{:?}] and linked to chains",
            &self.source_to_chain_mapping.keys()
        );
        Ok((sources_list, shutdown_complete_rx))
    }

    pub fn from_file(filepath: String) -> Result<Topology> {
        let file = std::fs::File::open(filepath)?;
        let config: TopologyConfig = serde_yaml::from_reader(file)?;
        Ok(Topology::topology_from_config(config))
    }

    pub fn from_string(topology: String) -> Result<Topology> {
        let config: TopologyConfig = serde_yaml::from_slice(topology.as_bytes())?;
        Ok(Topology::topology_from_config(config))
    }

    pub fn topology_from_config(config: TopologyConfig) -> Topology {
        let topics = config.named_topics.unwrap_or_else(|| {
            let mut default_topic = HashMap::new();
            default_topic.insert("testtopic".to_owned(), Some(5));
            default_topic
        });

        let built_topics = topics
            .iter()
            .map(|(k, v)| (k.to_owned(), v.unwrap_or(5)))
            .collect();
        return Topology {
            sources: config.sources,
            chain_config: config.chain_config,
            named_topics: built_topics,
            source_to_chain_mapping: config.source_to_chain_mapping,
        };
    }

    pub fn get_demo_config() -> Topology {
        let kafka_transform_config_obj = TransformsConfig::KafkaDestination(KafkaConfig {
            keys: [
                ("bootstrap.servers", "127.0.0.1:9092"),
                ("message.timeout.ms", "5000"),
            ]
            .iter()
            .map(|(x, y)| (String::from(*x), String::from(*y)))
            .collect(),
            topic: "test_topic".to_string(),
        });

        let listen_addr = "127.0.0.1:9043".to_string();

        let server_addr = "127.0.0.1:9042".to_string();

        let codec_config = TransformsConfig::CodecDestination(CodecConfiguration {
            address: server_addr,
            bypass_result_processing: false,
        });

        let mut cassandra_ks: HashMap<String, Vec<String>> = HashMap::new();
        cassandra_ks.insert("system.local".to_string(), vec!["key".to_string()]);
        cassandra_ks.insert("test.simple".to_string(), vec!["pk".to_string()]);
        cassandra_ks.insert(
            "test.clustering".to_string(),
            vec!["pk".to_string(), "clustering".to_string()],
        );

        let cassandra_source = SourcesConfig::Cassandra(CassandraConfig {
            listen_addr,
            cassandra_ks,
            bypass_query_processing: Some(false),
            connection_limit: None,
            hard_connection_limit: None,
        });

        let tee_conf = TransformsConfig::MPSCTee(TeeConfig {
            behavior: None,
            timeout_micros: None,
            chain: vec![kafka_transform_config_obj],
            buffer_size: None,
        });

        let mut sources: HashMap<String, SourcesConfig> = HashMap::new();
        sources.insert(String::from("cassandra_prod"), cassandra_source);

        let mut chain_config: HashMap<String, Vec<TransformsConfig>> = HashMap::new();
        chain_config.insert(String::from("main_chain"), vec![tee_conf, codec_config]);

        let mut named_topics: HashMap<String, usize> = HashMap::new();
        named_topics.insert(String::from("test_topic"), 1);

        let mut source_to_chain_mapping: HashMap<String, String> = HashMap::new();
        source_to_chain_mapping.insert(String::from("cassandra_prod"), String::from("main_chain"));

        Topology {
            sources,
            chain_config,
            named_topics,
            source_to_chain_mapping,
        }
    }
}

#[cfg(test)]
mod topology_tests {
    use crate::config::topology::Topology;
    use anyhow::Result;

    const TEST_STRING: &str = r###"---
sources:
  cassandra_prod:
    Cassandra:
      bypass_query_processing: false
      listen_addr: "127.0.0.1:9043"
      cassandra_ks:
        system.local:
          - key
        test.simple:
          - pk
        test.clustering:
          - pk
          - clustering
chain_config:
  main_chain:
    - MPSCTee:
        topic_name: test_topic
        chain:
          - KafkaDestination:
             topic: "test_topic"
             config_values:
               bootstrap.servers: "127.0.0.1:9092"
               message.timeout.ms: "5000"
    - CodecDestination:
        bypass_result_processing: false
        remote_address: "127.0.0.1:9042"    
named_topics:
  test_topic: 1
source_to_chain_mapping:
  cassandra_prod: main_chain"###;

    #[test]
    fn new_test() -> Result<()> {
        let topology = Topology::get_demo_config();
        println!("{:?}", topology.named_topics);
        let topology2 = Topology::new_from_yaml(String::from(TEST_STRING));
        println!("{:?}", topology2.named_topics);
        assert_eq!(topology2, topology);
        Ok(())
    }

    #[test]
    fn test_config_parse_format() -> Result<()> {
        let _ = Topology::new_from_yaml(String::from(TEST_STRING));
        Ok(())
    }
}
