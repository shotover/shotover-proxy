use crate::message::Message;
use crate::sources::cassandra_source::CassandraConfig;
use crate::sources::mpsc_source::AsyncMpscConfig;
use crate::sources::{Sources, SourcesConfig};
use crate::transforms::chain::TransformChain;
use crate::transforms::cassandra_codec_destination::CodecConfiguration;
use crate::transforms::kafka_destination::KafkaConfig;
use crate::transforms::mpsc::AsyncMpscTeeConfig;
use crate::transforms::{build_chain_from_config, TransformsConfig};
use serde::{Deserialize, Serialize};
use tracing::info;
use std::collections::HashMap;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::{broadcast, mpsc};
use anyhow::{anyhow, Result};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Topology {
    pub sources: HashMap<String, SourcesConfig>,
    pub chain_config: HashMap<String, Vec<TransformsConfig>>,
    pub named_topics: Vec<String>,
    pub source_to_chain_mapping: HashMap<String, String>,
}

pub struct TopicHolder {
    pub topics_rx: HashMap<String, Receiver<Message>>,
    pub topics_tx: HashMap<String, Sender<Message>>,
}

impl TopicHolder {
    pub fn get_rx(&mut self, name: &str) -> Option<Receiver<Message>> {
        let rx = self.topics_rx.remove(name)?;
        return Some(rx);
    }

    pub fn get_tx(&self, name: &str) -> Option<Sender<Message>> {
        let tx = self.topics_tx.get(name)?;
        return Some(tx.clone());
    }
}

impl Topology {
    pub fn new_from_yaml(yaml_contents: String) -> Result<Topology> {
        serde_yaml::from_str(&yaml_contents).map_err(|e| anyhow!(e))
    }

    fn build_topics(&self) -> TopicHolder {
        let mut topics_rx: HashMap<String, Receiver<Message>> = HashMap::new();
        let mut topics_tx: HashMap<String, Sender<Message>> = HashMap::new();
        for name in &self.named_topics {
            let (tx, rx) = channel::<Message>(5);
            topics_rx.insert(name.clone(), rx);
            topics_tx.insert(name.clone(), tx);
        }
        return TopicHolder {
            topics_rx,
            topics_tx,
        };
    }

    async fn build_chains(
        &self,
        topics: &TopicHolder,
    ) -> Result<HashMap<String, TransformChain>> {
        let mut temp: HashMap<String, TransformChain> = HashMap::new();
        for (key, value) in self.chain_config.clone() {
            temp.insert(
                key.clone(),
                build_chain_from_config(key, &value, &topics).await?,
            );
        }
        Ok(temp)
    }

    pub async fn run_chains(&self) -> Result<(Vec<Sources>, Receiver<()>)> {
        let mut topics = self.build_topics();
        info!("Loaded topics {:?}", topics.topics_tx.keys());

        let chains = self.build_chains(&topics).await?;
        info!("Loaded chains {:?}", chains.keys());

        let mut sources_list: Vec<Sources> = Vec::new();

        // Signal handling goes here... why not?
        let (notify_shutdown, _) = broadcast::channel(1);
        let (shutdown_complete_tx, shutdown_complete_rx) = mpsc::channel(1);

        for (source_name, chain_name) in &self.source_to_chain_mapping {
            if let Some(source_config) = self.sources.get(source_name.as_str()) {
                if let Some(chain) = chains.get(chain_name.as_str()) {
                    sources_list.push(source_config.get_source(chain, &mut topics, notify_shutdown.clone(), shutdown_complete_tx.clone()).await?);
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
        if let Ok(f) = std::fs::File::open(filepath.clone()) {
            let config: Topology = serde_yaml::from_reader(f)?;
            return Ok(config);
        }
        //TODO: Make Config errors implement the From trait for IO errors
        Err(
            anyhow!("Couldn't open the file {}", &filepath),
        )
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
        });

        let listen_addr = "127.0.0.1:9043".to_string();

        let server_addr = "127.0.0.1:9042".to_string();

        let codec_config = TransformsConfig::CodecDestination(CodecConfiguration {
            address: server_addr,
        });

        let mut cassandra_ks: HashMap<String, Vec<String>> = HashMap::new();
        cassandra_ks.insert("system.local".to_string(), vec!["key".to_string()]);
        cassandra_ks.insert("test.simple".to_string(), vec!["pk".to_string()]);
        cassandra_ks.insert(
            "test.clustering".to_string(),
            vec!["pk".to_string(), "clustering".to_string()],
        );

        let mpsc_config = SourcesConfig::Mpsc(AsyncMpscConfig {
            topic_name: String::from("testtopic"),
        });

        let cassandra_source = SourcesConfig::Cassandra(CassandraConfig {
            listen_addr,
            cassandra_ks,
        });

        let tee_conf = TransformsConfig::MPSCTee(AsyncMpscTeeConfig {
            topic_name: String::from("testtopic"),
        });

        let mut sources: HashMap<String, SourcesConfig> = HashMap::new();
        sources.insert(String::from("cassandra_prod"), cassandra_source);
        sources.insert(String::from("mpsc_chan"), mpsc_config);

        let mut chain_config: HashMap<String, Vec<TransformsConfig>> = HashMap::new();
        chain_config.insert(String::from("main_chain"), vec![tee_conf, codec_config]);
        chain_config.insert(
            String::from("async_chain"),
            vec![kafka_transform_config_obj],
        );
        let named_topics: Vec<String> = vec![String::from("testtopic")];
        let mut source_to_chain_mapping: HashMap<String, String> = HashMap::new();
        source_to_chain_mapping.insert(String::from("cassandra_prod"), String::from("main_chain"));
        source_to_chain_mapping.insert(String::from("mpsc_chan"), String::from("async_chain"));

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
      listen_addr: "127.0.0.1:9043"
      cassandra_ks:
        system.local:
          - key
        test.simple:
          - pk
        test.clustering:
          - pk
          - clustering
  mpsc_chan:
    Mpsc:
      topic_name: testtopic
chain_config:
  main_chain:
    - MPSCTee:
        topic_name: testtopic
    - CodecDestination:
        remote_address: "127.0.0.1:9042"
  async_chain:
    - KafkaDestination:
        config_values:
          bootstrap.servers: "127.0.0.1:9092"
          message.timeout.ms: "5000"
named_topics:
  - testtopic
source_to_chain_mapping:
  cassandra_prod: main_chain
  mpsc_chan: async_chain"###;

    #[test]
    fn new_test() -> Result<()> {
        let topology = Topology::get_demo_config();
        let topology2 = Topology::new_from_yaml(String::from(TEST_STRING))?;
        assert_eq!(topology2, topology);
        Ok(())
    }

    #[test]
    fn test_config_parse_format() -> Result<()> {
        let _ = Topology::new_from_yaml(String::from(TEST_STRING))?;
        Ok(())
    }
}
