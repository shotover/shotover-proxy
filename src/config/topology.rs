use crate::error::ChainResponse;
use crate::message::Messages;
use crate::sources::cassandra_source::CassandraConfig;
use crate::sources::mpsc_source::AsyncMpscConfig;
use crate::sources::{Sources, SourcesConfig};
use crate::transforms::cassandra_codec_destination::CodecConfiguration;
use crate::transforms::chain::TransformChain;
use crate::transforms::kafka_destination::KafkaConfig;
use crate::transforms::mpsc::TeeConfig;
use crate::transforms::{build_chain_from_config, TransformsConfig};
use anyhow::{anyhow, Result};
use bytes::Bytes;
use evmap::ReadHandleFactory;
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
    pub named_topics: Vec<String>,
    pub source_to_chain_mapping: HashMap<String, String>,
}

#[derive(Debug)]
pub struct ChannelMessage {
    pub messages: Messages,
    pub return_chan: Option<OneSender<ChainResponse>>,
    pub public_client: Option<String>,
}

impl ChannelMessage {
    pub fn new_with_no_return(m: Messages, public_client: Option<String>) -> Self {
        ChannelMessage {
            messages: m,
            return_chan: None,
            public_client,
        }
    }

    pub fn new(
        m: Messages,
        return_chan: OneSender<ChainResponse>,
        public_client: Option<String>,
    ) -> Self {
        ChannelMessage {
            messages: m,
            return_chan: Some(return_chan),
            public_client,
        }
    }
}

pub struct TopicHolder {
    pub topics_rx: HashMap<String, Receiver<ChannelMessage>>,
    pub topics_tx: HashMap<String, Sender<ChannelMessage>>,
    pub global_tx: Sender<(String, Bytes)>,
    pub global_map_handle: ReadHandleFactory<String, Bytes>,
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

    pub fn get_global_tx(&self) -> Sender<(String, Bytes)> {
        self.global_tx.clone()
    }

    pub fn get_global_map_handle(&self) -> ReadHandleFactory<String, Bytes> {
        self.global_map_handle.clone()
    }

    // #[cfg(test)]
    pub fn get_test_holder() -> Self {
        let (global_map_r, _global_map_w) = evmap::new();
        let (global_tx, _global_rx) = channel(1);

        TopicHolder {
            topics_rx: Default::default(),
            topics_tx: Default::default(),
            global_tx,
            global_map_handle: global_map_r.factory(),
        }
    }
}

impl Topology {
    pub fn new_from_yaml(yaml_contents: String) -> Result<Topology> {
        serde_yaml::from_str(&yaml_contents).map_err(|e| anyhow!(e))
    }

    fn build_topics(
        &self,
        global_tx: Sender<(String, Bytes)>,
        global_map_handle: ReadHandleFactory<String, Bytes>,
    ) -> TopicHolder {
        let mut topics_rx: HashMap<String, Receiver<ChannelMessage>> = HashMap::new();
        let mut topics_tx: HashMap<String, Sender<ChannelMessage>> = HashMap::new();
        for name in &self.named_topics {
            let (tx, rx) = channel::<ChannelMessage>(5);
            topics_rx.insert(name.clone(), rx);
            topics_tx.insert(name.clone(), tx);
        }
        TopicHolder {
            topics_rx,
            topics_tx,
            global_tx,
            global_map_handle,
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
        let (global_map_r, mut global_map_w) = evmap::new();
        let (global_tx, mut global_rx): (Sender<(String, Bytes)>, Receiver<(String, Bytes)>) =
            channel::<(String, Bytes)>(1);

        let mut topics = self.build_topics(global_tx, global_map_r.factory());
        info!("Loaded topics {:?}", topics.topics_tx.keys());

        let mut sources_list: Vec<Sources> = Vec::new();

        let (notify_shutdown, _) = broadcast::channel(1);
        let (shutdown_complete_tx, shutdown_complete_rx) = mpsc::channel(1);

        let _ = tokio::spawn(async move {
            // this loop will exit and the task will complete when all channel tx handles are dropped
            while let Some((k, v)) = global_rx.recv().await {
                global_map_w.insert(k, v);
                global_map_w.refresh();
            }
        });

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
        let config: Topology = serde_yaml::from_reader(file)?;
        Ok(config)
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

        let mpsc_config = SourcesConfig::Mpsc(AsyncMpscConfig {
            topic_name: String::from("test_topic"),
        });

        let cassandra_source = SourcesConfig::Cassandra(CassandraConfig {
            listen_addr,
            cassandra_ks,
            bypass_query_processing: false,
            connection_limit: None,
        });

        let tee_conf = TransformsConfig::MPSCTee(TeeConfig {
            topic_name: String::from("test_topic"),
            behavior: None,
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
        let named_topics: Vec<String> = vec![String::from("test_topic")];
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
  mpsc_chan:
    Mpsc:
      topic_name: test_topic
chain_config:
  main_chain:
    - MPSCTee:
        topic_name: test_topic
    - CodecDestination:
        bypass_result_processing: false
        remote_address: "127.0.0.1:9042"
  async_chain:
    - KafkaDestination:
        topic: "test_topic"
        config_values:
          bootstrap.servers: "127.0.0.1:9092"
          message.timeout.ms: "5000"
named_topics:
  - test_topic
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
