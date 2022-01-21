use crate::error::ChainResponse;
use crate::message::Messages;
use crate::sources::{Sources, SourcesConfig};
use crate::transforms::chain::TransformChain;
use crate::transforms::{build_chain_from_config, TransformsConfig};
use anyhow::{anyhow, Result};
use itertools::Itertools;
use serde::Deserialize;
use std::collections::HashMap;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::oneshot::Sender as OneSender;
use tokio::sync::watch;
use tracing::info;

#[derive(Deserialize, Debug, Clone)]
pub struct Topology {
    pub sources: HashMap<String, SourcesConfig>,
    pub chain_config: HashMap<String, Vec<TransformsConfig>>,
    pub named_topics: HashMap<String, usize>,
    pub source_to_chain_mapping: HashMap<String, String>,
}

#[derive(Deserialize, Debug, Clone)]
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

#[derive(Default)]
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
        for (key, value) in &self.chain_config {
            temp.insert(
                key.clone(),
                build_chain_from_config(key.clone(), value, topics).await?,
            );
        }
        Ok(temp)
    }

    pub async fn run_chains(
        &self,
        trigger_shutdown_rx: watch::Receiver<bool>,
    ) -> Result<Vec<Sources>> {
        let mut topics = self.build_topics();
        info!("Loaded topics {:?}", topics.topics_tx.keys());

        let mut sources_list: Vec<Sources> = Vec::new();

        let chains = self.build_chains(&topics).await?;
        info!("Loaded chains {:?}", chains.keys());

        let mut chain_errors = String::new();
        for chain in chains.values().sorted_by_key(|x| x.name.clone()) {
            let errs = chain.validate().join("\n");

            if !errs.is_empty() {
                chain_errors.push_str(&errs);
                chain_errors.push('\n');
            }
        }

        if !chain_errors.is_empty() {
            return Err(anyhow!(format!("Topology errors\n{chain_errors}")));
        }

        for (source_name, chain_name) in &self.source_to_chain_mapping {
            if let Some(source_config) = self.sources.get(source_name.as_str()) {
                if let Some(chain) = chains.get(chain_name.as_str()) {
                    sources_list.append(
                        &mut source_config
                            .get_source(chain, &mut topics, trigger_shutdown_rx.clone())
                            .await?,
                    );
                } else {
                    return Err(anyhow!("Could not find the [{}] chain from \
                    the source to chain mapping definition [{:?}] in list of configured chains [{:?}].",
                                                        chain_name.as_str(),
                                                        &self.source_to_chain_mapping.values().cloned().collect::<Vec<_>>(),
                                                        chains.into_keys().collect::<Vec<_>>()));
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
        Ok(sources_list)
    }

    pub fn from_file(filepath: String) -> Result<Topology> {
        let file = std::fs::File::open(&filepath)
            .map_err(|err| anyhow!("Couldn't open the topology file {}: {}", &filepath, err))?;
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

        Topology {
            sources: config.sources,
            chain_config: config.chain_config,
            named_topics: built_topics,
            source_to_chain_mapping: config.source_to_chain_mapping,
        }
    }
}

#[cfg(test)]
mod topology_tests {
    use tokio::sync::watch;

    use crate::transforms::coalesce::CoalesceConfig;
    use crate::{
        sources::{redis_source::RedisConfig, Sources, SourcesConfig},
        transforms::{
            distributed::consistent_scatter::ConsistentScatterConfig,
            parallel_map::ParallelMapConfig, redis::cache::RedisConfig as RedisCacheConfig,
            TransformsConfig,
        },
    };
    use std::{collections::HashMap, fs};

    use super::{Topology, TopologyConfig};

    async fn run_test_topology(chain: Vec<TransformsConfig>) -> anyhow::Result<Vec<Sources>> {
        let mut chain_config = HashMap::new();
        chain_config.insert("redis_chain".to_string(), chain);

        let redis_source = SourcesConfig::Redis(RedisConfig {
            listen_addr: "127.0.0.1".to_string(),
            connection_limit: None,
            hard_connection_limit: None,
            tls: None,
        });

        let mut sources = HashMap::new();
        sources.insert("redis_prod".to_string(), redis_source);

        let config = TopologyConfig {
            sources,
            chain_config,
            named_topics: None,
            source_to_chain_mapping: HashMap::new(), // Leave source to chain mapping empty so it doesn't build and run the transform chains
        };

        let (_sender, trigger_shutdown_rx) = watch::channel::<bool>(false);

        let topology = Topology::topology_from_config(config);
        topology.run_chains(trigger_shutdown_rx).await
    }

    #[tokio::test]
    async fn test_validate_chain_empty_chain() {
        let expected = r#"Topology errors
redis_chain:
  Chain cannot be empty
"#;

        let error = run_test_topology(vec![]).await.unwrap_err().to_string();
        assert_eq!(error, expected);
    }

    #[tokio::test]
    async fn test_validate_chain_valid_chain() {
        run_test_topology(vec![TransformsConfig::DebugPrinter, TransformsConfig::Null])
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_validate_coalesce() {
        let expected = r#"Topology errors
redis_chain:
  Coalesce:
    Need to provide at least one of these fields:
    * flush_when_buffered_message_count
    * flush_when_millis_since_last_flush
  
    But none of them were provided.
    Check https://docs.shotover.io/transforms.html#coalesce for more information.
"#;

        let error = run_test_topology(vec![
            TransformsConfig::Coalesce(CoalesceConfig {
                flush_when_buffered_message_count: None,
                flush_when_millis_since_last_flush: None,
            }),
            TransformsConfig::Null,
        ])
        .await
        .unwrap_err()
        .to_string();

        assert_eq!(error, expected);
    }

    #[tokio::test]
    async fn test_validate_chain_terminating_in_middle() {
        let expected = r#"Topology errors
redis_chain:
  Terminating transform "Null" is not last in chain. Terminating transform must be last in chain.
"#;

        let error = run_test_topology(vec![
            TransformsConfig::DebugPrinter,
            TransformsConfig::Null,
            TransformsConfig::Null,
        ])
        .await
        .unwrap_err()
        .to_string();

        assert_eq!(error, expected);
    }

    #[tokio::test]
    async fn test_validate_chain_non_terminating_at_end() {
        let expected = r#"Topology errors
redis_chain:
  Non-terminating transform "DebugPrinter" is last in chain. Last transform must be terminating.
"#;

        let error = run_test_topology(vec![
            TransformsConfig::DebugPrinter,
            TransformsConfig::DebugPrinter,
            TransformsConfig::DebugPrinter,
        ])
        .await
        .unwrap_err()
        .to_string();

        assert_eq!(error, expected);
    }

    #[tokio::test]
    async fn test_validate_chain_terminating_middle_non_terminating_at_end() {
        let expected = r#"Topology errors
redis_chain:
  Terminating transform "Null" is not last in chain. Terminating transform must be last in chain.
  Non-terminating transform "DebugPrinter" is last in chain. Last transform must be terminating.
"#;

        let error = run_test_topology(vec![
            TransformsConfig::DebugPrinter,
            TransformsConfig::DebugPrinter,
            TransformsConfig::Null,
            TransformsConfig::DebugPrinter,
        ])
        .await
        .unwrap_err()
        .to_string();

        assert_eq!(error, expected);
    }

    #[tokio::test]
    async fn test_validate_chain_valid_subchain_consistent_scatter() {
        let subchain = vec![
            TransformsConfig::DebugPrinter,
            TransformsConfig::DebugPrinter,
            TransformsConfig::Null,
        ];

        let mut route_map = HashMap::new();
        route_map.insert("subchain-1".to_string(), subchain);

        run_test_topology(vec![
            TransformsConfig::DebugPrinter,
            TransformsConfig::DebugPrinter,
            TransformsConfig::ConsistentScatter(ConsistentScatterConfig {
                route_map,
                write_consistency: 1,
                read_consistency: 1,
            }),
        ])
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_validate_chain_invalid_subchain_consistent_scatter() {
        let expected = r#"Topology errors
redis_chain:
  ConsistentScatter:
    subchain-1:
      Terminating transform "Null" is not last in chain. Terminating transform must be last in chain.
"#;

        let subchain = vec![
            TransformsConfig::DebugPrinter,
            TransformsConfig::Null,
            TransformsConfig::DebugPrinter,
            TransformsConfig::Null,
        ];

        let mut route_map = HashMap::new();
        route_map.insert("subchain-1".to_string(), subchain);

        let error = run_test_topology(vec![
            TransformsConfig::DebugPrinter,
            TransformsConfig::DebugPrinter,
            TransformsConfig::ConsistentScatter(ConsistentScatterConfig {
                route_map,
                write_consistency: 1,
                read_consistency: 1,
            }),
        ])
        .await
        .unwrap_err()
        .to_string();

        assert_eq!(error, expected);
    }

    #[tokio::test]
    async fn test_validate_chain_valid_subchain_redis_cache() {
        let chain = vec![
            TransformsConfig::DebugPrinter,
            TransformsConfig::DebugPrinter,
            TransformsConfig::Null,
        ];

        let caching_schema = HashMap::new();

        run_test_topology(vec![
            TransformsConfig::DebugPrinter,
            TransformsConfig::DebugPrinter,
            TransformsConfig::RedisCache(RedisCacheConfig {
                chain,
                caching_schema,
            }),
            TransformsConfig::Null,
        ])
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_validate_chain_invalid_subchain_redis_cache() {
        let expected = r#"Topology errors
redis_chain:
  SimpleRedisCache:
    cache_chain:
      Terminating transform "Null" is not last in chain. Terminating transform must be last in chain.
"#;

        let chain = vec![
            TransformsConfig::DebugPrinter,
            TransformsConfig::Null,
            TransformsConfig::DebugPrinter,
            TransformsConfig::Null,
        ];

        let error = run_test_topology(vec![
            TransformsConfig::DebugPrinter,
            TransformsConfig::DebugPrinter,
            TransformsConfig::RedisCache(RedisCacheConfig {
                chain,
                caching_schema: HashMap::new(),
            }),
            TransformsConfig::Null,
        ])
        .await
        .unwrap_err()
        .to_string();

        assert_eq!(error, expected);
    }

    #[tokio::test]
    async fn test_validate_chain_valid_subchain_parallel_map() {
        let chain = vec![
            TransformsConfig::DebugPrinter,
            TransformsConfig::DebugPrinter,
            TransformsConfig::Null,
        ];

        run_test_topology(vec![
            TransformsConfig::DebugPrinter,
            TransformsConfig::DebugPrinter,
            TransformsConfig::ParallelMap(ParallelMapConfig {
                parallelism: 1,
                chain,
                ordered_results: false,
            }),
        ])
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_validate_chain_invalid_subchain_parallel_map() {
        let expected = r#"Topology errors
redis_chain:
  ParallelMap:
    parallel_map_chain:
      Terminating transform "Null" is not last in chain. Terminating transform must be last in chain.
"#;

        let chain = vec![
            TransformsConfig::DebugPrinter,
            TransformsConfig::Null,
            TransformsConfig::DebugPrinter,
            TransformsConfig::Null,
        ];

        let error = run_test_topology(vec![
            TransformsConfig::DebugPrinter,
            TransformsConfig::DebugPrinter,
            TransformsConfig::ParallelMap(ParallelMapConfig {
                parallelism: 1,
                chain,
                ordered_results: false,
            }),
        ])
        .await
        .unwrap_err()
        .to_string();

        assert_eq!(error, expected);
    }

    #[tokio::test]
    async fn test_validate_chain_subchain_terminating_in_middle() {
        let expected = r#"Topology errors
redis_chain:
  ConsistentScatter:
    subchain-1:
      Terminating transform "Null" is not last in chain. Terminating transform must be last in chain.
"#;

        let subchain = vec![
            TransformsConfig::DebugPrinter,
            TransformsConfig::Null,
            TransformsConfig::DebugPrinter,
            TransformsConfig::Null,
        ];

        let mut route_map = HashMap::new();
        route_map.insert("subchain-1".to_string(), subchain);

        let error = run_test_topology(vec![
            TransformsConfig::DebugPrinter,
            TransformsConfig::DebugPrinter,
            TransformsConfig::ConsistentScatter(ConsistentScatterConfig {
                route_map,
                write_consistency: 1,
                read_consistency: 1,
            }),
        ])
        .await
        .unwrap_err()
        .to_string();

        assert_eq!(error, expected);
    }

    #[tokio::test]
    async fn test_validate_chain_subchain_non_terminating_at_end() {
        let expected = r#"Topology errors
redis_chain:
  ConsistentScatter:
    subchain-1:
      Non-terminating transform "DebugPrinter" is last in chain. Last transform must be terminating.
"#;

        let subchain = vec![
            TransformsConfig::DebugPrinter,
            TransformsConfig::DebugPrinter,
        ];

        let mut route_map = HashMap::new();
        route_map.insert("subchain-1".to_string(), subchain);

        let error = run_test_topology(vec![
            TransformsConfig::DebugPrinter,
            TransformsConfig::DebugPrinter,
            TransformsConfig::ConsistentScatter(ConsistentScatterConfig {
                route_map,
                write_consistency: 1,
                read_consistency: 1,
            }),
        ])
        .await
        .unwrap_err()
        .to_string();

        assert_eq!(error, expected);
    }

    #[tokio::test]
    async fn test_validate_chain_subchain_terminating_middle_non_terminating_at_end() {
        let expected = r#"Topology errors
redis_chain:
  ConsistentScatter:
    subchain-1:
      Terminating transform "Null" is not last in chain. Terminating transform must be last in chain.
      Non-terminating transform "DebugPrinter" is last in chain. Last transform must be terminating.
"#;

        let subchain = vec![
            TransformsConfig::DebugPrinter,
            TransformsConfig::Null,
            TransformsConfig::DebugPrinter,
        ];

        let mut route_map = HashMap::new();
        route_map.insert("subchain-1".to_string(), subchain);

        let error = run_test_topology(vec![
            TransformsConfig::DebugPrinter,
            TransformsConfig::DebugPrinter,
            TransformsConfig::ConsistentScatter(ConsistentScatterConfig {
                route_map,
                write_consistency: 1,
                read_consistency: 1,
            }),
        ])
        .await
        .unwrap_err()
        .to_string();

        assert_eq!(error, expected);
    }

    #[tokio::test]
    async fn test_validate_chain_multiple_subchains() {
        let (_sender, trigger_shutdown_rx) = watch::channel::<bool>(false);

        let yaml_contents =
            fs::read_to_string("tests/test-topologies/invalid_subchains.yaml").unwrap();

        let topology = Topology::new_from_yaml(yaml_contents);
        let error = topology
            .run_chains(trigger_shutdown_rx)
            .await
            .unwrap_err()
            .to_string();

        let expected = r#"Topology errors
a_first_chain:
  Terminating transform "Null" is not last in chain. Terminating transform must be last in chain.
  Terminating transform "Null" is not last in chain. Terminating transform must be last in chain.
  Non-terminating transform "DebugPrinter" is last in chain. Last transform must be terminating.
b_second_chain:
  ConsistentScatter:
    a_chain_1:
      Terminating transform "Null" is not last in chain. Terminating transform must be last in chain.
      Non-terminating transform "DebugPrinter" is last in chain. Last transform must be terminating.
    b_chain_2:
      Terminating transform "Null" is not last in chain. Terminating transform must be last in chain.
    c_chain_3:
      ConsistentScatter:
        sub_chain_2:
          Terminating transform "Null" is not last in chain. Terminating transform must be last in chain.
"#;

        assert_eq!(error, expected);
    }
}
