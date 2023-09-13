use crate::config::chain::TransformChainConfig;
use crate::sources::{Source, SourceConfig};
use crate::transforms::chain::TransformChainBuilder;
use anyhow::{anyhow, Context, Result};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::sync::watch;
use tracing::info;

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct Topology {
    pub sources: HashMap<String, SourceConfig>,
    pub chain_config: HashMap<String, TransformChainConfig>,
    pub source_to_chain_mapping: HashMap<String, String>,
}

impl Topology {
    /// Load the topology.yaml from the provided path into a Topology instance
    pub fn from_file(filepath: &str) -> Result<Topology> {
        let file = std::fs::File::open(filepath)
            .with_context(|| format!("Couldn't open the topology file {}", filepath))?;

        let deserializer = serde_yaml::Deserializer::from_reader(file);
        serde_yaml::with::singleton_map_recursive::deserialize(deserializer)
            .with_context(|| format!("Failed to parse topology file {}", filepath))
    }

    /// Generate the yaml representation of this instance
    pub fn serialize(&self) -> Result<String> {
        let mut output = vec![];
        let mut serializer = serde_yaml::Serializer::new(&mut output);
        serde_yaml::with::singleton_map_recursive::serialize(self, &mut serializer)?;
        Ok(String::from_utf8(output).unwrap())
    }

    async fn build_chains(&self) -> Result<HashMap<String, Option<TransformChainBuilder>>> {
        let mut result = HashMap::new();
        for (source_name, chain_name) in &self.source_to_chain_mapping {
            let chain_config = self.chain_config.get(chain_name);
            result.insert(
                source_name.clone(),
                match chain_config {
                    Some(chain_config) => Some(chain_config.get_builder(chain_name.clone()).await?),
                    None => None,
                },
            );
        }
        Ok(result)
    }

    pub async fn run_chains(
        &self,
        trigger_shutdown_rx: watch::Receiver<bool>,
    ) -> Result<Vec<Source>> {
        let mut sources_list: Vec<Source> = Vec::new();

        let mut chains = self.build_chains().await?;
        info!("Loaded chains {:?}", chains.keys());

        let mut chain_errors = String::new();
        for chain in chains
            .values()
            .sorted_by_key(|x| x.as_ref().map(|x| x.name.clone()))
            .flatten()
        {
            let errs = chain.validate().join("\n");

            if !errs.is_empty() {
                chain_errors.push_str(&errs);
                chain_errors.push('\n');
            }
        }

        if !chain_errors.is_empty() {
            return Err(anyhow!("Topology errors\n{chain_errors}"));
        }

        if self.source_to_chain_mapping.is_empty() {
            return Err(anyhow!("source_to_chain_mapping is empty"));
        }

        for (source_name, chain_name) in &self.source_to_chain_mapping {
            if let Some(source_config) = self.sources.get(source_name.as_str()) {
                if let Some(Some(chain)) = chains.remove(source_name.as_str()) {
                    sources_list.push(
                        source_config
                            .get_source(chain, trigger_shutdown_rx.clone())
                            .await
                            .with_context(|| {
                                format!("Failed to initialize source {source_name}")
                            })?,
                    );
                } else {
                    return Err(anyhow!(
                        "Could not find the {:?} chain from \
                    the source to chain mapping definition {:?} in list of configured chains {:?}.",
                        chain_name.as_str(),
                        &self
                            .source_to_chain_mapping
                            .values()
                            .cloned()
                            .collect::<Vec<_>>(),
                        self.chain_config.keys().collect::<Vec<_>>()
                    ));
                }
            } else {
                return Err(anyhow!("Could not find the {:?} source from \
                    the source to chain mapping definition {:?} in list of configured sources {:?}.",
                                                    source_name.as_str(),
                                                    &self.source_to_chain_mapping.keys().cloned().collect::<Vec<_>>(),
                                                    self.sources.keys().cloned().collect::<Vec<_>>()));
            }
        }

        // This info log is considered part of our external API.
        // Users rely on this to know when shotover is ready in their integration tests.
        // In production they would probably just have some kind of retry mechanism though.
        info!("Shotover is now accepting inbound connections");
        Ok(sources_list)
    }
}

#[cfg(test)]
mod topology_tests {
    use crate::config::chain::TransformChainConfig;
    use crate::config::topology::Topology;
    use crate::transforms::coalesce::CoalesceConfig;
    use crate::transforms::debug::printer::DebugPrinterConfig;
    use crate::transforms::null::NullSinkConfig;
    use crate::transforms::TransformConfig;
    use crate::{
        sources::{redis::RedisConfig, Source, SourceConfig},
        transforms::{
            distributed::tuneable_consistency_scatter::TuneableConsistencyScatterConfig,
            parallel_map::ParallelMapConfig, redis::cache::RedisConfig as RedisCacheConfig,
        },
    };
    use std::collections::HashMap;
    use tokio::sync::watch;

    fn create_chain_config_and_sources(
        chain: Vec<Box<dyn TransformConfig>>,
    ) -> (
        HashMap<String, TransformChainConfig>,
        HashMap<String, SourceConfig>,
    ) {
        let mut chain_config = HashMap::new();
        chain_config.insert("redis_chain".to_string(), TransformChainConfig(chain));

        let redis_source = SourceConfig::Redis(RedisConfig {
            listen_addr: "127.0.0.1:0".to_string(),
            connection_limit: None,
            hard_connection_limit: None,
            tls: None,
            timeout: None,
        });

        let mut sources = HashMap::new();
        sources.insert("redis_prod".to_string(), redis_source);

        (chain_config, sources)
    }

    fn create_source_to_chain_mapping() -> HashMap<String, String> {
        let mut source_to_chain_mapping = HashMap::<String, String>::new();
        source_to_chain_mapping.insert("redis_prod".into(), "redis_chain".into());

        source_to_chain_mapping
    }

    async fn run_test_topology(
        chain: Vec<Box<dyn TransformConfig>>,
        source_to_chain_mapping: HashMap<String, String>,
    ) -> anyhow::Result<Vec<Source>> {
        let (chain_config, sources) = create_chain_config_and_sources(chain);

        let topology = Topology {
            sources,
            chain_config,
            source_to_chain_mapping,
        };

        let (_sender, trigger_shutdown_rx) = watch::channel::<bool>(false);

        topology.run_chains(trigger_shutdown_rx).await
    }

    #[tokio::test]
    async fn test_empty_source_to_chain_mapping() {
        let expected = "source_to_chain_mapping is empty";

        let error = run_test_topology(vec![Box::new(NullSinkConfig)], HashMap::new())
            .await
            .unwrap_err()
            .to_string();
        assert_eq!(error, expected);
    }

    #[tokio::test]
    async fn test_validate_chain_empty_chain() {
        let expected = r#"Topology errors
redis_chain:
  Chain cannot be empty
"#;

        let error = run_test_topology(vec![], create_source_to_chain_mapping())
            .await
            .unwrap_err()
            .to_string();
        assert_eq!(error, expected);
    }

    #[tokio::test]
    async fn test_validate_chain_valid_chain() {
        run_test_topology(
            vec![Box::new(DebugPrinterConfig), Box::new(NullSinkConfig)],
            create_source_to_chain_mapping(),
        )
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

        let error = run_test_topology(
            vec![
                Box::new(CoalesceConfig {
                    flush_when_buffered_message_count: None,
                    flush_when_millis_since_last_flush: None,
                }),
                Box::new(NullSinkConfig),
            ],
            create_source_to_chain_mapping(),
        )
        .await
        .unwrap_err()
        .to_string();

        assert_eq!(error, expected);
    }

    #[tokio::test]
    async fn test_validate_chain_terminating_in_middle() {
        let expected = r#"Topology errors
redis_chain:
  Terminating transform "NullSink" is not last in chain. Terminating transform must be last in chain.
"#;

        let error = run_test_topology(
            vec![
                Box::new(DebugPrinterConfig),
                Box::new(NullSinkConfig),
                Box::new(NullSinkConfig),
            ],
            create_source_to_chain_mapping(),
        )
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

        let error = run_test_topology(
            vec![
                Box::new(DebugPrinterConfig),
                Box::new(DebugPrinterConfig),
                Box::new(DebugPrinterConfig),
            ],
            create_source_to_chain_mapping(),
        )
        .await
        .unwrap_err()
        .to_string();

        assert_eq!(error, expected);
    }

    #[tokio::test]
    async fn test_validate_chain_terminating_middle_non_terminating_at_end() {
        let expected = r#"Topology errors
redis_chain:
  Terminating transform "NullSink" is not last in chain. Terminating transform must be last in chain.
  Non-terminating transform "DebugPrinter" is last in chain. Last transform must be terminating.
"#;

        let error = run_test_topology(
            vec![
                Box::new(DebugPrinterConfig),
                Box::new(DebugPrinterConfig),
                Box::new(NullSinkConfig),
                Box::new(DebugPrinterConfig),
            ],
            create_source_to_chain_mapping(),
        )
        .await
        .unwrap_err()
        .to_string();

        assert_eq!(error, expected);
    }

    #[tokio::test]
    async fn test_validate_chain_valid_subchain_consistent_scatter() {
        let subchain = TransformChainConfig(vec![
            Box::new(DebugPrinterConfig) as Box<dyn TransformConfig>,
            Box::new(DebugPrinterConfig),
            Box::new(NullSinkConfig),
        ]);

        let mut route_map = HashMap::new();
        route_map.insert("subchain-1".to_string(), subchain);

        run_test_topology(
            vec![
                Box::new(DebugPrinterConfig),
                Box::new(DebugPrinterConfig),
                Box::new(TuneableConsistencyScatterConfig {
                    route_map,
                    write_consistency: 1,
                    read_consistency: 1,
                }),
            ],
            create_source_to_chain_mapping(),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_validate_chain_invalid_subchain_scatter() {
        let expected = r#"Topology errors
redis_chain:
  TuneableConsistencyScatter:
    subchain-1:
      Terminating transform "NullSink" is not last in chain. Terminating transform must be last in chain.
"#;

        let subchain = TransformChainConfig(vec![
            Box::new(DebugPrinterConfig) as Box<dyn TransformConfig>,
            Box::new(NullSinkConfig),
            Box::new(DebugPrinterConfig),
            Box::new(NullSinkConfig),
        ]);

        let mut route_map = HashMap::new();
        route_map.insert("subchain-1".to_string(), subchain);

        let error = run_test_topology(
            vec![
                Box::new(DebugPrinterConfig),
                Box::new(DebugPrinterConfig),
                Box::new(TuneableConsistencyScatterConfig {
                    route_map,
                    write_consistency: 1,
                    read_consistency: 1,
                }),
            ],
            create_source_to_chain_mapping(),
        )
        .await
        .unwrap_err()
        .to_string();

        assert_eq!(error, expected);
    }

    #[tokio::test]
    async fn test_validate_chain_valid_subchain_redis_cache() {
        let caching_schema = HashMap::new();

        run_test_topology(
            vec![
                Box::new(DebugPrinterConfig),
                Box::new(DebugPrinterConfig),
                Box::new(RedisCacheConfig {
                    chain: TransformChainConfig(vec![
                        Box::new(DebugPrinterConfig),
                        Box::new(DebugPrinterConfig),
                        Box::new(NullSinkConfig),
                    ]),
                    caching_schema,
                }),
                Box::new(NullSinkConfig),
            ],
            create_source_to_chain_mapping(),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_validate_chain_invalid_subchain_redis_cache() {
        let expected = r#"Topology errors
redis_chain:
  RedisCache:
    cache_chain:
      Terminating transform "NullSink" is not last in chain. Terminating transform must be last in chain.
"#;

        let error = run_test_topology(
            vec![
                Box::new(DebugPrinterConfig),
                Box::new(DebugPrinterConfig),
                Box::new(RedisCacheConfig {
                    chain: TransformChainConfig(vec![
                        Box::new(DebugPrinterConfig),
                        Box::new(NullSinkConfig),
                        Box::new(DebugPrinterConfig),
                        Box::new(NullSinkConfig),
                    ]),
                    caching_schema: HashMap::new(),
                }),
                Box::new(NullSinkConfig),
            ],
            create_source_to_chain_mapping(),
        )
        .await
        .unwrap_err()
        .to_string();

        assert_eq!(error, expected);
    }

    #[tokio::test]
    async fn test_validate_chain_valid_subchain_parallel_map() {
        run_test_topology(
            vec![
                Box::new(DebugPrinterConfig),
                Box::new(DebugPrinterConfig),
                Box::new(ParallelMapConfig {
                    parallelism: 1,
                    chain: TransformChainConfig(vec![
                        Box::new(DebugPrinterConfig),
                        Box::new(DebugPrinterConfig),
                        Box::new(NullSinkConfig),
                    ]),
                    ordered_results: false,
                }),
            ],
            create_source_to_chain_mapping(),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_validate_chain_invalid_subchain_parallel_map() {
        let expected = r#"Topology errors
redis_chain:
  ParallelMap:
    parallel_map_chain:
      Terminating transform "NullSink" is not last in chain. Terminating transform must be last in chain.
"#;

        let error = run_test_topology(
            vec![
                Box::new(DebugPrinterConfig),
                Box::new(DebugPrinterConfig),
                Box::new(ParallelMapConfig {
                    parallelism: 1,
                    chain: TransformChainConfig(vec![
                        Box::new(DebugPrinterConfig),
                        Box::new(NullSinkConfig),
                        Box::new(DebugPrinterConfig),
                        Box::new(NullSinkConfig),
                    ]),
                    ordered_results: false,
                }),
            ],
            create_source_to_chain_mapping(),
        )
        .await
        .unwrap_err()
        .to_string();

        assert_eq!(error, expected);
    }

    #[tokio::test]
    async fn test_validate_chain_subchain_terminating_in_middle() {
        let expected = r#"Topology errors
redis_chain:
  TuneableConsistencyScatter:
    subchain-1:
      Terminating transform "NullSink" is not last in chain. Terminating transform must be last in chain.
"#;

        let subchain = TransformChainConfig(vec![
            Box::new(DebugPrinterConfig),
            Box::new(NullSinkConfig),
            Box::new(DebugPrinterConfig),
            Box::new(NullSinkConfig),
        ]);

        let mut route_map = HashMap::new();
        route_map.insert("subchain-1".to_string(), subchain);

        let error = run_test_topology(
            vec![
                Box::new(DebugPrinterConfig),
                Box::new(DebugPrinterConfig),
                Box::new(TuneableConsistencyScatterConfig {
                    route_map,
                    write_consistency: 1,
                    read_consistency: 1,
                }),
            ],
            create_source_to_chain_mapping(),
        )
        .await
        .unwrap_err()
        .to_string();

        assert_eq!(error, expected);
    }

    #[tokio::test]
    async fn test_validate_chain_subchain_non_terminating_at_end() {
        let expected = r#"Topology errors
redis_chain:
  TuneableConsistencyScatter:
    subchain-1:
      Non-terminating transform "DebugPrinter" is last in chain. Last transform must be terminating.
"#;

        let subchain = TransformChainConfig(vec![
            Box::new(DebugPrinterConfig),
            Box::new(DebugPrinterConfig),
        ]);

        let mut route_map = HashMap::new();
        route_map.insert("subchain-1".to_string(), subchain);

        let error = run_test_topology(
            vec![
                Box::new(DebugPrinterConfig),
                Box::new(DebugPrinterConfig),
                Box::new(TuneableConsistencyScatterConfig {
                    route_map,
                    write_consistency: 1,
                    read_consistency: 1,
                }),
            ],
            create_source_to_chain_mapping(),
        )
        .await
        .unwrap_err()
        .to_string();

        assert_eq!(error, expected);
    }

    #[tokio::test]
    async fn test_validate_chain_subchain_terminating_middle_non_terminating_at_end() {
        let expected = r#"Topology errors
redis_chain:
  TuneableConsistencyScatter:
    subchain-1:
      Terminating transform "NullSink" is not last in chain. Terminating transform must be last in chain.
      Non-terminating transform "DebugPrinter" is last in chain. Last transform must be terminating.
"#;

        let subchain = TransformChainConfig(vec![
            Box::new(DebugPrinterConfig),
            Box::new(NullSinkConfig),
            Box::new(DebugPrinterConfig),
        ]);

        let mut route_map = HashMap::new();
        route_map.insert("subchain-1".to_string(), subchain);

        let error = run_test_topology(
            vec![
                Box::new(DebugPrinterConfig),
                Box::new(DebugPrinterConfig),
                Box::new(TuneableConsistencyScatterConfig {
                    route_map,
                    write_consistency: 1,
                    read_consistency: 1,
                }),
            ],
            create_source_to_chain_mapping(),
        )
        .await
        .unwrap_err()
        .to_string();

        assert_eq!(error, expected);
    }

    #[tokio::test]
    async fn test_validate_chain_multiple_subchains() {
        let (_sender, trigger_shutdown_rx) = watch::channel::<bool>(false);

        let topology =
            Topology::from_file("../shotover-proxy/tests/test-configs/invalid_subchains.yaml")
                .unwrap();
        let error = topology
            .run_chains(trigger_shutdown_rx)
            .await
            .unwrap_err()
            .to_string();

        let expected = r#"Topology errors
a_first_chain:
  Terminating transform "NullSink" is not last in chain. Terminating transform must be last in chain.
  Terminating transform "NullSink" is not last in chain. Terminating transform must be last in chain.
  Non-terminating transform "DebugPrinter" is last in chain. Last transform must be terminating.
b_second_chain:
  TuneableConsistencyScatter:
    a_chain_1:
      Terminating transform "NullSink" is not last in chain. Terminating transform must be last in chain.
      Non-terminating transform "DebugPrinter" is last in chain. Last transform must be terminating.
    b_chain_2:
      Terminating transform "NullSink" is not last in chain. Terminating transform must be last in chain.
    c_chain_3:
      TuneableConsistencyScatter:
        sub_chain_2:
          Terminating transform "NullSink" is not last in chain. Terminating transform must be last in chain.
"#;

        assert_eq!(error, expected);
    }
}
