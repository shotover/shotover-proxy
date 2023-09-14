use crate::config::chain::TransformChainConfig;
use crate::sources::Source;
use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
use std::collections::HashMap;
use tokio::sync::watch;
use tracing::info;

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct Topology {
    pub chain_config: HashMap<String, TransformChainConfig>,
}

impl Topology {
    pub fn from_file(filepath: &str) -> Result<Topology> {
        let file = std::fs::File::open(filepath)
            .with_context(|| format!("Couldn't open the topology file {}", filepath))?;

        let deserializer = serde_yaml::Deserializer::from_reader(file);
        serde_yaml::with::singleton_map_recursive::deserialize(deserializer)
            .with_context(|| format!("Failed to parse topology file {}", filepath))
    }

    pub async fn run_chains(
        &self,
        trigger_shutdown_rx: watch::Receiver<bool>,
    ) -> Result<Vec<Source>> {
        let mut chain_builders = vec![];
        let mut chain_errors = String::new();
        for (name, chain) in &self.chain_config {
            match chain.get_source_and_builder(name.clone()).await {
                Ok(source_and_chain) => chain_builders.push(source_and_chain),
                Err(err) => {
                    chain_errors.push_str(&err);
                    chain_errors.push('\n');
                }
            }
        }
        chain_builders.sort_by_key(|x| x.1.name.clone());

        for (_, chain_builder) in &chain_builders {
            let errs = chain_builder.validate().join("\n");

            if !errs.is_empty() {
                chain_errors.push_str(&errs);
                chain_errors.push('\n');
            }
        }

        if !chain_errors.is_empty() {
            return Err(anyhow!("Topology errors\n{chain_errors}"));
        }

        let mut sources_list: Vec<Source> = Vec::new();
        for (source_builder, chain_builder) in chain_builders {
            sources_list.push(
                source_builder
                    .build(chain_builder, trigger_shutdown_rx.clone())
                    .await?,
            );
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
        sources::{redis::RedisSourceConfig, Source},
        transforms::{
            distributed::tuneable_consistency_scatter::TuneableConsistencyScatterConfig,
            parallel_map::ParallelMapConfig, redis::cache::RedisConfig as RedisCacheConfig,
        },
    };
    use std::collections::HashMap;
    use tokio::sync::watch;

    fn create_source() -> Box<dyn TransformConfig> {
        Box::new(RedisSourceConfig {
            listen_addr: "127.0.0.1:0".to_string(),
            connection_limit: None,
            hard_connection_limit: None,
            tls: None,
            timeout: None,
        })
    }

    async fn run_test_topology(
        chain: Vec<Box<dyn TransformConfig>>,
    ) -> anyhow::Result<Vec<Source>> {
        let topology = Topology {
            chain_config: HashMap::from([("redis_chain".into(), TransformChainConfig(chain))]),
        };

        let (_sender, trigger_shutdown_rx) = watch::channel::<bool>(false);

        topology.run_chains(trigger_shutdown_rx).await
    }

    #[tokio::test]
    async fn test_validate_chain_root_chain_empty() {
        let expected = r#"Topology errors
redis_chain:
  The chain is empty, but it must contain at least a source transform and a terminating transform.
"#;

        let error = run_test_topology(vec![]).await.unwrap_err().to_string();
        assert_eq!(error, expected);
    }

    #[tokio::test]
    async fn test_validate_chain_root_chain_has_only_source() {
        let expected = r#"Topology errors
redis_chain:
  The chain contains only the source transform RedisSource with no terminating transform.
"#;

        let error = run_test_topology(vec![create_source()])
            .await
            .unwrap_err()
            .to_string();
        assert_eq!(error, expected);
    }

    #[tokio::test]
    async fn test_validate_chain_valid_chain() {
        run_test_topology(vec![
            create_source(),
            Box::new(DebugPrinterConfig),
            Box::new(NullSinkConfig),
        ])
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
            create_source(),
            Box::new(CoalesceConfig {
                flush_when_buffered_message_count: None,
                flush_when_millis_since_last_flush: None,
            }),
            Box::new(NullSinkConfig),
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
  Terminating transform "NullSink" is not last in chain. Terminating transform must be last in chain.
"#;

        let error = run_test_topology(vec![
            create_source(),
            Box::new(DebugPrinterConfig),
            Box::new(NullSinkConfig),
            Box::new(NullSinkConfig),
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
            create_source(),
            Box::new(DebugPrinterConfig),
            Box::new(DebugPrinterConfig),
            Box::new(DebugPrinterConfig),
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
  Terminating transform "NullSink" is not last in chain. Terminating transform must be last in chain.
  Non-terminating transform "DebugPrinter" is last in chain. Last transform must be terminating.
"#;

        let error = run_test_topology(vec![
            create_source(),
            Box::new(DebugPrinterConfig),
            Box::new(DebugPrinterConfig),
            Box::new(NullSinkConfig),
            Box::new(DebugPrinterConfig),
        ])
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

        run_test_topology(vec![
            create_source(),
            Box::new(DebugPrinterConfig),
            Box::new(DebugPrinterConfig),
            Box::new(TuneableConsistencyScatterConfig {
                route_map,
                write_consistency: 1,
                read_consistency: 1,
            }),
        ])
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

        let error = run_test_topology(vec![
            create_source(),
            Box::new(DebugPrinterConfig),
            Box::new(DebugPrinterConfig),
            Box::new(TuneableConsistencyScatterConfig {
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
        let caching_schema = HashMap::new();

        run_test_topology(vec![
            create_source(),
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
        ])
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

        let error = run_test_topology(vec![
            create_source(),
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
        ])
        .await
        .unwrap_err()
        .to_string();

        assert_eq!(error, expected);
    }

    #[tokio::test]
    async fn test_validate_chain_valid_subchain_parallel_map() {
        run_test_topology(vec![
            create_source(),
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
      Terminating transform "NullSink" is not last in chain. Terminating transform must be last in chain.
"#;

        let error = run_test_topology(vec![
            create_source(),
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

        let error = run_test_topology(vec![
            create_source(),
            Box::new(DebugPrinterConfig),
            Box::new(DebugPrinterConfig),
            Box::new(TuneableConsistencyScatterConfig {
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

        let error = run_test_topology(vec![
            create_source(),
            Box::new(DebugPrinterConfig),
            Box::new(DebugPrinterConfig),
            Box::new(TuneableConsistencyScatterConfig {
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

        let error = run_test_topology(vec![
            create_source(),
            Box::new(DebugPrinterConfig),
            Box::new(DebugPrinterConfig),
            Box::new(TuneableConsistencyScatterConfig {
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
