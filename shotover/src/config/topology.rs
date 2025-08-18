use crate::hot_reload::HotReloadChannelManager;
use crate::sources::{Source, SourceConfig};
use anyhow::{Context, Result, anyhow};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::fmt::Write;
use tokio::sync::watch;
use tracing::info;

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct Topology {
    pub sources: Vec<SourceConfig>,
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

    pub async fn run_chains(
        &self,
        trigger_shutdown_rx: watch::Receiver<bool>,
        mut hot_reload_channel_manager: Option<&mut HotReloadChannelManager>,
    ) -> Result<Vec<Source>> {
        let mut sources: Vec<Source> = Vec::new();

        let mut topology_errors = String::new();

        let mut duplicated_names = vec![];
        for source in &self.sources {
            let name = source.get_name();
            if self.sources.iter().filter(|x| x.get_name() == name).count() > 1 {
                duplicated_names.push(name);
            }
        }
        for name in duplicated_names.iter().unique() {
            writeln!(
                topology_errors,
                "Source name {name:?} occurred more than once. Make sure all source names are unique. The names will be used in logging and metrics."
            )?;
        }

        for source in &self.sources {
            match source
                .get_source(
                    trigger_shutdown_rx.clone(),
                    hot_reload_channel_manager.as_deref_mut(),
                )
                .await
            {
                Ok(source) => sources.push(source),
                Err(source_errors) => {
                    if !source_errors.is_empty() {
                        topology_errors.push_str(&source_errors.join("\n"));
                        topology_errors.push('\n');
                    }
                }
            };
        }

        if !topology_errors.is_empty() {
            return Err(anyhow!("Topology errors\n{topology_errors}"));
        }

        // This info log is considered part of our external API.
        // Users rely on this to know when shotover is ready in their integration tests.
        // In production they would probably just have some kind of retry mechanism though.
        info!("Shotover is now accepting inbound connections");
        Ok(sources)
    }
}

#[cfg(all(test, feature = "valkey", feature = "cassandra"))]
mod topology_tests {
    use crate::config::chain::TransformChainConfig;
    use crate::config::topology::Topology;
    use crate::sources::cassandra::CassandraConfig;
    use crate::transforms::TransformConfig;
    use crate::transforms::coalesce::CoalesceConfig;
    use crate::transforms::debug::printer::DebugPrinterConfig;
    use crate::transforms::null::NullSinkConfig;
    use crate::{
        sources::{Source, SourceConfig, valkey::ValkeyConfig},
        transforms::{
            parallel_map::ParallelMapConfig, valkey::cache::ValkeyConfig as ValkeyCacheConfig,
        },
    };
    use pretty_assertions::assert_eq;
    use std::collections::HashMap;
    use tokio::sync::watch;

    fn create_source_from_chain_valkey(chain: Vec<Box<dyn TransformConfig>>) -> Vec<SourceConfig> {
        vec![SourceConfig::Valkey(ValkeyConfig {
            name: "foo".to_string(),
            listen_addr: "127.0.0.1:0".to_string(),
            connection_limit: None,
            hard_connection_limit: None,
            tls: None,
            timeout: None,
            chain: TransformChainConfig(chain),
        })]
    }

    fn create_source_from_chain_cassandra(
        chain: Vec<Box<dyn TransformConfig>>,
    ) -> Vec<SourceConfig> {
        vec![SourceConfig::Cassandra(CassandraConfig {
            name: "foo".to_string(),
            listen_addr: "127.0.0.1:0".to_string(),
            connection_limit: None,
            hard_connection_limit: None,
            tls: None,
            timeout: None,
            chain: TransformChainConfig(chain),
            transport: None,
        })]
    }

    async fn run_test_topology_valkey(
        chain: Vec<Box<dyn TransformConfig>>,
    ) -> anyhow::Result<Vec<Source>> {
        let sources = create_source_from_chain_valkey(chain);

        let topology = Topology { sources };

        let (_sender, trigger_shutdown_rx) = watch::channel::<bool>(false);

        topology.run_chains(trigger_shutdown_rx, None).await
    }

    async fn run_test_topology_cassandra(
        chain: Vec<Box<dyn TransformConfig>>,
    ) -> anyhow::Result<Vec<Source>> {
        let sources = create_source_from_chain_cassandra(chain);

        let topology = Topology { sources };

        let (_sender, trigger_shutdown_rx) = watch::channel::<bool>(false);

        topology.run_chains(trigger_shutdown_rx, None).await
    }

    #[tokio::test]
    async fn test_validate_chain_empty_chain() {
        let expected = r#"Topology errors
foo source:
  foo chain:
    Chain cannot be empty
"#;

        let error = run_test_topology_valkey(vec![])
            .await
            .unwrap_err()
            .to_string();
        assert_eq!(error, expected);
    }

    #[tokio::test]
    async fn test_validate_chain_valid_chain() {
        run_test_topology_valkey(vec![Box::new(DebugPrinterConfig), Box::new(NullSinkConfig)])
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_validate_coalesce() {
        let expected = r#"Topology errors
foo source:
  foo chain:
    Coalesce:
      Need to provide at least one of these fields:
      * flush_when_buffered_message_count
      * flush_when_millis_since_last_flush
    
      But none of them were provided.
      Check https://shotover.io/docs/latest/transforms.html#coalesce for more information.
"#;

        let error = run_test_topology_valkey(vec![
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
foo source:
  foo chain:
    Terminating transform "NullSink" is not last in chain. Terminating transform must be last in chain.
"#;

        let error = run_test_topology_valkey(vec![
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
foo source:
  foo chain:
    Non-terminating transform "DebugPrinter" is last in chain. Last transform must be terminating.
"#;

        let error = run_test_topology_valkey(vec![
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
foo source:
  foo chain:
    Terminating transform "NullSink" is not last in chain. Terminating transform must be last in chain.
    Non-terminating transform "DebugPrinter" is last in chain. Last transform must be terminating.
"#;

        let error = run_test_topology_valkey(vec![
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
    async fn test_validate_chain_valid_subchain_cassandra_valkey_cache() {
        let caching_schema = HashMap::new();

        run_test_topology_cassandra(vec![
            Box::new(DebugPrinterConfig),
            Box::new(DebugPrinterConfig),
            Box::new(ValkeyCacheConfig {
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
    async fn test_validate_chain_invalid_subchain_cassandra_valkey_cache() {
        let expected = r#"Topology errors
foo source:
  foo chain:
    ValkeyCache:
      cache_chain chain:
        Terminating transform "NullSink" is not last in chain. Terminating transform must be last in chain.
"#;

        let error = run_test_topology_cassandra(vec![
            Box::new(DebugPrinterConfig),
            Box::new(DebugPrinterConfig),
            Box::new(ValkeyCacheConfig {
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
        run_test_topology_valkey(vec![
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
foo source:
  foo chain:
    ParallelMap:
      parallel_map_chain chain:
        Terminating transform "NullSink" is not last in chain. Terminating transform must be last in chain.
"#;

        let error = run_test_topology_valkey(vec![
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
foo source:
  foo chain:
    ParallelMap:
      parallel_map_chain chain:
        Terminating transform "NullSink" is not last in chain. Terminating transform must be last in chain.
"#;

        let subchain = TransformChainConfig(vec![
            Box::new(DebugPrinterConfig),
            Box::new(NullSinkConfig),
            Box::new(DebugPrinterConfig),
            Box::new(NullSinkConfig),
        ]);

        let error = run_test_topology_valkey(vec![
            Box::new(DebugPrinterConfig),
            Box::new(DebugPrinterConfig),
            Box::new(ParallelMapConfig {
                parallelism: 1,
                chain: subchain,
                ordered_results: true,
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
foo source:
  foo chain:
    ParallelMap:
      parallel_map_chain chain:
        Non-terminating transform "DebugPrinter" is last in chain. Last transform must be terminating.
"#;

        let subchain = TransformChainConfig(vec![
            Box::new(DebugPrinterConfig),
            Box::new(DebugPrinterConfig),
        ]);

        let error = run_test_topology_valkey(vec![
            Box::new(DebugPrinterConfig),
            Box::new(DebugPrinterConfig),
            Box::new(ParallelMapConfig {
                parallelism: 1,
                chain: subchain,
                ordered_results: true,
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
foo source:
  foo chain:
    ParallelMap:
      parallel_map_chain chain:
        Terminating transform "NullSink" is not last in chain. Terminating transform must be last in chain.
        Non-terminating transform "DebugPrinter" is last in chain. Last transform must be terminating.
"#;

        let subchain = TransformChainConfig(vec![
            Box::new(DebugPrinterConfig),
            Box::new(NullSinkConfig),
            Box::new(DebugPrinterConfig),
        ]);

        let error = run_test_topology_valkey(vec![
            Box::new(DebugPrinterConfig),
            Box::new(DebugPrinterConfig),
            Box::new(ParallelMapConfig {
                parallelism: 1,
                chain: subchain,
                ordered_results: true,
            }),
        ])
        .await
        .unwrap_err()
        .to_string();

        assert_eq!(error, expected);
    }

    #[tokio::test]
    async fn test_validate_repeated_source_names() {
        let expected = r#"Topology errors
Source name "foo" occurred more than once. Make sure all source names are unique. The names will be used in logging and metrics.
"#;

        let mut sources = create_source_from_chain_valkey(vec![Box::new(NullSinkConfig)]);
        sources.extend(create_source_from_chain_valkey(vec![Box::new(
            NullSinkConfig,
        )]));

        let topology = Topology { sources };
        let (_sender, trigger_shutdown_rx) = watch::channel::<bool>(false);
        let error = topology
            .run_chains(trigger_shutdown_rx, None)
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
            .run_chains(trigger_shutdown_rx, None)
            .await
            .unwrap_err()
            .to_string();

        let expected = r#"Topology errors
valkey1 source:
  valkey1 chain:
    Terminating transform "NullSink" is not last in chain. Terminating transform must be last in chain.
    Terminating transform "NullSink" is not last in chain. Terminating transform must be last in chain.
    Non-terminating transform "DebugPrinter" is last in chain. Last transform must be terminating.
valkey2 source:
  valkey2 chain:
    ParallelMap:
      parallel_map_chain chain:
        Terminating transform "NullSink" is not last in chain. Terminating transform must be last in chain.
        Non-terminating transform "DebugPrinter" is last in chain. Last transform must be terminating.
"#;

        assert_eq!(error, expected);
    }
}
