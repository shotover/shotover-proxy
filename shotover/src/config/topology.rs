use crate::sources::{Source, SourceConfig};
use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::fmt::Write;
use tokio::net::TcpListener;
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
        mut hot_reload_listeners: HashMap<u16, TcpListener>,
    ) -> Result<Vec<Source>> {
        let mut sources: Vec<Source> = Vec::new();

        let mut topology_errors = String::new();

        #[derive(Default)]
        struct NameValidationState {
            source_uses: BTreeMap<String, Vec<String>>,
            transform_uses: BTreeMap<String, Vec<String>>,
            chain_uses: BTreeMap<String, Vec<String>>,
        }

        impl NameValidationState {
            fn register_source(&mut self, name: &str, usage: String) {
                self.source_uses
                    .entry(name.to_string())
                    .or_default()
                    .push(usage);
            }

            fn register_transform(&mut self, name: &str, usage: String) {
                self.transform_uses
                    .entry(name.to_string())
                    .or_default()
                    .push(usage);
            }

            fn register_chain(&mut self, name: &str, usage: String) {
                self.chain_uses
                    .entry(name.to_string())
                    .or_default()
                    .push(usage);
            }

            fn duplicate_names(map: &BTreeMap<String, Vec<String>>) -> Vec<(String, Vec<String>)> {
                map.iter()
                    .filter(|(_, uses)| uses.len() > 1)
                    .map(|(name, uses)| (name.clone(), uses.clone()))
                    .collect()
            }
        }

        // Validate name uniqueness across sources, transforms, and chains in a single traversal.
        let mut name_state = NameValidationState::default();

        fn collect_chain_names(
            state: &mut NameValidationState,
            chain: &crate::config::chain::TransformChainConfig,
            chain_path: &str,
        ) {
            for (transform_index, config) in chain.0.iter().enumerate() {
                let transform_name = config.get_name();
                let transform_type = config.typetag_name();
                state.register_transform(
                    transform_name,
                    format!(
                        "transform[{transform_index}] {transform_name:?} ({transform_type}) in {chain_path}"
                    ),
                );

                for (sub_chain, sub_chain_name) in config.get_sub_chain_configs() {
                    let sub_chain_path = format!(
                        "{chain_path} -> subchain {sub_chain_name:?} (from {transform_type} {transform_name:?})"
                    );
                    state.register_chain(
                        &sub_chain_name,
                        format!("chain {sub_chain_name:?} at {sub_chain_path}"),
                    );
                    collect_chain_names(state, sub_chain, &sub_chain_path);
                }
            }
        }

        for (index, source) in self.sources.iter().enumerate() {
            let source_name = source.get_name();
            name_state.register_source(source_name, format!("source[{index}] {source_name:?}"));
            name_state.register_chain(
                source_name,
                format!("root chain for source[{index}] {source_name:?}"),
            );
            let root_chain_path = format!("source[{index}] {source_name:?} chain {source_name:?}");
            collect_chain_names(&mut name_state, source.get_chain_config(), &root_chain_path);
        }

        let duplicate_sources = NameValidationState::duplicate_names(&name_state.source_uses);
        if !duplicate_sources.is_empty() {
            writeln!(topology_errors, "Duplicate source names detected:")?;
            for (name, usages) in duplicate_sources {
                writeln!(topology_errors, "  {name:?} used by:")?;
                for usage in usages {
                    writeln!(topology_errors, "    {usage}")?;
                }
            }
        }

        let duplicate_transforms = NameValidationState::duplicate_names(&name_state.transform_uses);
        if !duplicate_transforms.is_empty() {
            writeln!(topology_errors, "Duplicate transform names detected:")?;
            for (name, usages) in duplicate_transforms {
                writeln!(topology_errors, "  {name:?} used by:")?;
                for usage in usages {
                    writeln!(topology_errors, "    {usage}")?;
                }
            }
        }

        let duplicate_chains = NameValidationState::duplicate_names(&name_state.chain_uses);
        if !duplicate_chains.is_empty() {
            writeln!(topology_errors, "Duplicate chain names detected:")?;
            for (name, usages) in duplicate_chains {
                writeln!(topology_errors, "  {name:?} used by:")?;
                for usage in usages {
                    writeln!(topology_errors, "    {usage}")?;
                }
            }
        }

        for source in &self.sources {
            match source
                .build(trigger_shutdown_rx.clone(), &mut hot_reload_listeners)
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
    use crate::sources::cassandra::CassandraSourceConfig;
    use crate::transforms::TransformConfig;
    use crate::transforms::coalesce::CoalesceConfig;
    use crate::transforms::debug::printer::DebugPrinterConfig;
    use crate::transforms::null::NullSinkConfig;
    use crate::{
        sources::{Source, SourceConfig, valkey::ValkeySourceConfig},
        transforms::{
            parallel_map::ParallelMapConfig, tee::ConsistencyBehaviorConfig, tee::TeeConfig,
            valkey::cache::ValkeyConfig as ValkeyCacheConfig,
        },
    };
    use pretty_assertions::assert_eq;
    use std::collections::HashMap;
    use tokio::sync::watch;

    fn create_source_from_chain_valkey(
        transforms: Vec<Box<dyn TransformConfig>>,
    ) -> Vec<SourceConfig> {
        vec![SourceConfig::Valkey(ValkeySourceConfig {
            name: "foo".to_string(),
            listen_addr: "127.0.0.1:0".to_string(),
            connection_limit: None,
            hard_connection_limit: None,
            tls: None,
            timeout: None,
            chain: TransformChainConfig(transforms),
        })]
    }

    fn create_source_from_chain_cassandra(
        transforms: Vec<Box<dyn TransformConfig>>,
    ) -> Vec<SourceConfig> {
        vec![SourceConfig::Cassandra(CassandraSourceConfig {
            name: "foo".to_string(),
            listen_addr: "127.0.0.1:0".to_string(),
            connection_limit: None,
            hard_connection_limit: None,
            tls: None,
            timeout: None,
            chain: TransformChainConfig(transforms),
            transport: None,
        })]
    }

    async fn run_test_topology_valkey(
        transforms: Vec<Box<dyn TransformConfig>>,
    ) -> anyhow::Result<Vec<Source>> {
        let sources = create_source_from_chain_valkey(transforms);

        let topology = Topology { sources };

        let (_sender, trigger_shutdown_rx) = watch::channel::<bool>(false);

        topology
            .run_chains(trigger_shutdown_rx, HashMap::new())
            .await
    }

    async fn run_test_topology_cassandra(
        transforms: Vec<Box<dyn TransformConfig>>,
    ) -> anyhow::Result<Vec<Source>> {
        let sources = create_source_from_chain_cassandra(transforms);

        let topology = Topology { sources };

        let (_sender, trigger_shutdown_rx) = watch::channel::<bool>(false);

        topology
            .run_chains(trigger_shutdown_rx, HashMap::new())
            .await
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
        run_test_topology_valkey(vec![
            Box::new(DebugPrinterConfig {
                name: "debug".to_string(),
            }),
            Box::new(NullSinkConfig {
                name: "sink".to_string(),
            }),
        ])
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
                name: "coalesce".to_string(),
                flush_when_buffered_message_count: None,
                flush_when_millis_since_last_flush: None,
            }),
            Box::new(NullSinkConfig {
                name: "sink".to_string(),
            }),
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
    Terminating transform "sink-1" is not last in chain. Terminating transform must be last in chain.
"#;

        let error = run_test_topology_valkey(vec![
            Box::new(DebugPrinterConfig {
                name: "debug".to_string(),
            }),
            Box::new(NullSinkConfig {
                name: "sink-1".to_string(),
            }),
            Box::new(NullSinkConfig {
                name: "sink-2".to_string(),
            }),
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
    Non-terminating transform "debug-3" is last in chain. Last transform must be terminating.
"#;

        let error = run_test_topology_valkey(vec![
            Box::new(DebugPrinterConfig {
                name: "debug-1".to_string(),
            }),
            Box::new(DebugPrinterConfig {
                name: "debug-2".to_string(),
            }),
            Box::new(DebugPrinterConfig {
                name: "debug-3".to_string(),
            }),
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
    Terminating transform "sink" is not last in chain. Terminating transform must be last in chain.
    Non-terminating transform "debug-3" is last in chain. Last transform must be terminating.
"#;

        let error = run_test_topology_valkey(vec![
            Box::new(DebugPrinterConfig {
                name: "debug-1".to_string(),
            }),
            Box::new(DebugPrinterConfig {
                name: "debug-2".to_string(),
            }),
            Box::new(NullSinkConfig {
                name: "sink".to_string(),
            }),
            Box::new(DebugPrinterConfig {
                name: "debug-3".to_string(),
            }),
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
            Box::new(DebugPrinterConfig {
                name: "debug-1".to_string(),
            }),
            Box::new(DebugPrinterConfig {
                name: "debug-2".to_string(),
            }),
            Box::new(ValkeyCacheConfig {
                name: "cache".to_string(),
                chain: TransformChainConfig(vec![
                    Box::new(DebugPrinterConfig {
                        name: "c-debug-1".to_string(),
                    }),
                    Box::new(DebugPrinterConfig {
                        name: "c-debug-2".to_string(),
                    }),
                    Box::new(NullSinkConfig {
                        name: "c-sink".to_string(),
                    }),
                ]),
                caching_schema,
            }),
            Box::new(NullSinkConfig {
                name: "sink".to_string(),
            }),
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
      cache chain:
        Terminating transform "c-sink-1" is not last in chain. Terminating transform must be last in chain.
"#;

        let error = run_test_topology_cassandra(vec![
            Box::new(DebugPrinterConfig {
                name: "debug-1".to_string(),
            }),
            Box::new(DebugPrinterConfig {
                name: "debug-2".to_string(),
            }),
            Box::new(ValkeyCacheConfig {
                name: "cache".to_string(),
                chain: TransformChainConfig(vec![
                    Box::new(DebugPrinterConfig {
                        name: "c-debug".to_string(),
                    }),
                    Box::new(NullSinkConfig {
                        name: "c-sink-1".to_string(),
                    }),
                    Box::new(DebugPrinterConfig {
                        name: "c-debug-2".to_string(),
                    }),
                    Box::new(NullSinkConfig {
                        name: "c-sink-2".to_string(),
                    }),
                ]),
                caching_schema: HashMap::new(),
            }),
            Box::new(NullSinkConfig {
                name: "sink".to_string(),
            }),
        ])
        .await
        .unwrap_err()
        .to_string();

        assert_eq!(error, expected);
    }

    #[tokio::test]
    async fn test_validate_chain_valid_subchain_parallel_map() {
        run_test_topology_valkey(vec![
            Box::new(DebugPrinterConfig {
                name: "debug-1".to_string(),
            }),
            Box::new(DebugPrinterConfig {
                name: "debug-2".to_string(),
            }),
            Box::new(ParallelMapConfig {
                name: "pmap".to_string(),
                parallelism: 1,
                chain: TransformChainConfig(vec![
                    Box::new(DebugPrinterConfig {
                        name: "p-debug-1".to_string(),
                    }),
                    Box::new(DebugPrinterConfig {
                        name: "p-debug-2".to_string(),
                    }),
                    Box::new(NullSinkConfig {
                        name: "p-sink".to_string(),
                    }),
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
      pmap[0] chain:
        Terminating transform "p-sink-1" is not last in chain. Terminating transform must be last in chain.
"#;

        let error = run_test_topology_valkey(vec![
            Box::new(DebugPrinterConfig {
                name: "debug-1".to_string(),
            }),
            Box::new(DebugPrinterConfig {
                name: "debug-2".to_string(),
            }),
            Box::new(ParallelMapConfig {
                name: "pmap".to_string(),
                parallelism: 1,
                chain: TransformChainConfig(vec![
                    Box::new(DebugPrinterConfig {
                        name: "p-debug".to_string(),
                    }),
                    Box::new(NullSinkConfig {
                        name: "p-sink-1".to_string(),
                    }),
                    Box::new(DebugPrinterConfig {
                        name: "p-debug-2".to_string(),
                    }),
                    Box::new(NullSinkConfig {
                        name: "p-sink-2".to_string(),
                    }),
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
      pmap[0] chain:
        Terminating transform "p-sink-1" is not last in chain. Terminating transform must be last in chain.
"#;

        let subchain = TransformChainConfig(vec![
            Box::new(DebugPrinterConfig {
                name: "p-debug".to_string(),
            }),
            Box::new(NullSinkConfig {
                name: "p-sink-1".to_string(),
            }),
            Box::new(DebugPrinterConfig {
                name: "p-debug-2".to_string(),
            }),
            Box::new(NullSinkConfig {
                name: "p-sink-2".to_string(),
            }),
        ]);

        let error = run_test_topology_valkey(vec![
            Box::new(DebugPrinterConfig {
                name: "debug-1".to_string(),
            }),
            Box::new(DebugPrinterConfig {
                name: "debug-2".to_string(),
            }),
            Box::new(ParallelMapConfig {
                name: "pmap".to_string(),
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
      pmap[0] chain:
        Non-terminating transform "p-debug-2" is last in chain. Last transform must be terminating.
"#;

        let subchain = TransformChainConfig(vec![
            Box::new(DebugPrinterConfig {
                name: "p-debug-1".to_string(),
            }),
            Box::new(DebugPrinterConfig {
                name: "p-debug-2".to_string(),
            }),
        ]);

        let error = run_test_topology_valkey(vec![
            Box::new(DebugPrinterConfig {
                name: "debug-1".to_string(),
            }),
            Box::new(DebugPrinterConfig {
                name: "debug-2".to_string(),
            }),
            Box::new(ParallelMapConfig {
                name: "pmap".to_string(),
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
      pmap[0] chain:
        Terminating transform "p-sink" is not last in chain. Terminating transform must be last in chain.
        Non-terminating transform "p-debug-2" is last in chain. Last transform must be terminating.
"#;

        let subchain = TransformChainConfig(vec![
            Box::new(DebugPrinterConfig {
                name: "p-debug-1".to_string(),
            }),
            Box::new(NullSinkConfig {
                name: "p-sink".to_string(),
            }),
            Box::new(DebugPrinterConfig {
                name: "p-debug-2".to_string(),
            }),
        ]);

        let error = run_test_topology_valkey(vec![
            Box::new(DebugPrinterConfig {
                name: "debug-1".to_string(),
            }),
            Box::new(DebugPrinterConfig {
                name: "debug-2".to_string(),
            }),
            Box::new(ParallelMapConfig {
                name: "pmap".to_string(),
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
Duplicate source names detected:
  "foo" used by:
    source[0] "foo"
    source[1] "foo"
Duplicate chain names detected:
  "foo" used by:
    root chain for source[0] "foo"
    root chain for source[1] "foo"
"#;

        let mut sources = create_source_from_chain_valkey(vec![Box::new(NullSinkConfig {
            name: "sink".to_string(),
        })]);
        sources.extend(create_source_from_chain_valkey(vec![Box::new(
            NullSinkConfig {
                name: "sink1".to_string(),
            },
        )]));

        let topology = Topology { sources };
        let (_sender, trigger_shutdown_rx) = watch::channel::<bool>(false);
        let error = topology
            .run_chains(trigger_shutdown_rx, HashMap::new())
            .await
            .unwrap_err()
            .to_string();

        assert_eq!(error, expected);
    }

    #[tokio::test]
    async fn test_validate_repeated_transform_names() {
        let expected = r#"Topology errors
Duplicate transform names detected:
  "dup" used by:
    transform[0] "dup" (DebugPrinter) in source[0] "foo" chain "foo"
    transform[1] "dup" (NullSink) in source[0] "foo" chain "foo"
"#;

        let error = run_test_topology_valkey(vec![
            Box::new(DebugPrinterConfig {
                name: "dup".to_string(),
            }),
            Box::new(NullSinkConfig {
                name: "dup".to_string(),
            }),
        ])
        .await
        .unwrap_err()
        .to_string();

        assert_eq!(error, expected);
    }

    #[tokio::test]
    async fn test_validate_duplicate_chain_names_user_defined() {
        let expected = r#"Topology errors
Duplicate chain names detected:
  "foo" used by:
    root chain for source[0] "foo"
    chain "foo" at source[0] "foo" chain "foo" -> subchain "foo" (from Tee "tee-main")
"#;

        let error = run_test_topology_valkey(vec![
            Box::new(TeeConfig {
                name: "tee-main".to_string(),
                behavior: Some(ConsistencyBehaviorConfig::SubchainOnMismatch {
                    name: "foo".to_string(),
                    chain: TransformChainConfig(vec![Box::new(NullSinkConfig {
                        name: "mismatch-sink".to_string(),
                    })]),
                }),
                timeout_micros: None,
                chain: TransformChainConfig(vec![Box::new(NullSinkConfig {
                    name: "tee-sink".to_string(),
                })]),
                buffer_size: None,
                switch_port: None,
            }),
            Box::new(NullSinkConfig {
                name: "sink".to_string(),
            }),
        ])
        .await
        .unwrap_err()
        .to_string();

        assert_eq!(error, expected);
    }

    #[tokio::test]
    async fn test_validate_duplicate_chain_names_auto_derived() {
        let expected = r#"Topology errors
Duplicate chain names detected:
  "foo" used by:
    root chain for source[0] "foo"
    chain "foo" at source[0] "foo" chain "foo" -> subchain "foo" (from ValkeyCache "foo")
"#;

        let error = run_test_topology_cassandra(vec![
            Box::new(ValkeyCacheConfig {
                name: "foo".to_string(),
                chain: TransformChainConfig(vec![Box::new(NullSinkConfig {
                    name: "cache-sink".to_string(),
                })]),
                caching_schema: HashMap::new(),
            }),
            Box::new(NullSinkConfig {
                name: "sink".to_string(),
            }),
        ])
        .await
        .unwrap_err()
        .to_string();

        assert_eq!(error, expected);
    }

    #[tokio::test]
    async fn test_validate_duplicate_chain_names_user_defined_across_transforms() {
        let expected = r#"Topology errors
Duplicate chain names detected:
  "mismatch" used by:
    chain "mismatch" at source[0] "foo" chain "foo" -> subchain "mismatch" (from Tee "tee-a")
    chain "mismatch" at source[0] "foo" chain "foo" -> subchain "mismatch" (from Tee "tee-b")
"#;

        let error = run_test_topology_valkey(vec![
            Box::new(TeeConfig {
                name: "tee-a".to_string(),
                behavior: Some(ConsistencyBehaviorConfig::SubchainOnMismatch {
                    name: "mismatch".to_string(),
                    chain: TransformChainConfig(vec![Box::new(NullSinkConfig {
                        name: "mismatch-sink-a".to_string(),
                    })]),
                }),
                timeout_micros: None,
                chain: TransformChainConfig(vec![Box::new(NullSinkConfig {
                    name: "tee-a-sink".to_string(),
                })]),
                buffer_size: None,
                switch_port: None,
            }),
            Box::new(TeeConfig {
                name: "tee-b".to_string(),
                behavior: Some(ConsistencyBehaviorConfig::SubchainOnMismatch {
                    name: "mismatch".to_string(),
                    chain: TransformChainConfig(vec![Box::new(NullSinkConfig {
                        name: "mismatch-sink-b".to_string(),
                    })]),
                }),
                timeout_micros: None,
                chain: TransformChainConfig(vec![Box::new(NullSinkConfig {
                    name: "tee-b-sink".to_string(),
                })]),
                buffer_size: None,
                switch_port: None,
            }),
            Box::new(NullSinkConfig {
                name: "sink".to_string(),
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
            .run_chains(trigger_shutdown_rx, HashMap::new())
            .await
            .unwrap_err()
            .to_string();

        let expected = r#"Topology errors
valkey1 source:
  valkey1 chain:
    Terminating transform "sink-1" is not last in chain. Terminating transform must be last in chain.
    Terminating transform "sink-2" is not last in chain. Terminating transform must be last in chain.
    Non-terminating transform "debug" is last in chain. Last transform must be terminating.
valkey2 source:
  valkey2 chain:
    ParallelMap:
      pmap[0] chain:
        Terminating transform "p-sink" is not last in chain. Terminating transform must be last in chain.
        Non-terminating transform "p-debug" is last in chain. Last transform must be terminating.
"#;

        assert_eq!(error, expected);
    }
}
