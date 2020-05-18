use crate::transforms::TransformsConfig;
use std::collections::HashMap;
use crate::sources::SourcesConfig;
use serde::{Serialize, Deserialize};
use crate::sources::cassandra_source::CassandraConfig;


#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Topology {
    sources: HashMap<String, SourcesConfig>,
    transforms: HashMap<String, TransformsConfig>,
    chains: HashMap<String, Vec<String>>
}

impl Topology {
    fn new_from_yaml(yaml_contents: String) -> Result<Topology, serde_yaml::Error> {
        serde_yaml::from_str(&yaml_contents)
    }
}

#[cfg(test)]
mod topology_tests {
    use crate::config::topology::Topology;

    const TEST_STRING: &str = "
---
sources:
  cassandra_prod:
    Cassandra:
      listen_addr: \"127.0.0.1:9043\"
      cassandra_ks:
        \"test.simple\": [\"key\"]
        \"test.clustering\": [\"pk\"]
        \"system.local\": [\"pk\"]
transforms:
  cassandra_prod_destination:
    CodecDestination:
      remote_address: \"127.0.0.1:9042\"
chains:
  cassandra_proxy: [\"cassandra_prod\", \"cassandra_prod_destination\"]
";

    #[test]
    fn test_config_parse_format() -> Result<(), serde_yaml::Error> {
        let _ = Topology::new_from_yaml(String::from(TEST_STRING))?;
        Ok(())
    }
}