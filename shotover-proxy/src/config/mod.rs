use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};

pub mod topology;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Config {
    pub main_log_level: String,
    pub observability_interface: String,
}

impl Config {
    pub fn from_file(filepath: String) -> Result<Config> {
        match std::fs::File::open(filepath.clone()) {
            Ok(f) => serde_yaml::from_reader(f).map_err(|x| x.into()),
            Err(err) => Err(anyhow!("Couldn't open the config file {}: {}", &filepath, err))
        }
    }
}
