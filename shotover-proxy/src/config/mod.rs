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
        let file = std::fs::File::open(&filepath)
            .map_err(|err| anyhow!("Couldn't open the config file {}: {}", &filepath, err))?;
        Ok(serde_yaml::from_reader(file)?)
    }
}
