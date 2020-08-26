use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};

pub mod topology;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Config {
    pub main_log_level: String,
    pub prometheus_interface: String,
}

impl Config {
    pub fn from_file(filepath: String) -> Result<Config> {
        if let Ok(f) = std::fs::File::open(filepath.clone()) {
            let config: Config = serde_yaml::from_reader(f)?;
            return Ok(config);
        }
        //TODO: Make Config errors implement the From trait for IO errors
        Err(anyhow!("Couldn't open the file {}", &filepath))
    }
}
