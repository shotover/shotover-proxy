use anyhow::{Context, Result};
use serde::Deserialize;

pub mod chain;
pub mod topology;

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    pub main_log_level: String,
    pub observability_interface: String,
}

impl Config {
    pub fn from_file(filepath: String) -> Result<Config> {
        let file = std::fs::File::open(&filepath)
            .with_context(|| format!("Couldn't open the config file {}", &filepath))?;
        serde_yaml::from_reader(file)
            .with_context(|| format!("Failed to parse config file {}", &filepath))
    }
}
