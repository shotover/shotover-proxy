use anyhow::{anyhow, Result};
use serde::Deserialize;

pub mod topology;

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    pub main_log_level: String,
    pub observability_interface: String,
}

impl Config {
    pub fn from_file(filepath: String) -> Result<Config> {
        let file = std::fs::File::open(&filepath).map_err(|err| {
            anyhow!(err).context(format!("Couldn't open the config file {}", &filepath))
        })?;
        Ok(serde_yaml::from_reader(file)?)
    }
}
