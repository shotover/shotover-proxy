use std::collections::HashMap;
use crate::transforms::chain::{TransformChain, Wrapper, Transform, ChainResponse, RequestError};
use async_trait::async_trait;
use serde::{Serialize, Deserialize};
use crate::transforms::{Transforms, TransformsConfig, build_chain_from_config, TransformsFromConfig};
use crate::config::ConfigError;
use crate::runtimes::rhai::RhaiEnvironment;
use crate::config::topology::TopicHolder;

#[derive(Clone)]
pub struct RhaiTransform {
    name: &'static str,
    function_env: RhaiEnvironment,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct RhaiConfig {
    #[serde(rename = "config_values")]
    pub rhai_script: String
}

#[async_trait]
impl TransformsFromConfig for RhaiConfig {
    async fn get_source(&self, topics: &TopicHolder) -> Result<Transforms, ConfigError> {
        Ok(Transforms::Rhai(RhaiTransform {
            name: "rhai",
            function_env: RhaiEnvironment::new(&self.rhai_script)?,
        }))
    }
}


#[async_trait]
impl Transform for RhaiTransform {
    async fn transform(&self, mut qd: Wrapper, t: &TransformChain) -> ChainResponse {
        let mut qd_mod = self.function_env.call_rhai_transform_func(qd.clone()).await?;
        let response = self.call_next_transform(qd_mod, t).await;
        let result = self.function_env.call_rhai_transform_response_func(response).await?;
        return result;
    }

    fn get_name(&self) -> &'static str {
        self.name
    }
}

