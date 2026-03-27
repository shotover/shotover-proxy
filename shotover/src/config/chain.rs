use crate::transforms::chain::TransformChainBuilder;
use crate::transforms::{
    DownChainProtocol, TransformBuilder, TransformConfig, TransformContextConfig, UpChainProtocol,
};
use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct TransformChainConfig(
    #[serde(rename = "TransformChain")] pub Vec<Box<dyn TransformConfig>>,
);

impl TransformChainConfig {
    pub async fn get_builder(
        &self,
        mut transform_context: TransformContextConfig,
    ) -> Result<TransformChainBuilder> {
        let mut builders: Vec<Box<dyn TransformBuilder>> = Vec::new();
        let mut upchain_protocol = transform_context.up_chain_protocol;
        for config in &self.0 {
            let type_name = config.typetag_name();
            match config.up_chain_protocol() {
                UpChainProtocol::MustBeOneOf(protocols) => {
                    if !protocols.contains(&upchain_protocol) {
                        return Err(anyhow!(
                            "Transform {type_name} requires upchain protocol to be one of {protocols:?} but was {upchain_protocol:?}"
                        ));
                    }
                }
                UpChainProtocol::Any => {
                    // anything is fine
                }
            }
            transform_context.up_chain_protocol = upchain_protocol;
            let builder = config.get_builder(transform_context.clone()).await?;
            builders.push(builder);

            upchain_protocol = match config.down_chain_protocol() {
                DownChainProtocol::TransformedTo(new) => new,
                DownChainProtocol::SameAsUpChain => upchain_protocol,
                DownChainProtocol::Terminating => {
                    // TODO: Move bad sink reporting to here
                    upchain_protocol
                }
            }
        }
        Ok(TransformChainBuilder::new(
            builders,
            transform_context.chain_name.leak(),
        ))
    }
}
