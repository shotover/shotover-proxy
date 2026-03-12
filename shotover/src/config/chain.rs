use crate::transforms::chain::TransformChainBuilder;
use crate::transforms::{
    DownChainProtocol, TransformBuilder, TransformConfig, TransformContextConfig, UpChainProtocol,
};
use anyhow::{Result, anyhow};
use serde::de::{DeserializeSeed, Deserializer, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::fmt::{self, Debug};
use std::iter;

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct TransformChainConfig(
    #[serde(rename = "TransformChain", deserialize_with = "vec_transform_config")]
    pub  Vec<Box<dyn TransformConfig>>,
);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChainNameOrigin {
    Auto,
    UserDefined,
}

#[derive(Default)]
pub struct NameValidationState {
    pub transform_names: HashSet<String>,
    pub transform_duplicates: BTreeSet<String>,
    pub chain_names: HashMap<String, ChainNameOrigin>,
    pub chain_duplicates: BTreeSet<String>,
}

impl NameValidationState {
    pub fn register_transform_name(&mut self, name: &str) {
        if !self.transform_names.insert(name.to_string()) {
            self.transform_duplicates.insert(name.to_string());
        }
    }

    pub fn register_chain_name(&mut self, name: &str, origin: ChainNameOrigin) {
        match self.chain_names.get(name) {
            None => {
                self.chain_names.insert(name.to_string(), origin);
            }
            Some(existing_origin) => {
                if matches!(origin, ChainNameOrigin::UserDefined)
                    || matches!(existing_origin, ChainNameOrigin::UserDefined)
                {
                    self.chain_duplicates.insert(name.to_string());
                }

                if matches!(origin, ChainNameOrigin::UserDefined) {
                    self.chain_names
                        .insert(name.to_string(), ChainNameOrigin::UserDefined);
                }
            }
        }
    }
}

impl TransformChainConfig {
    pub async fn get_builder(
        &self,
        mut transform_context: TransformContextConfig,
    ) -> Result<TransformChainBuilder> {
        let mut builders: Vec<(Box<dyn TransformBuilder>, String)> = Vec::new();
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
            let transform_name = config.get_name().to_string();
            builders.push((builder, transform_name));

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

    pub fn collect_names(&self, state: &mut NameValidationState) {
        for config in &self.0 {
            let transform_name = config.get_name();
            state.register_transform_name(transform_name);

            let user_named_subchains = config.get_user_named_sub_chain_names();

            for (sub_chain, sub_chain_name) in config.get_sub_chain_configs() {
                let origin = if user_named_subchains
                    .iter()
                    .any(|name| name == &sub_chain_name)
                {
                    ChainNameOrigin::UserDefined
                } else {
                    ChainNameOrigin::Auto
                };

                state.register_chain_name(&sub_chain_name, origin);
                sub_chain.collect_names(state);
            }
        }
    }
}

/// This function is a custom deserializer that works around a mismatch in the way yaml and typetag represent things,
/// resulting in typetagged structs with no fields failing to deserialize from a single line yaml entry.
/// e.g. with typetag + yaml + the default serializer:
/// this would fail to deserialize:
/// ```yaml
/// Valkey:
///   ...
///   chain:
///     - NullSink
/// ```
///
/// but this would work fine:
/// ```yaml
/// Valkey:
///   ...
///   chain:
///     - NullSink: {}
/// ```
///
/// With the use of this custom deserializer both cases now deserialize correctly.
/// The implementation was a suggestion from dtolnay: https://github.com/dtolnay/typetag/pull/40#issuecomment-1454961686
fn vec_transform_config<'de, D>(deserializer: D) -> Result<Vec<Box<dyn TransformConfig>>, D::Error>
where
    D: Deserializer<'de>,
{
    struct VecTransformConfigVisitor;

    impl<'de> Visitor<'de> for VecTransformConfigVisitor {
        type Value = Vec<Box<dyn TransformConfig>>;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("list of TransformConfig")
        }

        fn visit_seq<S>(self, mut seq: S) -> Result<Self::Value, S::Error>
        where
            S: SeqAccess<'de>,
        {
            let mut vec = Vec::new();
            while let Some(item) = seq.next_element_seed(TransformConfigVisitor)? {
                vec.push(item);
            }
            Ok(vec)
        }
    }

    struct TransformConfigVisitor;

    impl<'de> Visitor<'de> for TransformConfigVisitor {
        type Value = Box<dyn TransformConfig>;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("TransformConfig")
        }

        fn visit_map<M>(self, map: M) -> Result<Self::Value, M::Error>
        where
            M: MapAccess<'de>,
        {
            let de = serde::de::value::MapAccessDeserializer::new(map);
            Deserialize::deserialize(de)
        }

        fn visit_str<E>(self, string: &str) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            let singleton_map = iter::once((string, ()));
            let de = serde::de::value::MapDeserializer::new(singleton_map);
            Deserialize::deserialize(de)
        }
    }

    impl<'de> DeserializeSeed<'de> for TransformConfigVisitor {
        type Value = Box<dyn TransformConfig>;

        fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
        where
            D: Deserializer<'de>,
        {
            deserializer.deserialize_any(self)
        }
    }

    deserializer.deserialize_seq(VecTransformConfigVisitor)
}
