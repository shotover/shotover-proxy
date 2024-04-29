use crate::transforms::chain::TransformChainBuilder;
use crate::transforms::{
    DownChainProtocol, TransformBuilder, TransformConfig, TransformContextConfig, UpChainProtocol,
};
use anyhow::{anyhow, Result};
use serde::de::{DeserializeSeed, Deserializer, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Serialize};
use std::fmt::{self, Debug};
use std::iter;

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct TransformChainConfig(
    #[serde(rename = "TransformChain", deserialize_with = "vec_transform_config")]
    pub  Vec<Box<dyn TransformConfig>>,
);

impl TransformChainConfig {
    pub async fn get_builder(
        &self,
        mut transform_context: TransformContextConfig,
    ) -> Result<TransformChainBuilder> {
        let mut transforms: Vec<Box<dyn TransformBuilder>> = Vec::new();
        let mut upchain_protocol = transform_context.protocol;
        for (i, tc) in self.0.iter().enumerate() {
            let name = tc.typetag_name();
            match tc.up_chain_protocol() {
                UpChainProtocol::MustBeOneOf(protocols) => {
                    if !protocols.contains(&upchain_protocol) {
                        return Err(anyhow!("Transform {name} requires upchain protocol to be one of {protocols:?} but was {upchain_protocol:?}"));
                    }
                }
                UpChainProtocol::Any => {
                    // anything is fine
                }
            }
            transform_context.protocol = upchain_protocol;
            transforms.push(tc.get_builder(transform_context.clone()).await?);

            upchain_protocol = match tc.down_chain_protocol() {
                DownChainProtocol::TransformedTo(new) => new,
                DownChainProtocol::SameAsIncoming => upchain_protocol,
                DownChainProtocol::Sink => {
                    if i + 1 != self.0.len() {
                        // TODO: Move bad sink reporting to here
                        upchain_protocol
                    } else {
                        upchain_protocol
                    }
                }
            }
        }
        Ok(TransformChainBuilder::new(
            transforms,
            transform_context.chain_name.leak(),
        ))
    }
}

/// This function is a custom deserializer that works around a mismatch in the way yaml and typetag represent things,
/// resulting in typetagged structs with no fields failing to deserialize from a single line yaml entry.
/// e.g. with typetag + yaml + the default serializer:
/// this would fail to deserialize:
/// ```yaml
/// Redis:
///   ...
///   chain:
///     - NullSink
/// ```
///
/// but this would work fine:
/// ```yaml
/// Redis:
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
