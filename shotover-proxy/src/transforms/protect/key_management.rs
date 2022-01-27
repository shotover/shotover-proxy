use crate::transforms::protect::aws_kms::AWSKeyManagement;
use crate::transforms::protect::local_kek::LocalKeyManagement;
use crate::transforms::protect::KeyMaterial;
use anyhow::Result;
use async_trait::async_trait;
use cached::proc_macro::cached;
use rusoto_kms::KmsClient;
use rusoto_signature::Region;
use serde::Deserialize;
use sodiumoxide::crypto::secretbox::Key;
use std::collections::HashMap;
use std::str::FromStr;

#[async_trait]
pub trait KeyManagement {
    async fn get_key(&self, dek: Option<Vec<u8>>, kek_alt: Option<String>) -> Result<KeyMaterial>;
}

#[derive(Clone)]
pub enum KeyManager {
    AWSKms(AWSKeyManagement),
    Local(LocalKeyManagement),
}

#[derive(Deserialize, Debug, Clone)]
pub enum KeyManagerConfig {
    AWSKms {
        region: String,
        cmk_id: String,
        encryption_context: Option<HashMap<String, String>>,
        key_spec: Option<String>,
        number_of_bytes: Option<i64>,
        grant_tokens: Option<Vec<String>>,
        endpoint: Option<String>,
    },
    Local {
        kek: Key,
        kek_id: String,
    },
}

impl KeyManagerConfig {
    pub fn build(&self) -> Result<KeyManager> {
        match self.clone() {
            KeyManagerConfig::AWSKms {
                region,
                cmk_id,
                encryption_context,
                key_spec,
                number_of_bytes,
                grant_tokens,
                endpoint,
            } => Ok(KeyManager::AWSKms(AWSKeyManagement {
                client: {
                    let region_obj = match endpoint {
                        Some(x) => Region::Custom {
                            name: Region::from_str(&region)?.name().to_string(),
                            endpoint: x,
                        },
                        _ => Region::from_str(&region)?,
                    };
                    KmsClient::new(region_obj)
                },
                cmk_id,
                encryption_context,
                key_spec,
                number_of_bytes,
                grant_tokens,
            })),
            KeyManagerConfig::Local { kek, kek_id } => {
                Ok(KeyManager::Local(LocalKeyManagement { kek, kek_id }))
            }
        }
    }
}

#[async_trait]
impl KeyManagement for KeyManager {
    async fn get_key(&self, dek: Option<Vec<u8>>, kek_alt: Option<String>) -> Result<KeyMaterial> {
        match &self {
            KeyManager::AWSKms(aws) => aws.get_aws_key(dek, kek_alt).await,
            KeyManager::Local(local) => local.get_key(dek).await,
        }
    }
}

impl KeyManager {
    pub async fn cached_get_key(
        &self,
        _key_id: String,
        dek: Option<Vec<u8>>,
        kek_alt: Option<String>,
    ) -> Result<KeyMaterial> {
        private_cached_fetch(_key_id, self, dek, kek_alt).await
    }
}

// We don't cache fetch key directly to make testing key fetching easier with caching getting in the way

#[cached(
    result = true,
    key = "String",
    convert = r#"{ format!("{}", _key_id) }"#
)]
async fn private_cached_fetch(
    _key_id: String,
    km: &KeyManager,
    dek: Option<Vec<u8>>,
    kek_alt: Option<String>,
) -> Result<KeyMaterial> {
    km.get_key(dek, kek_alt).await
}
