use rusoto_kms::{KmsClient};
use std::collections::HashMap;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use crate::transforms::protect::KeyMaterial;
use sodiumoxide::crypto::secretbox::Key;
use anyhow::Result;
use cached::proc_macro::cached;
use crate::transforms::protect::aws_kms::AWSKeyManagement;
use crate::transforms::protect::local_kek::LocalKeyManagement;
use rusoto_signature::Region;
use std::str::FromStr;

#[async_trait]
pub trait KeyManagement {
    async fn get_key(&self, dek: Option<Vec<u8>>, kek_alt: Option<String>) -> Result<KeyMaterial>;
}

#[derive(Clone)]
pub enum KeyManager {
    AWS_KMS(AWSKeyManagement),
    LOCAL(LocalKeyManagement)
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub enum KeyManagerConfig {
    AWS_KMS{
        region: String,
        cmk_id: String,
        encryption_context: Option<HashMap<String, String>>,
        key_spec: Option<String>,
        number_of_bytes: Option<i64>,
        grant_tokens: Option<Vec<String>>
    },
    LOCAL {
        kek: Key,
        kek_id: String
    }
}

impl KeyManagerConfig {
    pub fn build(&self) -> Result<KeyManager> {
        return match self.clone() {
            KeyManagerConfig::AWS_KMS {
                region,
                cmk_id,
                encryption_context,
                key_spec,
                number_of_bytes,
                grant_tokens } => {
                    Ok(KeyManager::AWS_KMS(AWSKeyManagement{
                        client: KmsClient::new(Region::from_str(region.as_str())?),
                        cmk_id,
                        encryption_context,
                        key_spec,
                        number_of_bytes,
                        grant_tokens
                    }))
            },
            KeyManagerConfig::LOCAL {
                kek,
                kek_id } => {
                Ok(KeyManager::LOCAL(LocalKeyManagement{
                    kek,
                    kek_id
                }))
            },
        }
    }
}

#[async_trait]
impl KeyManagement for KeyManager {
    async fn get_key(&self, dek: Option<Vec<u8>>, kek_alt: Option<String>) -> Result<KeyMaterial> {
        return match &self {
            KeyManager::AWS_KMS(aws) => aws.get_aws_key(dek, kek_alt).await,
            KeyManager::LOCAL(local) => local.get_key( dek).await,
        }
    }
}

impl KeyManager {
    pub async fn cached_get_key(&self, _key_id: String,dek: Option<Vec<u8>>, kek_alt: Option<String>) -> Result<KeyMaterial> {
        private_cached_fetch(_key_id, self, dek, kek_alt).await
    }
}

// We don't cache fetch key directly to make testing key fetching easier with caching getting in the way

#[cached(
result = true,
key = "String",
convert = r#"{ format!("{}", _key_id) }"#,
)]
async fn private_cached_fetch(_key_id: String, km: &KeyManager, dek: Option<Vec<u8>>, kek_alt: Option<String>) -> Result<KeyMaterial> {
    km.get_key(dek, kek_alt).await
}