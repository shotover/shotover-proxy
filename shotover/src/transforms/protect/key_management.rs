use crate::transforms::protect::aws_kms::AWSKeyManagement;
use crate::transforms::protect::local_kek::LocalKeyManagement;
use anyhow::{anyhow, Result};
use aws_config::SdkConfig;
use aws_config::{meta::region::RegionProviderChain, BehaviorVersion};
use aws_sdk_kms::config::Region;
use aws_sdk_kms::Client as KmsClient;
use base64::{engine::general_purpose, Engine as _};
use bytes::Bytes;
use cached::proc_macro::cached;
use chacha20poly1305::Key;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Clone, Debug)]
pub enum KeyManager {
    AWSKms(AWSKeyManagement),
    Local(LocalKeyManagement),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub enum KeyManagerConfig {
    AWSKms {
        region: String,
        cmk_id: String,
        encryption_context: Option<HashMap<String, String>>,
        key_spec: Option<String>,
        number_of_bytes: Option<i32>,
        grant_tokens: Option<Vec<String>>,
        endpoint: Option<String>,
    },
    Local {
        kek: String,
        kek_id: String,
    },
}

async fn config(region: String, endpoint: Option<String>) -> SdkConfig {
    let builder = aws_config::defaults(BehaviorVersion::latest())
        .region(RegionProviderChain::first_try(Region::new(region)));
    match endpoint {
        Some(endpoint) => builder.endpoint_url(endpoint).load().await,
        None => builder.load().await,
    }
}

impl KeyManagerConfig {
    pub async fn build(&self) -> Result<KeyManager> {
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
                client: KmsClient::new(&config(region, endpoint).await),
                cmk_id,
                encryption_context,
                key_spec,
                number_of_bytes,
                grant_tokens,
            })),
            KeyManagerConfig::Local { kek, kek_id } => {
                let decoded_base64 = general_purpose::STANDARD.decode(kek)?;

                if decoded_base64.len() != 32 {
                    return Err(anyhow!("Invalid key length"));
                }

                let kek = Key::from_slice(&decoded_base64);
                Ok(KeyManager::Local(LocalKeyManagement { kek: *kek, kek_id }))
            }
        }
    }
}

impl KeyManager {
    async fn get_key(&self, dek: Option<Vec<u8>>, kek_alt: Option<String>) -> Result<KeyMaterial> {
        match &self {
            KeyManager::AWSKms(aws) => aws.get_key(dek, kek_alt).await,
            KeyManager::Local(local) => local.get_key(dek),
        }
    }
}

impl KeyManager {
    pub async fn cached_get_key(
        &self,
        key_id: &str,
        dek: Option<Vec<u8>>,
        kek_alt: Option<String>,
    ) -> Result<KeyMaterial> {
        private_cached_fetch(key_id, self, dek, kek_alt).await
    }
}

// We don't cache fetch key directly to make testing key fetching easier with caching getting in the way

#[cached(
    result = true,
    key = "String",
    convert = r#"{ format!("{}", _key_id) }"#
)]
async fn private_cached_fetch(
    _key_id: &str,
    km: &KeyManager,
    dek: Option<Vec<u8>>,
    kek_alt: Option<String>,
) -> Result<KeyMaterial> {
    km.get_key(dek, kek_alt).await
}

#[derive(Clone)]
pub struct KeyMaterial {
    pub ciphertext_blob: Bytes,
    pub key_id: String,
    pub plaintext: Key,
}

#[cfg(test)]
mod key_manager_tests {
    use super::*;

    #[test]
    fn test_valid_local() {
        let config = KeyManagerConfig::Local {
            kek: "Ht8M1nDO/7fay+cft71M2Xy7j30EnLAsA84hSUMCm1k=".into(),
            kek_id: "".into(),
        };

        let _ = futures::executor::block_on(config.build()).unwrap();
    }

    #[test]
    fn test_invalid_key_length_local() {
        let config = KeyManagerConfig::Local {
            kek: "dGVzdHRlc3R0ZXN0".into(),
            kek_id: "".into(),
        };

        let result = futures::executor::block_on(config.build()).unwrap_err();
        assert_eq!(result.to_string(), "Invalid key length".to_string());
    }

    #[test]
    fn test_invalid_key_base64_local() {
        let config = KeyManagerConfig::Local {
            kek: "Ht8M1nDO/7fay+cft71M2Xy7j30EnLAsA84hSUMCm1k=blahblahblah".into(),
            kek_id: "".into(),
        };

        let result = futures::executor::block_on(config.build()).unwrap_err();
        assert_eq!(
            result.to_string(),
            "Invalid symbol 61, offset 43.".to_string()
        );
    }
}
