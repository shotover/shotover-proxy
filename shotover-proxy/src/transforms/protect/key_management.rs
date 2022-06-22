use crate::transforms::protect::aws_kms::AWSKeyManagement;
use crate::transforms::protect::local_kek::LocalKeyManagement;
use anyhow::{anyhow, Result};
use bytes::Bytes;
use cached::proc_macro::cached;
use chacha20poly1305::Key;
use rusoto_kms::KmsClient;
use rusoto_signature::Region;
use serde::Deserialize;
use std::collections::HashMap;
use std::str::FromStr;

#[derive(Clone, Debug)]
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
        kek: String,
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
                let decoded_base64 = base64::decode(&kek)?;

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
            KeyManager::Local(local) => local.get_key(dek).await,
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

        let _ = config.build().unwrap();
    }

    #[test]
    fn test_invalid_key_length_local() {
        let config = KeyManagerConfig::Local {
            kek: "dGVzdHRlc3R0ZXN0".into(),
            kek_id: "".into(),
        };

        let result = config.build().unwrap_err();
        assert_eq!(result.to_string(), "Invalid key length".to_string());
    }

    #[test]
    fn test_invalid_key_base64_local() {
        let config = KeyManagerConfig::Local {
            kek: "Ht8M1nDO/7fay+cft71M2Xy7j30EnLAsA84hSUMCm1k=blahblahblah".into(),
            kek_id: "".into(),
        };

        let result = config.build().unwrap_err();
        assert_eq!(
            result.to_string(),
            "Invalid byte 61, offset 43.".to_string()
        );
    }
}
