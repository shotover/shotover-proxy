use crate::transforms::protect::key_management::KeyMaterial;
use anyhow::anyhow;
use anyhow::Result;
use bytes::Bytes;
use chacha20poly1305::Key;
use rusoto_kms::{DecryptRequest, GenerateDataKeyRequest, Kms, KmsClient};
use std::collections::HashMap;

#[derive(Clone)]
pub struct AWSKeyManagement {
    pub client: KmsClient,
    pub cmk_id: String,
    pub encryption_context: Option<HashMap<String, String>>,
    pub key_spec: Option<String>,
    pub number_of_bytes: Option<i64>,
    pub grant_tokens: Option<Vec<String>>,
}

enum DecOrGen {
    Gen(GenerateDataKeyRequest),
    Dec(DecryptRequest),
}

// See https://docs.rs/rusoto_kms/0.44.0/rusoto_kms/trait.Kms.html#tymethod.generate_data_key

impl AWSKeyManagement {
    pub async fn get_aws_key(
        &self,
        dek: Option<Vec<u8>>,
        kek_alt: Option<String>,
    ) -> anyhow::Result<KeyMaterial> {
        let dog = dek.map_or_else(
            || {
                DecOrGen::Gen(GenerateDataKeyRequest {
                    encryption_context: self.encryption_context.clone(),
                    grant_tokens: self.grant_tokens.clone(),
                    key_id: self.cmk_id.clone(),
                    key_spec: self.key_spec.clone(),
                    number_of_bytes: self.number_of_bytes,
                })
            },
            |dek| {
                DecOrGen::Dec(DecryptRequest {
                    ciphertext_blob: Bytes::from(dek),
                    // This parameter is required only when the ciphertext was encrypted under an asymmetric CMK.
                    // The default value, SYMMETRIC_DEFAULT, represents the only supported algorithm that is valid for symmetric CMKs.
                    encryption_algorithm: None,
                    encryption_context: self.encryption_context.clone(),
                    grant_tokens: self.grant_tokens.clone(),
                    // We use the cmk id provided by the protected value over the configured one as it may have been
                    // rotated out
                    key_id: kek_alt.or_else(|| Some(self.cmk_id.clone())),
                })
            },
        );
        self.fetch_key(dog).await
    }

    async fn fetch_key(&self, dog: DecOrGen) -> Result<KeyMaterial> {
        match dog {
            DecOrGen::Gen(g) => {
                let resp = self.client.generate_data_key(g).await?;
                Ok(KeyMaterial {
                    ciphertext_blob: resp
                        .ciphertext_blob
                        .ok_or_else(|| anyhow!("no ciphertext DEK found"))?,
                    key_id: resp.key_id.ok_or_else(|| anyhow!("no CMK id found"))?,
                    plaintext: Key::from_slice(
                        &resp
                            .plaintext
                            .ok_or_else(|| anyhow!("no plaintext DEK provided"))?,
                    )
                    .to_vec(),
                })
            }
            DecOrGen::Dec(d) => {
                let resp = self.client.decrypt(d.clone()).await?;
                Ok(KeyMaterial {
                    ciphertext_blob: d.ciphertext_blob.clone(),
                    key_id: d
                        .key_id
                        .ok_or_else(|| anyhow!("seemed to have lost cmk id on the way???"))?,
                    plaintext: Key::from_slice(
                        &resp
                            .plaintext
                            .ok_or_else(|| anyhow!("no plaintext DEK provided"))?,
                    )
                    .to_vec(),
                })
            }
        }
    }
}
