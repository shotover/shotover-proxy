use crate::transforms::protect::key_management::KeyMaterial;
use anyhow::{Result, anyhow};
use aws_sdk_kms::Client as KmsClient;
use aws_sdk_kms::operation::decrypt::builders::DecryptFluentBuilder;
use aws_sdk_kms::operation::generate_data_key::builders::GenerateDataKeyFluentBuilder;
use aws_sdk_kms::primitives::Blob;
use bytes::Bytes;
use chacha20poly1305::Key;
use derivative::Derivative;
use std::collections::HashMap;

#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct AWSKeyManagement {
    #[derivative(Debug = "ignore")]
    pub client: KmsClient,

    pub cmk_id: String,
    pub encryption_context: Option<HashMap<String, String>>,
    pub key_spec: Option<String>,
    pub number_of_bytes: Option<i32>,
    pub grant_tokens: Option<Vec<String>>,
}

enum DecOrGen {
    Gen(GenerateDataKeyFluentBuilder),
    Dec(DecryptFluentBuilder),
}

// See https://docs.rs/aws-sdk-kms/latest/aws_sdk_kms/operation/generate_data_key/builders/struct.GenerateDataKeyFluentBuilder.html

impl AWSKeyManagement {
    pub async fn get_key(
        &self,
        dek: Option<Vec<u8>>,
        kek_alt: Option<String>,
    ) -> anyhow::Result<KeyMaterial> {
        let dog = dek.map_or_else(
            || {
                DecOrGen::Gen(
                    self.client
                        .generate_data_key()
                        .set_encryption_context(self.encryption_context.clone())
                        .set_grant_tokens(self.grant_tokens.clone())
                        .key_id(self.cmk_id.clone())
                        .set_key_spec(self.key_spec.as_ref().map(|x| x.as_str().into()))
                        .set_number_of_bytes(self.number_of_bytes),
                )
            },
            |dek| {
                DecOrGen::Dec(
                    self.client
                        .decrypt()
                        .ciphertext_blob(Blob::new(dek))
                        // This parameter is required only when the ciphertext was encrypted under an asymmetric CMK.
                        // The default value, SYMMETRIC_DEFAULT, represents the only supported algorithm that is valid for symmetric CMKs.
                        .set_encryption_algorithm(None)
                        .set_encryption_context(self.encryption_context.clone())
                        .set_grant_tokens(self.grant_tokens.clone())
                        // We use the cmk id provided by the protected value over the configured one as it may have been
                        // rotated out
                        .key_id(kek_alt.unwrap_or_else(|| self.cmk_id.clone())),
                )
            },
        );
        self.fetch_key(dog).await
    }
}

impl AWSKeyManagement {
    async fn fetch_key(&self, dog: DecOrGen) -> Result<KeyMaterial> {
        match dog {
            DecOrGen::Gen(generate) => {
                let resp = generate.send().await?;
                Ok(KeyMaterial {
                    ciphertext_blob: Bytes::from(
                        resp.ciphertext_blob
                            .ok_or_else(|| anyhow!("no ciphertext DEK found"))?
                            .into_inner(),
                    ),
                    key_id: resp.key_id.ok_or_else(|| anyhow!("no CMK id found"))?,
                    plaintext: *Key::from_slice(
                        resp.plaintext
                            .ok_or_else(|| anyhow!("no plaintext DEK provided"))?
                            .as_ref(),
                    ),
                })
            }
            DecOrGen::Dec(decrypt) => {
                let ciphertext_blob = decrypt
                    .get_ciphertext_blob()
                    .clone()
                    .unwrap()
                    .into_inner()
                    .into();
                let resp = decrypt.send().await?;
                Ok(KeyMaterial {
                    ciphertext_blob,
                    key_id: resp
                        .key_id
                        .ok_or_else(|| anyhow!("seemed to have lost cmk id on the way???"))?,
                    plaintext: *Key::from_slice(
                        resp.plaintext
                            .ok_or_else(|| anyhow!("no plaintext DEK provided"))?
                            .as_ref(),
                    ),
                })
            }
        }
    }
}
