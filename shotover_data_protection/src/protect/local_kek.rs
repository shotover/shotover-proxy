use crate::protect::KeyMaterial;
use anyhow::anyhow;
use anyhow::Result;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sodiumoxide::crypto::secretbox;
use sodiumoxide::crypto::secretbox::{Key, Nonce};

#[derive(Clone)]
pub struct LocalKeyManagement {
    pub kek: Key,
    pub kek_id: String,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct DEKStructure {
    pub nonce: Nonce,
    pub key: Vec<u8>,
}

impl LocalKeyManagement {
    pub async fn get_key(&self, dek: Option<Vec<u8>>) -> Result<KeyMaterial> {
        match dek {
            None => {
                let plaintext_dek = secretbox::gen_key();
                let nonce = secretbox::gen_nonce();
                let encrypted_dek = secretbox::seal(&plaintext_dek.0, &nonce, &self.kek);
                let dek_struct = DEKStructure {
                    nonce,
                    key: encrypted_dek,
                };
                let cipher_blob = serde_json::to_string(&dek_struct)?;
                Ok(KeyMaterial {
                    ciphertext_blob: Bytes::from(cipher_blob),
                    key_id: self.kek_id.clone(),
                    plaintext: plaintext_dek,
                })
            }
            Some(dek) => {
                let dek_struct: DEKStructure = serde_json::from_slice(dek.as_slice())?;
                let plaintext_dek =
                    secretbox::open(dek_struct.key.as_slice(), &dek_struct.nonce, &self.kek)
                        .map_err(|_| anyhow!("couldn't decrypt DEK"))?;
                Ok(KeyMaterial {
                    ciphertext_blob: Bytes::from(dek),
                    key_id: self.kek_id.clone(),
                    plaintext: Key::from_slice(plaintext_dek.as_slice())
                        .ok_or_else(|| anyhow!("could not build key"))?,
                })
            }
        }
    }
}
