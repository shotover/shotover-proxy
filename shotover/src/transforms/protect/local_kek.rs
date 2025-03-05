use crate::transforms::protect::crypto::{gen_key, gen_nonce};
use crate::transforms::protect::key_management::KeyMaterial;
use anyhow::{Result, anyhow};
use bytes::Bytes;
use chacha20poly1305::aead::Aead;
use chacha20poly1305::{ChaCha20Poly1305, Key, KeyInit, Nonce};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug)]
pub struct LocalKeyManagement {
    pub kek: Key,
    pub kek_id: String,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DEKStructure {
    pub nonce: Nonce,
    pub key: Vec<u8>,
}

impl LocalKeyManagement {
    pub fn get_key(&self, dek: Option<Vec<u8>>) -> Result<KeyMaterial> {
        match dek {
            None => {
                let plaintext_dek = gen_key();
                let nonce = gen_nonce();

                let cipher = ChaCha20Poly1305::new(Key::from_slice(&plaintext_dek));

                let encrypted_dek = cipher
                    .encrypt(&nonce, &*plaintext_dek)
                    .map_err(|_| anyhow!("couldn't encrypt value"))?;

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

                let cipher = ChaCha20Poly1305::new(Key::from_slice(&self.kek));

                let plaintext_dek = cipher
                    .decrypt(Nonce::from_slice(&dek_struct.nonce), &*dek_struct.key)
                    .map_err(|_| anyhow!("couldn't decrypt DEK"))?;

                Ok(KeyMaterial {
                    ciphertext_blob: Bytes::from(dek),
                    key_id: self.kek_id.clone(),
                    plaintext: *Key::from_slice(plaintext_dek.as_slice()),
                })
            }
        }
    }
}
