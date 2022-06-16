use crate::message::MessageValue;
use crate::transforms::protect::key_management::KeyManager;
use anyhow::{anyhow, bail, Result};
use chacha20poly1305::aead::{Aead, NewAead};
use chacha20poly1305::{ChaCha20Poly1305, Key, Nonce};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sqlparser::ast::Value as SQLValue;

// A protected value meets the following properties:
// https://doc.libsodium.org/secret-key_cryptography/secretbox
// This all relies on crypto_secretbox_easy which takes care of
// all padding, copying and timing issues associated with crypto
#[derive(Serialize, Deserialize)]
struct Protected {
    cipher: Vec<u8>,
    nonce: Vec<u8>,
    enc_dek: Vec<u8>,
    kek_id: String,
}

pub async fn encrypt(
    value: &SQLValue,
    key_management: &KeyManager,
    key_id: &str,
) -> Result<SQLValue> {
    let value = MessageValue::from(&*value);

    let sym_key = key_management.cached_get_key(key_id, None, None).await?;

    let ser = bincode::serialize(&value)?;
    let nonce = gen_nonce();
    let cipher = ChaCha20Poly1305::new(Key::from_slice(&sym_key.plaintext));
    let ciphertext = cipher
        .encrypt(&nonce, &*ser)
        .map_err(|_| anyhow!("couldn't encrypt value"))?;

    let protected = Protected {
        cipher: ciphertext,
        nonce: nonce.to_vec(),
        enc_dek: sym_key.ciphertext_blob.to_vec(),
        kek_id: sym_key.key_id,
    };

    Ok(SQLValue::SingleQuotedString(serde_json::to_string(
        &protected,
    )?))
}

pub async fn decrypt(
    value: &MessageValue,
    key_management: &KeyManager,
    key_id: &str,
) -> Result<MessageValue> {
    let bytes = match value {
        MessageValue::Bytes(bytes) => bytes,
        _ => bail!("expected varchar to decrypt but was {:?}", value),
    };
    let protected: Protected = serde_json::from_slice(bytes)?;

    let sym_key = key_management
        .cached_get_key(key_id, Some(protected.enc_dek), Some(protected.kek_id))
        .await?;

    let nonce = Nonce::from_slice(&protected.nonce);
    let cipher = ChaCha20Poly1305::new(Key::from_slice(&sym_key.plaintext));

    let decrypted_bytes = cipher
        .decrypt(nonce, &*protected.cipher)
        .map_err(|_| anyhow!("couldn't decrypt value"))?;

    //TODO make error handing better here - failure here indicates an authenticity failure
    bincode::deserialize(&decrypted_bytes).map_err(|_| anyhow!("couldn't decrypt value"))
}

pub fn gen_key() -> Key {
    let mut key_bytes = [0; 32];
    let mut rng = rand::thread_rng();
    rng.fill_bytes(&mut key_bytes);
    let key = Key::from_slice(&key_bytes);
    *key
}

pub fn gen_nonce() -> Nonce {
    let mut rng = rand::thread_rng();
    let mut nonce_bytes = [0; 12];
    rng.fill_bytes(&mut nonce_bytes);
    let nonce = Nonce::from_slice(&nonce_bytes);
    *nonce
}
