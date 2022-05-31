use crate::message::MessageValue;
use crate::transforms::protect::key_management::KeyManager;
use anyhow::{anyhow, bail, Result};
use serde::{Deserialize, Serialize};
use sodiumoxide::crypto::secretbox;
use sodiumoxide::crypto::secretbox::Nonce;
use sqlparser::ast::Value as SQLValue;

// A protected value meets the following properties:
// https://doc.libsodium.org/secret-key_cryptography/secretbox
// This all relies on crypto_secretbox_easy which takes care of
// all padding, copying and timing issues associated with crypto
#[derive(Serialize, Deserialize)]
struct Protected {
    cipher: Vec<u8>,
    nonce: Nonce,
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
    let nonce = secretbox::gen_nonce();
    let cipher = secretbox::seal(&ser, &nonce, &sym_key.plaintext);

    let protected = Protected {
        cipher,
        nonce,
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

    let decrypted_bytes = secretbox::open(&protected.cipher, &protected.nonce, &sym_key.plaintext)
        .map_err(|_| anyhow!("couldn't decrypt value"))?;

    //TODO make error handing better here - failure here indicates an authenticity failure
    bincode::deserialize(&decrypted_bytes).map_err(|_| anyhow!("couldn't decrypt value"))
}
