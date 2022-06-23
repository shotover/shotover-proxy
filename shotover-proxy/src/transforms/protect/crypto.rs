use crate::message::MessageValue;
use crate::transforms::protect::key_management::KeyManager;
use anyhow::{anyhow, bail, Result};
use chacha20poly1305::{
    aead::{rand_core::RngCore, Aead, NewAead},
    {ChaCha20Poly1305, Key, Nonce},
};
use cql3_parser::common::Operand;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Protected {
    cipher: Vec<u8>,
    nonce: Nonce,
    enc_dek: Vec<u8>,
    kek_id: String,
}

pub async fn encrypt(
    value: &Operand,
    key_management: &KeyManager,
    key_id: &str,
) -> Result<Operand> {
    let value = MessageValue::from(&*value);

    let sym_key = key_management.cached_get_key(key_id, None, None).await?;

    let ser = bincode::serialize(&value)?;
    let nonce = gen_nonce();
    let cipher = ChaCha20Poly1305::new(&sym_key.plaintext);
    let ciphertext = cipher
        .encrypt(&nonce, &*ser)
        .map_err(|_| anyhow!("couldn't encrypt value"))?;

    let protected = Protected {
        cipher: ciphertext,
        nonce,
        enc_dek: sym_key.ciphertext_blob.to_vec(),
        kek_id: sym_key.key_id,
    };

    // TODO: investigate using storing as blob instead of text and then use textAsBlob here
    // We should functionally verify which has better performance before sticking with one.
    Ok(Operand::Const(format!(
        "'{}'",
        serde_json::to_string(&protected)?
    )))
}

pub async fn decrypt(
    value: &MessageValue,
    key_management: &KeyManager,
    key_id: &str,
) -> Result<MessageValue> {
    let varchar = match value {
        MessageValue::Varchar(varchar) => varchar,
        _ => bail!("expected varchar to decrypt but was {:?}", value),
    };
    let protected: Protected = serde_json::from_str(varchar)?;

    let sym_key = key_management
        .cached_get_key(key_id, Some(protected.enc_dek), Some(protected.kek_id))
        .await?;

    let nonce = Nonce::from_slice(&protected.nonce);
    let cipher = ChaCha20Poly1305::new(&sym_key.plaintext);

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
    *Key::from_slice(&key_bytes)
}

pub fn gen_nonce() -> Nonce {
    let mut rng = rand::thread_rng();
    let mut nonce_bytes = [0; 12];
    rng.fill_bytes(&mut nonce_bytes);
    *Nonce::from_slice(&nonce_bytes)
}
