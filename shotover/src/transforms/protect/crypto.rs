use crate::frame::value::GenericValue;
use crate::transforms::protect::key_management::KeyManager;
use anyhow::{Result, anyhow, bail};
use chacha20poly1305::aead::Aead;
use chacha20poly1305::aead::rand_core::RngCore;
use chacha20poly1305::{ChaCha20Poly1305, Key, KeyInit, Nonce};
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
    let value = GenericValue::from(value);

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

    Ok(Operand::Const(format!(
        "0x{}",
        hex::encode(bincode::serialize(&protected)?)
    )))
}

pub async fn decrypt(
    value: &GenericValue,
    key_management: &KeyManager,
    key_id: &str,
) -> Result<GenericValue> {
    let bytes = match value {
        GenericValue::Bytes(bytes) => bytes,
        _ => bail!("expected varchar to decrypt but was not varchar"),
    };
    let protected: Protected = bincode::deserialize(bytes)?;

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
