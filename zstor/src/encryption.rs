use crate::config;
use aes_gcm::aead::{generic_array::GenericArray, Aead, NewAead};
use rand::prelude::*;
use serde::{
    de::{self, Visitor},
    Deserialize, Deserializer, Serialize, Serializer,
};
use std::convert::TryInto;
use std::fmt;

/// Result type for encryption and decryption operations
pub type EncryptionResult<T> = Result<T, EncryptionError>;

const AES_GCM_NONCE_SIZE: usize = 12;

/// A general encryptor, able to encrypt and decrypt data. Encryptors are expected to implement
/// symmetric encryption.
pub trait Encryptor {
    /// Encrypt some data using the encryptor. The encryptor generates a new IV, and prepends it to
    /// the encrypted data.
    fn encrypt(&self, data: &[u8]) -> EncryptionResult<Vec<u8>>;
    /// Decrypt some data using the encryptor. The IV is assumed to be prepended to the data. It is
    /// the callers responsibility to ensure storage and recovery of the IV ishandled properly.
    fn decrypt(&self, data: &[u8]) -> EncryptionResult<Vec<u8>>;
}

/// Create a new [`Encryptor`] from an encryption config object.
pub fn new(ce: config::Encryption) -> Box<dyn Encryptor> {
    match ce {
        config::Encryption::Aes(key) => Box::new(AesGcm::new(key)),
    }
}

///  An implementation of the AES encryption algorithm running in GCM mode.
#[derive(Debug, Clone)]
pub struct AesGcm {
    /// the key to use for encrypting and decrypting.
    key: SymmetricKey,
}

impl AesGcm {
    /// Create a new instance of the [`AesGcm`] encryptor, using the provided key for all
    /// operations.
    pub fn new(key: SymmetricKey) -> Self {
        Self { key }
    }
}

impl Encryptor for AesGcm {
    fn encrypt(&self, data: &[u8]) -> EncryptionResult<Vec<u8>> {
        let key = GenericArray::from_slice(&self.key[..]);
        let cipher = aes_gcm::Aes256Gcm::new(key);

        let nonce = GenericArray::clone_from_slice(&thread_rng().gen::<[u8; AES_GCM_NONCE_SIZE]>());

        // TODO: not really efficient way of doing things here
        let mut total = Vec::with_capacity(AES_GCM_NONCE_SIZE + data.len() + 16);

        let ciphertext = cipher.encrypt(&nonce, data).map_err(|e| EncryptionError {
            kind: EncryptionErrorKind::Encrypt,
            internal: e,
        })?;
        total.extend(nonce.as_slice());
        total.extend(ciphertext);

        Ok(total)
    }

    fn decrypt(&self, data: &[u8]) -> EncryptionResult<Vec<u8>> {
        let key = GenericArray::from_slice(&self.key[..]);
        let cipher = aes_gcm::Aes256Gcm::new(key);

        let iv = &data[..AES_GCM_NONCE_SIZE];
        debug_assert!(iv.len() == AES_GCM_NONCE_SIZE);
        let nonce = GenericArray::from_slice(iv);

        let plaintext = cipher.decrypt(nonce, &data[AES_GCM_NONCE_SIZE..]).unwrap();
        Ok(plaintext)
    }
}

const KEY_LEN: usize = 32;
const HEX_KEY_LEN: usize = 2 * KEY_LEN;

/// A symmetric encryption key of exactly 32 bytes
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SymmetricKey([u8; KEY_LEN]);

impl SymmetricKey {
    /// Create a new [`SymmetricKey`] from the given array.
    pub const fn new(value: [u8; KEY_LEN]) -> Self {
        Self(value)
    }
}

impl std::ops::Deref for SymmetricKey {
    type Target = [u8; 32];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Serialize for SymmetricKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&hex::encode(self.0))
    }
}

impl<'de> Deserialize<'de> for SymmetricKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(SymKeyVisitor)
    }
}

struct SymKeyVisitor;

impl<'de> Visitor<'de> for SymKeyVisitor {
    type Value = SymmetricKey;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a hex encoded byte slice of length 32 (64 hex chars)")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        if v.len() != HEX_KEY_LEN {
            return Err(E::invalid_length(v.len(), &Self));
        }

        hex::decode(v)
            .map_err(E::custom)
            .map(|vec| SymmetricKey(vec.try_into().unwrap()))
    }
}

/// Errors related to encyrpting and decrypting
#[derive(Debug)]
pub struct EncryptionError {
    kind: EncryptionErrorKind,
    internal: aes_gcm::Error,
}

impl fmt::Display for EncryptionError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Crypto error: {}: {}", self.kind, self.internal)
    }
}

impl std::error::Error for EncryptionError {
    fn cause(&self) -> Option<&dyn std::error::Error> {
        Some(&self.internal)
    }
}

/// Specific error type related to encyrpting and decrypting
#[derive(Debug)]
pub enum EncryptionErrorKind {
    /// Error while encrypting data
    Encrypt,
    /// Error while decrypting data
    Decrypt,
}

impl fmt::Display for EncryptionErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Operation {}",
            match self {
                EncryptionErrorKind::Encrypt => "ENCRYPT",
                EncryptionErrorKind::Decrypt => "DECRYPT",
            }
        )
    }
}

#[cfg(test)]
mod tests {
    use super::{AesGcm, Encryptor, SymmetricKey};
    use rand::Rng;

    #[test]
    fn aesgcm_roundtrip() {
        let key = SymmetricKey::new(rand::thread_rng().gen::<[u8; 32]>());
        let enc = AesGcm::new(key);

        let data = rand::thread_rng().gen::<[u8; 16]>();

        let res = enc.encrypt(&data);
        assert!(res.is_ok());

        let ciphertext = res.unwrap();

        let plain_res = enc.decrypt(&ciphertext);

        assert!(plain_res.is_ok());

        let plain = plain_res.unwrap();

        assert_eq!(&plain, &data);
    }
}
