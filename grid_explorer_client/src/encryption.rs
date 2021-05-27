use bip39::Mnemonic;
use sodiumoxide::randombytes;

use x25519_dalek::{PublicKey, StaticSecret};

use blake2::digest::{Update, VariableOutput};
use sodiumoxide::crypto::secretbox;
use std::convert::TryInto;
use std::str;

pub enum EncryptionError {
    EncryptionError(String),
}

fn random_nonce() -> [u8; 24] {
    randombytes::randombytes(24).try_into().unwrap()
}

pub fn encrypt(msg: &str, mnemonic: &Mnemonic, node_pub: &str) -> Result<String, EncryptionError> {
    let entropy = mnemonic.to_entropy();
    let mut node_verify_bin = [0u8; 32];
    let err = binascii::hex2bin(node_pub.as_bytes(), &mut node_verify_bin);
    if err.is_err() {
        return Err(EncryptionError::EncryptionError(String::from(
            "bad node public key",
        )));
    }
    let mut lpk = [1u8; 32];
    let mut lsk = [1u8; 32];
    unsafe {
        let e = libsodium_sys::crypto_sign_ed25519_pk_to_curve25519(
            lpk.as_mut_ptr(),
            node_verify_bin.as_ptr(),
        );
        if e == -1 {
            return Err(EncryptionError::EncryptionError(String::from(
                "coudn't get public key from the mnemonic",
            )));
        }
        let e =
            libsodium_sys::crypto_sign_ed25519_sk_to_curve25519(lsk.as_mut_ptr(), entropy.as_ptr());
        if e == -1 {
            return Err(EncryptionError::EncryptionError(String::from(
                "couldn't get private key from mnemonic",
            )));
        }
    }
    let os = StaticSecret::from(lsk);
    let tp = PublicKey::from(lpk);
    let sh = os.diffie_hellman(&tp);
    let mut hasher = blake2::VarBlake2b::new(32).unwrap();
    hasher.update(sh.as_bytes());
    let res = hasher.finalize_boxed();
    let nonce_bytes: [u8; 24] = random_nonce();
    let nonce = secretbox::Nonce(nonce_bytes);
    let shared_secret = secretbox::Key::from_slice(&*res);
    if shared_secret.is_none() {
        return Err(EncryptionError::EncryptionError(String::from(
            "coulnd't generate shared secret from the given keys",
        )));
    };
    let shared_secret = shared_secret.unwrap();
    let mut ciph = secretbox::seal(msg.as_bytes(), &nonce, &shared_secret);
    let mut res = nonce_bytes.to_vec();
    res.append(&mut ciph);
    let mut hex = vec![0; res.len() * 2];
    let err = binascii::bin2hex(res.as_slice(), &mut hex);
    if err.is_err() {
        return Err(EncryptionError::EncryptionError(String::from(
            "coudn't convert result to hex",
        )));
    }
    Ok(String::from_utf8(hex).unwrap())
}
