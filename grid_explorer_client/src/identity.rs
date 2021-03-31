// use stellar_base::crypto::KeyPair;
use bip39::{Mnemonic};
use std::str::FromStr;
use ed25519_dalek::{Signer, Keypair};
use hex;

pub struct Identity {
    pub name: String,
    pub email: String,
    pub user_id: i64,
}

impl Identity {
    pub fn get_id(&self) -> i64 {
        self.user_id
    }

    pub fn sign_hex(&self, json: String) -> String {
        let mnemonic = Mnemonic::from_str("").unwrap();

        let seed = mnemonic.to_entropy();

        // if seed.len() != 32 {
            // throw error
        // }

        let keypair: Keypair = Keypair::from_bytes(&seed[0..31]).unwrap();

        let signature = keypair.sign(json.as_bytes());

        hex::encode(signature.to_bytes().to_vec())
    }
}
