// use stellar_base::crypto::KeyPair;
use bip39::{Mnemonic, Error};
use std::str::FromStr;
use ed25519_dalek::{Signer, Keypair, SecretKey, PublicKey, SignatureError};

#[derive(Debug)]
pub enum IdentityError {
    MnemonicError(Error),
    SignatureError(SignatureError),
}

impl From<Error> for IdentityError {
    fn from(err: Error) -> IdentityError {
        IdentityError::MnemonicError(err)
    }
}

impl From<SignatureError> for IdentityError {
    fn from(err: SignatureError) -> IdentityError {
        IdentityError::SignatureError(err)
    }
}

pub struct Identity {
    pub name: String,
    pub email: String,
    pub user_id: i64,
    pub keypair: Keypair,
}

pub fn new(name: String, email: String, user_id: i64, mnemonic: &str) -> Result<Identity, IdentityError> {
    let mnemonic = Mnemonic::from_str(mnemonic)?;
    let entropy = mnemonic.to_entropy();

    let secret_key: SecretKey = SecretKey::from_bytes(&entropy)?;
    let public_key: PublicKey = (&secret_key).into();

    let keypair = Keypair {
        secret: secret_key,
        public: public_key
    };

    let id = Identity{
        name,
        email,
        user_id,
        keypair
    };

    return Ok(id)
}

impl Identity {
    pub fn get_id(&self) -> i64 {
        self.user_id
    }

    pub fn sign(&self, input: &[u8]) -> [u8; 64] {
        self.keypair.sign(input).to_bytes()
    }
}
