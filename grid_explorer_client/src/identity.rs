use bip39::{Error, Mnemonic};
use ed25519_dalek::{Keypair, PublicKey, SecretKey, SignatureError, Signer};
use sha2::{Digest, Sha256};
use std::str::FromStr;

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
    pub mnemonic: Mnemonic,
}

impl Identity {
    pub fn new(
        name: String,
        email: String,
        user_id: i64,
        mnemonic: &str,
    ) -> Result<Identity, IdentityError> {
        let mnemonic = Mnemonic::from_str(mnemonic)?;
        let entropy = mnemonic.to_entropy();

        let secret_key: SecretKey = SecretKey::from_bytes(&entropy)?;
        let public_key: PublicKey = (&secret_key).into();

        let keypair = Keypair {
            secret: secret_key,
            public: public_key,
        };

        let id = Identity {
            name,
            email,
            user_id,
            keypair,
            mnemonic,
        };

        Ok(id)
    }
    pub fn get_id(&self) -> i64 {
        self.user_id
    }

    pub fn hash_and_sign(&self, input: &[u8]) -> [u8; 64] {
        let mut hasher = Sha256::new();
        hasher.update(input);
        let result = hasher.finalize();
        self.keypair.sign(result.as_slice()).to_bytes()
    }

    pub fn sign(&self, input: &[u8]) -> [u8; 64] {
        self.keypair.sign(input).to_bytes()
    }
}
