use stellar_base::crypto::KeyPair;

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
        unimplemented!()
    }
}
