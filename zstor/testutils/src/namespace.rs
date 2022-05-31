use crate::zdb::Zdb;

pub struct Namespace {
    name: String,
    zdb: Zdb,
}

impl Namespace {
    pub fn new(name: String, zdb: Zdb) -> Self {
        Self { name, zdb }
    }
    pub fn delete(&self) {
        self.zdb.delete_namespace(self.name.clone());
    }
}
