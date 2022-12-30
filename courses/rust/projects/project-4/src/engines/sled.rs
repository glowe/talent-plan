use crate::KvsEngine;

use crate::error::KvsError;
use crate::error::Result;
use sled::Db;

#[derive(Clone)]
pub struct SledKvsEngine {
    db: Db,
}

impl SledKvsEngine {
    pub fn new(db: Db) -> Self {
        Self { db }
    }
}

impl KvsEngine for SledKvsEngine {
    fn set(&self, key: String, value: String) -> Result<()> {
        self.db.insert(key, value.as_str())?;
        self.db.flush()?;
        Ok(())
    }

    fn get(&self, key: String) -> Result<Option<String>> {
        let value = self
            .db
            .get(key)?
            .map(|i_vec| i_vec.to_vec())
            .map(String::from_utf8)
            .transpose()?;
        Ok(value)
    }

    fn remove(&self, key: String) -> Result<()> {
        self.db.remove(key)?.ok_or(KvsError::KeyNotFound)?;
        self.db.flush()?;
        Ok(())
    }
}
