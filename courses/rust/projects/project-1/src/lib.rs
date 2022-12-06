use std::collections::HashMap;

pub struct KvStore {
    table: HashMap<String, String>,
}

impl KvStore {
    pub fn new() -> Self {
        Self {
            table: HashMap::new(),
        }
    }
    /// Set the value of a string key to a string
    pub fn set(&mut self, key: String, value: String) {
        self.table.insert(key, value);
    }

    /// Get the string value of the a string key. If the key does not exist, return None.
    pub fn get(&self, key: String) -> Option<String> {
        self.table.get(&key).map(|s| s.to_owned())
    }

    /// Remove a given key.
    pub fn remove(&mut self, key: String) {
        self.table.remove(&key);
    }
}
