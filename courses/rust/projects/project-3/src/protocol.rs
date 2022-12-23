use serde::Deserialize;
use serde::Serialize;

#[derive(Deserialize, Serialize, Debug)]
pub enum Request {
    Get(String),
    Set(String, String),
    Remove(String),
}

#[derive(Deserialize, Serialize, Debug)]
pub enum GetResponse {
    Ok(Option<String>),
    Err(String),
}

#[derive(Deserialize, Serialize, Debug)]
pub enum SetResponse {
    Ok(()),
    Err(String),
}

#[derive(Deserialize, Serialize, Debug)]
pub enum RemoveResponse {
    Ok(()),
    Err(String),
}
