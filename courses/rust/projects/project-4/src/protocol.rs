use serde::Deserialize;
use serde::Serialize;

#[derive(Deserialize, Serialize, Debug)]
pub enum Request {
    Get(String),
    Set(String, String),
    Remove(String),
}

#[derive(Deserialize, Serialize, Debug)]
pub enum Response {
    GetOk(Option<String>),
    SetOk(()),
    RemoveOk(()),
    Err(String),
}
