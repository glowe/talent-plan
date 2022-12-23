use std::error;
use std::fmt;
use std::io;
use std::result;
use std::string::FromUtf8Error;

use rmp_serde::decode;
use rmp_serde::encode;

#[derive(Debug)]
pub enum KvsError {
    Decode(decode::Error),
    Encode(encode::Error),
    IO(io::Error),
    KeyNotFound,
    UnexpectedCommand,
    StringError(String),
    Sled(sled::Error),
    Utf8(FromUtf8Error),
}

impl fmt::Display for KvsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Encode(err) => write!(f, "Encode: {}", err),
            Self::Decode(err) => write!(f, "Decode: {}", err),
            Self::IO(err) => write!(f, "IO: {}", err),
            Self::KeyNotFound => write!(f, "Key not found"),
            Self::UnexpectedCommand => write!(f, "UnexpectedCommand"),
            Self::StringError(msg) => write!(f, "{}", msg),
            Self::Sled(err) => write!(f, "Sled: {}", err),
            Self::Utf8(err) => write!(f, "Utf8: {}", err),
        }
    }
}

impl error::Error for KvsError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            Self::Decode(source) => Some(source),
            Self::Encode(source) => Some(source),
            Self::IO(source) => Some(source),
            Self::KeyNotFound => None,
            Self::UnexpectedCommand => None,
            Self::StringError(_) => None,
            Self::Sled(source) => Some(source),
            Self::Utf8(source) => Some(source),
        }
    }
}

impl From<encode::Error> for KvsError {
    fn from(e: encode::Error) -> Self {
        Self::Encode(e)
    }
}

impl From<decode::Error> for KvsError {
    fn from(e: decode::Error) -> Self {
        Self::Decode(e)
    }
}

impl From<io::Error> for KvsError {
    fn from(e: io::Error) -> Self {
        Self::IO(e)
    }
}

impl From<sled::Error> for KvsError {
    fn from(e: sled::Error) -> Self {
        Self::Sled(e)
    }
}

impl From<FromUtf8Error> for KvsError {
    fn from(e: FromUtf8Error) -> Self {
        Self::Utf8(e)
    }
}

pub type Result<T> = result::Result<T, KvsError>;
