use rmp_serde::decode;
use rmp_serde::encode;
use rmp_serde::Deserializer;
use rmp_serde::Serializer;
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashMap;
use std::convert::From;
use std::convert::Into;
use std::error::Error;
use std::fmt::Display;
use std::fmt::Formatter;
use std::fs;
use std::fs::File;
use std::io;
use std::io::prelude::*;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Seek;
use std::io::SeekFrom;
use std::path::PathBuf;
use std::result;

#[derive(Debug)]
pub enum KvStoreError {
    DecodeError(String),
    EncodeError(String),
    IOError(io::Error),
    KeyNotFound,
}

impl Display for KvStoreError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::IOError(err) => write!(f, "IOError: {}", err),
            Self::EncodeError(msg) => write!(f, "trouble encoding command: {}", msg),
            Self::DecodeError(msg) => write!(f, "trouble decoding command: {}", msg),
            Self::KeyNotFound => write!(f, "Key not found"),
        }
    }
}

impl Error for KvStoreError {}

impl From<encode::Error> for KvStoreError {
    fn from(e: encode::Error) -> Self {
        Self::EncodeError(e.to_string())
    }
}

impl From<decode::Error> for KvStoreError {
    fn from(e: decode::Error) -> Self {
        Self::DecodeError(e.to_string())
    }
}

impl From<io::Error> for KvStoreError {
    fn from(e: io::Error) -> Self {
        Self::IOError(e)
    }
}

pub struct KvStore {
    reader: BufReader<File>,
    writer: BufWriter<File>,
    index: HashMap<String, u64>,
}

pub type Result<T> = result::Result<T, KvStoreError>;

#[derive(Deserialize, Serialize, Debug)]
enum Command {
    Set(String, String),
    Remove(String),
}

impl KvStore {
    /// Open the KvStore at a given path. Return the KvStore.
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let mut path: PathBuf = path.into();
        if !path.exists() {
            fs::create_dir_all(&path)?;
        }
        path.push("kvs.log");

        let mut wfile = File::options()
            .write(true)
            .append(true)
            .create(true)
            .open(&path)?;

        let rfile = File::open(&path)?;
        let mut reader = BufReader::new(rfile);
        let mut des = Deserializer::new(&mut reader);
        let mut index = HashMap::new();
        let mut pos = 0;
        loop {
            match Command::deserialize(&mut des) {
                Ok(Command::Set(key, _)) => {
                    index.insert(key, pos);
                }
                Ok(Command::Remove(key)) => {
                    index.remove(&key);
                }
                Err(decode::Error::InvalidMarkerRead(err)) => match err.kind() {
                    std::io::ErrorKind::UnexpectedEof => {
                        break;
                    }
                    _ => return Err(KvStoreError::IOError(err)),
                },
                Err(err) => return Err(KvStoreError::DecodeError(err.to_string())),
            }
            pos = des.get_mut().stream_position()?;
        }

        wfile.seek(SeekFrom::End(0))?;
        let writer = BufWriter::new(wfile);
        Ok(Self {
            reader,
            writer,
            index,
        })
    }

    /// Set the value of a string key to a string. Return an error if the value is not written successfully.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        /*
        The user invokes kvs set mykey myvalue
        kvs creates a value representing the "set" command, containing its key and value
        It then serializes that command to a String
        It then appends the serialized command to a file containing the log
        If that succeeds, it exits silently with error code 0
        If it fails, it exits by printing the error and returning a non-zero error code
        */
        let cmd = Command::Set(key.clone(), value.clone());
        let pos = self.writer.stream_position()?;
        cmd.serialize(&mut Serializer::new(&mut self.writer))?;
        self.index.insert(key, pos);
        self.writer.flush()?;
        Ok(())
    }

    /// Get the string value of a string key. If the key does not exist, return None. Return an error if the value is not read successfully.
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        /*
        The user invokes kvs get mykey
        kvs reads the entire log, one command at a time, recording the affected key and file offset of the command to an in-memory key -> log pointer map
        It then checks the map for the log pointer
        If it fails, it prints "Key not found", and exits with exit code 0
        If it succeeds
        It deserializes the command to get the last recorded value of the key
        It prints the value to stdout and exits with exit code 0
        */
        if let Some(pos) = self.index.get(&key) {
            self.reader.seek(SeekFrom::Start(*pos))?;

            let mut des = Deserializer::new(&mut self.reader);
            match Command::deserialize(&mut des) {
                Ok(Command::Set(_, value)) => Ok(Some(value)),
                Ok(Command::Remove(_)) => Err(KvStoreError::DecodeError(
                    "Found remove, when expected set".to_string(),
                )),
                Err(decode::Error::InvalidMarkerRead(err)) => Err(KvStoreError::IOError(err)),
                Err(err) => return Err(KvStoreError::DecodeError(err.to_string())),
            }
        } else {
            Ok(None)
        }
    }

    /// Remove a given key. Return an error if the key does not exist or is not removed successfully.
    pub fn remove(&mut self, key: String) -> Result<()> {
        /*
        The user invokes kvs rm mykey
        Same as the "get" command, kvs reads the entire log to build the in-memory index
        It then checks the map if the given key exists
        If the key does not exist, it prints "Key not found", and exits with a non-zero error code
        If it succeeds
        It creates a value representing the "rm" command, containing its key
        It then appends the serialized command to the log
        If that succeeds, it exits silently with error code 0
        */
        if let Some(_) = self.index.remove(&key) {
            let cmd = Command::Remove(key.clone());
            cmd.serialize(&mut Serializer::new(&mut self.writer))?;
            Ok(())
        } else {
            Err(KvStoreError::KeyNotFound)
        }
    }
}
