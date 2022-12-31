use super::KvsEngine;
use crate::KvsError;
use crate::Result;
use rmp_serde::decode;
use rmp_serde::Deserializer;
use rmp_serde::Serializer;
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashMap;
use std::convert::Into;
use std::ffi::OsStr;
use std::fs;
use std::fs::File;
use std::io;
use std::io::prelude::*;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Seek;
use std::io::SeekFrom;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;

struct CommandPosition {
    log_number: u64,
    offset: u64,
    bytes: u64,
}

#[derive(Clone)]
pub struct KvStore {
    readers: Arc<Mutex<HashMap<u64, BufReader<File>>>>,
    writer: Arc<Mutex<BufWriter<File>>>,
    index: Arc<Mutex<HashMap<String, CommandPosition>>>,
    log_number: Arc<Mutex<u64>>,
    path: PathBuf,
    uncompacted_bytes: Arc<Mutex<u64>>,
}

#[derive(Deserialize, Serialize, Debug)]
enum Command {
    Set(String, String),
    Remove(String),
}

fn log_path(path: &Path, log_number: u64) -> PathBuf {
    let file_name = format!("{}.kvs.log", log_number);
    path.join(file_name)
}

fn get_log_numbers(dir: &Path) -> io::Result<Vec<u64>> {
    // Format of a log file name is <number>.kvs.log
    let mut log_numbers: Vec<u64> = fs::read_dir(dir)?
        .flat_map(|result| -> io::Result<PathBuf> { Ok::<PathBuf, io::Error>(result?.path()) })
        .filter(|path| path.is_file() && path.extension() == Some("log".as_ref()))
        .flat_map(|path| {
            path.file_stem()
                .and_then(OsStr::to_str)
                .map(|stem| stem.trim_end_matches(".kvs"))
                .map(|number| number.parse::<u64>())
        })
        .flatten()
        .collect();
    log_numbers.sort_unstable();
    Ok(log_numbers)
}

fn load_index(
    log_number: u64,
    index: &mut HashMap<String, CommandPosition>,
    reader: &mut BufReader<File>,
) -> Result<()> {
    let mut des = Deserializer::new(reader);
    let mut offset = 0;
    loop {
        match Command::deserialize(&mut des) {
            Ok(Command::Set(key, _)) => {
                let bytes = des.get_mut().stream_position()? - offset;
                index.insert(
                    key,
                    CommandPosition {
                        log_number,
                        offset,
                        bytes,
                    },
                );
            }
            Ok(Command::Remove(key)) => {
                index.remove(&key);
            }
            Err(decode::Error::InvalidMarkerRead(err)) => match err.kind() {
                std::io::ErrorKind::UnexpectedEof => {
                    break;
                }
                _ => return Err(KvsError::IO(err)),
            },
            Err(err) => return Err(KvsError::Decode(err)),
        }
        offset = des.get_mut().stream_position()?;
    }
    Ok(())
}

const COMPACTION_THRESHOLD_BYTES: u64 = 1048576;

impl KvStore {
    /// Open the KvStore at a given path. Return the KvStore.
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        fs::create_dir_all(&path)?;

        let log_numbers = get_log_numbers(&path)?;
        let mut index = HashMap::new();
        let mut readers = HashMap::new();

        for &log_number in &log_numbers {
            let rfile = File::open(log_path(&path, log_number))?;
            let mut reader = BufReader::new(rfile);
            load_index(log_number, &mut index, &mut reader)?;
            readers.insert(log_number, reader);
        }

        let &log_number = log_numbers.last().unwrap_or(&0);
        let writer = new_log_file(&path, log_number, &mut readers)?;

        Ok(Self {
            readers: Arc::new(Mutex::new(readers)),
            writer: Arc::new(Mutex::new(writer)),
            index: Arc::new(Mutex::new(index)),
            log_number: Arc::new(Mutex::new(log_number)),
            path,
            uncompacted_bytes: Arc::new(Mutex::new(0)),
        })
    }

    fn compact(&self) -> Result<()> {
        let mut log_number = self.log_number.lock().unwrap();
        *log_number += 1;
        let mut readers = self.readers.lock().unwrap();
        let mut writer = self.writer.lock().unwrap();

        *writer = new_log_file(&self.path, *log_number, &mut readers)?;
        let mut index = self.index.lock().unwrap();

        for command_pos in &mut index.values_mut() {
            let reader = readers.get_mut(&command_pos.log_number).unwrap();
            reader.seek(SeekFrom::Start(command_pos.offset))?;
            let mut source = reader.take(command_pos.bytes);
            command_pos.log_number = *log_number;
            command_pos.offset = writer.stream_position()?;
            let mut inner = writer.get_mut();
            io::copy(&mut source, &mut inner)?;
        }

        let stale_log_numbers: Vec<u64> = readers
            .keys()
            .filter(|&&number| number < *log_number)
            .cloned()
            .collect();

        for log_number in stale_log_numbers {
            readers.remove(&log_number);
            let log_path = log_path(&self.path, log_number);
            fs::remove_file(log_path)?;
        }

        let mut uncompacted_bytes = self.uncompacted_bytes.lock().unwrap();
        *uncompacted_bytes = 0;

        Ok(())
    }
}

impl KvsEngine for KvStore {
    /// Set the value of a string key to a string. Return an error if the value is not written successfully.
    fn set(&self, key: String, value: String) -> Result<()> {
        let cmd = Command::Set(key.clone(), value);
        let mut writer = self.writer.lock().unwrap();
        let offset = writer.stream_position()?;
        let mut inner = writer.get_mut();
        cmd.serialize(&mut Serializer::new(&mut inner))?;
        let bytes = writer.stream_position()? - offset;
        let mut index = self.index.lock().unwrap();
        let mut uncompacted_bytes = self.uncompacted_bytes.lock().unwrap();
        if let Some(cmd) = index.insert(
            key,
            CommandPosition {
                log_number: *self.log_number.lock().unwrap(),
                offset,
                bytes,
            },
        ) {
            *uncompacted_bytes += cmd.bytes;
        }
        writer.flush()?;

        if *uncompacted_bytes > COMPACTION_THRESHOLD_BYTES {
            self.compact()?;
        }

        Ok(())
    }

    /// Get the string value of a string key. If the key does not exist, return None. Return an error if the value is not read successfully.
    fn get(&self, key: String) -> Result<Option<String>> {
        let index = self.index.lock().unwrap();
        if let Some(pos) = index.get(&key) {
            let mut readers = self.readers.lock().unwrap();
            let mut reader = readers.get_mut(&pos.log_number).unwrap();
            reader.seek(SeekFrom::Start(pos.offset))?;

            let mut des = Deserializer::new(&mut reader);
            match Command::deserialize(&mut des) {
                Ok(Command::Set(_, value)) => Ok(Some(value)),
                Ok(Command::Remove(_)) => Err(KvsError::UnexpectedCommand),
                Err(decode::Error::InvalidMarkerRead(err)) => Err(KvsError::IO(err)),
                Err(err) => Err(KvsError::Decode(err)),
            }
        } else {
            Ok(None)
        }
    }

    /// Remove a given key. Return an error if the key does not exist or is not removed successfully.
    fn remove(&self, key: String) -> Result<()> {
        let mut index = self.index.lock().unwrap();
        if let Some(old_cmd) = index.remove(&key) {
            let cmd = Command::Remove(key.clone());
            let mut writer = self.writer.lock().unwrap();
            let mut inner = writer.get_mut();
            cmd.serialize(&mut Serializer::new(&mut inner))?;
            writer.flush()?;
            let mut uncompacted_bytes = self.uncompacted_bytes.lock().unwrap();
            *uncompacted_bytes += old_cmd.bytes;
            if *uncompacted_bytes > COMPACTION_THRESHOLD_BYTES {
                self.compact()?;
            }
            Ok(())
        } else {
            Err(KvsError::KeyNotFound)
        }
    }
}

fn new_log_file(
    path: &Path,
    new_log_number: u64,
    readers: &mut HashMap<u64, BufReader<File>>,
) -> Result<BufWriter<File>> {
    let log_path = log_path(path, new_log_number);

    let mut wfile = File::options()
        .create(true)
        .write(true)
        .append(true)
        .open(&log_path)?;
    wfile.seek(SeekFrom::End(0))?;
    let writer = BufWriter::new(wfile);
    let rfile = File::open(&log_path)?;
    let reader = BufReader::new(rfile);
    readers.insert(new_log_number, reader);
    Ok(writer)
}
