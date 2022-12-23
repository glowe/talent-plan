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

use super::KvsEngine;

use crate::KvsError;
use crate::Result;

struct CommandPosition {
    log_number: u64,
    offset: u64,
    bytes: u64,
}

pub struct KvStore {
    readers: HashMap<u64, BufReader<File>>,
    writer: BufWriter<File>,
    index: HashMap<String, CommandPosition>,
    log_number: u64,
    path: PathBuf,
    uncompacted_bytes: u64,
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
            readers,
            writer,
            index,
            log_number,
            path,
            uncompacted_bytes: 0,
        })
    }

    fn compact(&mut self) -> Result<()> {
        self.log_number += 1;
        self.writer = new_log_file(&self.path, self.log_number, &mut self.readers)?;

        for command_pos in &mut self.index.values_mut() {
            let reader = self.readers.get_mut(&command_pos.log_number).unwrap();
            reader.seek(SeekFrom::Start(command_pos.offset))?;
            let mut source = reader.take(command_pos.bytes);
            command_pos.log_number = self.log_number;
            command_pos.offset = self.writer.stream_position()?;
            io::copy(&mut source, &mut self.writer)?;
        }

        let stale_log_numbers: Vec<u64> = self
            .readers
            .keys()
            .filter(|&&log_number| log_number < self.log_number)
            .cloned()
            .collect();

        for log_number in stale_log_numbers {
            self.readers.remove(&log_number);
            let log_path = log_path(&self.path, log_number);
            fs::remove_file(log_path)?;
        }

        self.uncompacted_bytes = 0;

        Ok(())
    }
}

impl KvsEngine for KvStore {
    /// Set the value of a string key to a string. Return an error if the value is not written successfully.
    fn set(&mut self, key: String, value: String) -> Result<()> {
        let cmd = Command::Set(key.clone(), value);
        let offset = self.writer.stream_position()?;
        cmd.serialize(&mut Serializer::new(&mut self.writer))?;
        let bytes = self.writer.stream_position()? - offset;
        if let Some(cmd) = self.index.insert(
            key,
            CommandPosition {
                log_number: self.log_number,
                offset,
                bytes,
            },
        ) {
            self.uncompacted_bytes += cmd.bytes;
        }
        self.writer.flush()?;

        if self.uncompacted_bytes > COMPACTION_THRESHOLD_BYTES {
            self.compact()?;
        }

        Ok(())
    }

    /// Get the string value of a string key. If the key does not exist, return None. Return an error if the value is not read successfully.
    fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(pos) = self.index.get(&key) {
            let mut reader = self.readers.get_mut(&pos.log_number).unwrap();
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
    fn remove(&mut self, key: String) -> Result<()> {
        if let Some(old_cmd) = self.index.remove(&key) {
            let cmd = Command::Remove(key.clone());
            cmd.serialize(&mut Serializer::new(&mut self.writer))?;
            self.writer.flush()?;
            self.uncompacted_bytes += old_cmd.bytes;
            if self.uncompacted_bytes > COMPACTION_THRESHOLD_BYTES {
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
