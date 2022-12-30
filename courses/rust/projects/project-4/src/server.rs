use std::net::SocketAddr;
use std::net::TcpListener;

use crate::engines::KvsEngine;

use crate::error::KvsError;
use crate::error::Result;
use crate::protocol::GetResponse;
use crate::protocol::RemoveResponse;
use crate::protocol::Request;
use crate::protocol::SetResponse;
use rmp_serde::Deserializer;
use rmp_serde::Serializer;
use serde::Deserialize;
use serde::Serialize;
use slog::debug;
use slog::error;
use slog::Logger;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Write;
use std::net::TcpStream;

pub struct KvsServer<E: KvsEngine> {
    engine: E,
    log: Logger,
}

impl<E: KvsEngine> KvsServer<E> {
    pub fn new(engine: E, log: Logger) -> Self {
        Self { engine, log }
    }

    pub fn serve(&mut self, addr: &SocketAddr) -> Result<()> {
        let listener = TcpListener::bind(addr)?;
        for stream in listener.incoming() {
            debug!(self.log, "received connection");
            match stream {
                Ok(stream) => {
                    let mut reader = Deserializer::new(BufReader::new(&stream));
                    let writer = Serializer::new(BufWriter::new(&stream));
                    match Request::deserialize(&mut reader) {
                        Ok(Request::Get(key)) => {
                            self.handle_get_request(writer, key)?;
                        }
                        Ok(Request::Set(key, value)) => {
                            self.handle_set_request(writer, key, value)?;
                        }
                        Ok(Request::Remove(key)) => {
                            self.handle_remove_request(writer, key)?;
                        }
                        Err(err) => return Err(KvsError::Decode(err)),
                    }
                }
                Err(err) => {
                    error!(self.log, "error with incoming connection: {}", err);
                }
            }
        }
        Ok(())
    }

    fn handle_get_request(
        &mut self,
        mut writer: Serializer<BufWriter<&TcpStream>>,
        key: String,
    ) -> Result<()> {
        match self.engine.get(key.clone()) {
            Ok(value) => {
                let response = GetResponse::Ok(value.clone());
                response.serialize(&mut writer)?;
                writer.get_mut().flush()?;
                debug!(self.log, "get({}) = {:?}", key, value);
            }
            Err(err) => {
                let response = GetResponse::Err(err.to_string());
                response.serialize(&mut writer)?;
                writer.get_mut().flush()?;
            }
        }
        Ok(())
    }

    fn handle_set_request(
        &mut self,
        mut writer: Serializer<BufWriter<&TcpStream>>,
        key: String,
        value: String,
    ) -> Result<()> {
        debug!(self.log, "set({}, {})", key, value);
        match self.engine.set(key, value) {
            Ok(()) => {
                let response = SetResponse::Ok(());
                response.serialize(&mut writer)?;
                writer.get_mut().flush()?;
            }
            Err(err) => {
                let response = GetResponse::Err(err.to_string());
                response.serialize(&mut writer)?;
                writer.get_mut().flush()?;
            }
        }
        Ok(())
    }

    fn handle_remove_request(
        &mut self,
        mut writer: Serializer<BufWriter<&TcpStream>>,
        key: String,
    ) -> Result<()> {
        debug!(self.log, "remove({})", key);
        match self.engine.remove(key) {
            Ok(()) => {
                let response = RemoveResponse::Ok(());
                response.serialize(&mut writer)?;
                writer.get_mut().flush()?;
            }
            Err(err) => {
                let response = RemoveResponse::Err(err.to_string());
                response.serialize(&mut writer)?;
                writer.get_mut().flush()?;
            }
        }
        Ok(())
    }
}
