use crate::error::KvsError;
use crate::error::Result;
use crate::protocol::Request;
use crate::protocol::Response;
use rmp_serde::decode::Deserializer;
use rmp_serde::decode::ReadReader;
use rmp_serde::encode::Serializer;
use serde::Deserialize;
use serde::Serialize;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Write;
use std::net::SocketAddr;
use std::net::TcpStream;

pub struct KvsClient {
    reader: Deserializer<ReadReader<BufReader<TcpStream>>>,
    writer: Serializer<BufWriter<TcpStream>>,
}

impl KvsClient {
    pub fn connect(addr: &SocketAddr) -> Result<Self> {
        let reader_stream = TcpStream::connect(addr)?;
        let writer_stream = reader_stream.try_clone()?;

        let reader = Deserializer::new(BufReader::new(reader_stream));
        let writer = Serializer::new(BufWriter::new(writer_stream));
        Ok(Self { reader, writer })
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        let cmd = Request::Get(key);
        cmd.serialize(&mut self.writer)?;
        self.writer.get_mut().flush()?;
        match Response::deserialize(&mut self.reader)? {
            Response::GetOk(value) => Ok(value),
            Response::Err(msg) => Err(KvsError::StringError(msg)),
            _ => Err(KvsError::UnexpectedResponse),
        }
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let cmd = Request::Set(key, value);
        cmd.serialize(&mut self.writer)?;
        self.writer.get_mut().flush()?;
        match Response::deserialize(&mut self.reader)? {
            Response::SetOk(()) => Ok(()),
            Response::Err(msg) => Err(KvsError::StringError(msg)),
            _ => Err(KvsError::UnexpectedResponse),
        }
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        let cmd = Request::Remove(key);
        cmd.serialize(&mut self.writer)?;
        self.writer.get_mut().flush()?;
        match Response::deserialize(&mut self.reader)? {
            Response::RemoveOk(()) => Ok(()),
            Response::Err(msg) => Err(KvsError::StringError(msg)),
            _ => Err(KvsError::UnexpectedResponse),
        }
    }
}
