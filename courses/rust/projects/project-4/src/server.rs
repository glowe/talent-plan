use crate::engines::KvsEngine;
use crate::error::Result;
use crate::protocol::Request;
use crate::protocol::Response;
use rmp_serde::Deserializer;
use rmp_serde::Serializer;
use serde::Deserialize;
use serde::Serialize;
use slog::debug;
use slog::Logger;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Write;
use std::net::SocketAddr;
use std::net::TcpListener;
use std::net::TcpStream;

pub struct KvsServer<E: KvsEngine> {
    engine: E,
    log: Logger,
}

impl<E: KvsEngine> KvsServer<E> {
    pub fn new(engine: E, log: Logger) -> Self {
        Self { engine, log }
    }

    fn read_request(&mut self, stream: &mut TcpStream) -> Result<Request> {
        let mut reader = Deserializer::new(BufReader::new(stream));
        Ok(Request::deserialize(&mut reader)?)
    }

    fn process_request(&mut self, request: Request) -> Response {
        match request {
            Request::Get(key) => match self.engine.get(key.clone()) {
                Ok(value) => Response::GetOk(value.clone()),
                Err(err) => Response::Err(err.to_string()),
            },
            Request::Set(key, value) => match self.engine.set(key, value) {
                Ok(()) => Response::SetOk(()),
                Err(err) => Response::Err(err.to_string()),
            },
            Request::Remove(key) => match self.engine.remove(key) {
                Ok(()) => Response::RemoveOk(()),
                Err(err) => Response::Err(err.to_string()),
            },
        }
    }

    fn respond(&mut self, stream: TcpStream, response: &mut Response) -> Result<()> {
        let mut writer = Serializer::new(BufWriter::new(&stream));
        response.serialize(&mut writer)?;
        writer.get_mut().flush()?;
        Ok(())
    }

    pub fn serve(&mut self, addr: &SocketAddr) -> Result<()> {
        let listener = TcpListener::bind(addr)?;
        for result in listener.incoming() {
            debug!(self.log, "received connection");
            let mut stream = result?;
            let request = self.read_request(&mut stream)?;
            let mut response = self.process_request(request);
            self.respond(stream, &mut response)?;
        }
        Ok(())
    }
}
