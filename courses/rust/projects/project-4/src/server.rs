use crate::engines::KvsEngine;
use crate::error::Result;
use crate::protocol::Request;
use crate::protocol::Response;
use crate::thread_pool::NaiveThreadPool;
use crate::thread_pool::ThreadPool;
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

    pub fn serve(&mut self, addr: &SocketAddr) -> Result<()> {
        let listener = TcpListener::bind(addr)?;
        let thread_pool = NaiveThreadPool::new(32)?;
        for result in listener.incoming() {
            let stream = result?;
            let engine = self.engine.clone();
            let log = self.log.clone();
            thread_pool.spawn(move || {
                if let Err(err) = serve(&log, engine, stream) {
                    error!(&log, "failed with error {}", err.to_string())
                }
            })
        }
        Ok(())
    }
}

fn serve<E: KvsEngine>(log: &Logger, engine: E, mut stream: TcpStream) -> Result<()> {
    let request = read_request(&mut stream)?;
    debug!(&log, "request = {:?}", request);
    let mut response = process_request(&engine, request);
    debug!(&log, "response = {:?}", response);
    respond(stream, &mut response)?;
    Ok(())
}

fn read_request(stream: &mut TcpStream) -> Result<Request> {
    let mut reader = Deserializer::new(BufReader::new(stream));
    Ok(Request::deserialize(&mut reader)?)
}

fn process_request<E: KvsEngine>(engine: &E, request: Request) -> Response {
    match request {
        Request::Get(key) => match engine.get(key.clone()) {
            Ok(value) => Response::GetOk(value.clone()),
            Err(err) => Response::Err(err.to_string()),
        },
        Request::Set(key, value) => match engine.set(key, value) {
            Ok(()) => Response::SetOk(()),
            Err(err) => Response::Err(err.to_string()),
        },
        Request::Remove(key) => match engine.remove(key) {
            Ok(()) => Response::RemoveOk(()),
            Err(err) => Response::Err(err.to_string()),
        },
    }
}

fn respond(stream: TcpStream, response: &mut Response) -> Result<()> {
    let mut writer = Serializer::new(BufWriter::new(&stream));
    response.serialize(&mut writer)?;
    writer.get_mut().flush()?;
    Ok(())
}
