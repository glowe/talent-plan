use clap::Parser;
use clap::ValueEnum;

use kvs::KvStore;
use kvs::KvsEngine;
use kvs::KvsServer;
use kvs::SledKvsEngine;
use slog::info;
use slog::o;
use slog::Drain;
use slog::Logger;
use slog_async::Async;
use slog_term::CompactFormat;
use slog_term::TermDecorator;
use std::env::current_dir;
use std::error::Error;
use std::net::SocketAddr;
use std::result::Result;

#[derive(ValueEnum, Clone, Debug)]
enum EngineName {
    Kvs,
    Sled,
}

impl std::fmt::Display for EngineName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self {
            Self::Kvs => write!(f, "kvs"),
            Self::Sled => write!(f, "sled"),
        }
    }
}

// FIXME: define this in another module shared between client and server
const DEFAULT_ADDR: &str = "127.0.0.1:4000";
const ADDR_NAME: &str = "IP-PORT";

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[arg(long, name=ADDR_NAME, default_value=DEFAULT_ADDR)]
    addr: SocketAddr,

    #[arg(long, value_enum, name="ENGINE-NAME", default_value_t=EngineName::Kvs)]
    engine: EngineName,
}

fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();

    let decorator = TermDecorator::new().stderr().build();
    let drain = CompactFormat::new(decorator).build().fuse();
    let drain = Async::new(drain).build().fuse();

    let log = slog::Logger::root(drain, o!());

    info!(log, "starting up"; "version" => env!("CARGO_PKG_VERSION"));
    info!(
        log,
        "using configuration";
        "engine" => cli.engine.to_string(), "ip-port" => cli.addr.to_string()
    );

    match cli.engine {
        EngineName::Kvs => {
            let dir = current_dir()?;
            info!(log, "opening kvs logs"; "directory" => dir.to_str());
            let engine = KvStore::open(dir)?;
            serve(engine, log, &cli.addr)?;
        }
        EngineName::Sled => {
            serve(
                SledKvsEngine::new(sled::open(current_dir()?)?),
                log,
                &cli.addr,
            )?;
        }
    };
    Ok(())
}

fn serve<E: KvsEngine>(engine: E, log: Logger, addr: &SocketAddr) -> Result<(), Box<dyn Error>> {
    let mut server = KvsServer::new(engine, log);
    server.serve(addr)?;
    Ok(())
}
