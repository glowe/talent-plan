use clap::Parser;
use clap::ValueEnum;

use kvs::KvStore;
use kvs::KvsEngine;
use kvs::KvsServer;
use kvs::SledKvsEngine;
use slog::error;
use slog::info;
use slog::o;
use slog::Drain;
use slog::Logger;
use slog_async::Async;
use slog_term::CompactFormat;
use slog_term::TermDecorator;
use std::env::current_dir;
use std::error::Error;
use std::fmt;
use std::net::SocketAddr;
use std::result::Result;
use std::str::FromStr;

#[derive(ValueEnum, Clone, Debug, PartialEq)]
enum EngineName {
    Kvs,
    Sled,
}

impl fmt::Display for EngineName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> fmt::Result {
        match &self {
            Self::Kvs => write!(f, "kvs"),
            Self::Sled => write!(f, "sled"),
        }
    }
}

#[derive(Debug)]
struct ParseEngineNameError(String);

impl fmt::Display for ParseEngineNameError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Unrecogized engine name: {}", self.0)
    }
}

impl Error for ParseEngineNameError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        None
    }
}

impl FromStr for EngineName {
    type Err = ParseEngineNameError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        match input {
            "kvs" => Ok(Self::Kvs),
            "sled" => Ok(Self::Sled),
            val => Err(ParseEngineNameError(val.to_string())),
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

    let current_dir = current_dir()?;
    let engine_file = current_dir.join("kvs.engine");

    let last_engine = if !engine_file.exists() {
        None
    } else {
        Some(std::fs::read_to_string(&engine_file)?.parse::<EngineName>()?)
    };

    if last_engine.is_some() && last_engine != Some(cli.engine.clone()) {
        error!(
            log,
            "{} was chosen, but last engine was {}; quitting!",
            last_engine.unwrap(),
            cli.engine
        );
        log.fuse();
        std::process::exit(1);
    }

    std::fs::write(&engine_file, format!("{}", cli.engine))?;

    match cli.engine {
        EngineName::Kvs => {
            info!(log, "kvs store"; "directory" => current_dir.to_str());
            let engine = KvStore::open(current_dir)?;
            serve(engine, log, &cli.addr)?;
        }
        EngineName::Sled => {
            info!(log, "sled engine"; "directory" => current_dir.to_str());
            serve(SledKvsEngine::new(sled::open(current_dir)?), log, &cli.addr)?;
        }
    };
    Ok(())
}

fn serve<E: KvsEngine>(engine: E, log: Logger, addr: &SocketAddr) -> Result<(), Box<dyn Error>> {
    let mut server = KvsServer::new(engine, log);
    server.serve(addr)?;
    Ok(())
}
