use clap::Parser;
use clap::Subcommand;

use std::net::SocketAddr;

use std::error::Error;
use std::result::Result;

use kvs::KvsClient;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

const DEFAULT_ADDR: &str = "127.0.0.1:4000";
const ADDR_NAME: &str = "IP-PORT";

#[derive(Debug, Subcommand)]
enum Commands {
    /// Set the value of a string key to a string. Print an error and return a non-zero exit code on failure.
    Set {
        key: String,
        value: String,
        #[arg(long, name = ADDR_NAME, default_value = DEFAULT_ADDR)]
        addr: SocketAddr,
    },

    /// Get the string value of a given string key. Print an error and return a non-zero exit code on failure.
    Get {
        key: String,
        #[arg(long, name = ADDR_NAME, default_value = DEFAULT_ADDR)]
        addr: SocketAddr,
    },

    /// Remove a given key. Print an error and return a non-zero exit code on failure.
    #[command(name = "rm")]
    Remove {
        key: String,
        #[arg(long, name = ADDR_NAME, default_value = DEFAULT_ADDR)]
        addr: SocketAddr,
    },
}

fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Set { key, value, addr } => {
            let mut client = KvsClient::connect(&addr)?;
            client.set(key, value)?;
        }
        Commands::Get { key, addr } => {
            let mut client = KvsClient::connect(&addr)?;
            if let Some(value) = client.get(key)? {
                println!("{}", value);
            } else {
                println!("Key not found");
            }
        }
        Commands::Remove { key, addr } => {
            let mut client = KvsClient::connect(&addr)?;
            client.remove(key)?;
        }
    }
    Ok(())
}
