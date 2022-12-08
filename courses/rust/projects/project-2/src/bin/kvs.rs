use clap::Parser;
use clap::Subcommand;

use std::env::current_dir;
use std::error::Error;
use std::process;
use std::result::Result;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Set the value of a string key to a string. Print an error and return a non-zero exit code on failure.
    Set { key: String, value: String },

    /// Get the string value of a given string key. Print an error and return a non-zero exit code on failure.
    Get { key: String },

    /// Remove a given key. Print an error and return a non-zero exit code on failure.
    #[command(name = "rm")]
    Remove { key: String },
}

fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();

    let mut store = kvs::KvStore::open(current_dir()?)?;

    match cli.command {
        Commands::Set { key, value } => {
            store.set(key, value)?;
        }
        Commands::Get { key } => {
            if let Some(value) = store.get(key)? {
                println!("{}", value);
            } else {
                println!("Key not found");
            }
        }
        Commands::Remove { key } => match store.remove(key) {
            Err(kvs::KvStoreError::KeyNotFound) => {
                println!("Key not found");
                process::exit(1);
            }
            Err(err) => {
                return Err(Box::new(err));
            }
            Ok(_) => {}
        },
    }
    //    let mut store = kvs::KvStore::open("test.msg")?;
    Ok(())
}
