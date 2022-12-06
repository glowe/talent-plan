use clap::Parser;
use clap::Subcommand;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Set the value of a string key to a string
    Set { key: String, value: String },
    /// Get the string value of a given string key
    Get { key: String },
    /// Remove a given key
    #[command(name = "rm")]
    Remove { key: String },
}

fn main() {
    let cli = Cli::parse();

    // You can check for the existence of subcommands, and if found use their
    // matches just as you would the top level cmd
    match &cli.command {
        _ => {
            panic!("unimplemented");
        }
    }
}
