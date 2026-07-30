use clap::Parser;
use std::process::ExitCode;

#[derive(Debug, Parser)]
#[command(name = "mamcli", version)]
struct Cli {}

pub fn run() -> ExitCode {
    let _cli = Cli::parse();
    ExitCode::SUCCESS
}
