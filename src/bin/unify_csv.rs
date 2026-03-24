use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;

use mater::tools::unify_csv;

#[derive(Parser)]
#[command(author, version, about = "Convert tracking CSV files into a single unified CSV.")]
struct Args {
    /// Directory containing source CSV files.
    #[arg(long, default_value = "../tracking")]
    input: PathBuf,

    /// Output unified CSV file.
    #[arg(long, default_value = "../tracking/unified.csv")]
    output: PathBuf,
}

fn main() -> Result<()> {
    let args = Args::parse();
    unify_csv(&args.input, &args.output)
}
