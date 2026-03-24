use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;

use mater::tools::convert_csv;

#[derive(Parser)]
#[command(
    author,
    version,
    about = "Convert tracking YAML/JSON data files to CSV."
)]
struct Args {
    /// Directory containing source YAML/JSON data files.
    #[arg(long, default_value = "../tracking")]
    input: PathBuf,
}

fn main() -> Result<()> {
    let args = Args::parse();
    convert_csv(&args.input)
}
