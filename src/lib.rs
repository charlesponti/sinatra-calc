#![deny(missing_docs, unsafe_code)]

//! Geo CLI library for geolocation lookups and CSV geocoding.
//!
//! The binary entry point is intentionally thin so the command implementation
//! can be tested as a library.

mod cli;

/// Runs the CLI and dispatches to the selected subcommand.
pub use cli::run;
