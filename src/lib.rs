//! Core library for the `mater` CLI.
//!
//! This crate is structured as a small CLI tool.

pub mod cli;
pub mod db;
pub mod model;
pub mod state;
pub mod tools;

pub use cli::run;
