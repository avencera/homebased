//! Supervise long-running agent tasks and report them to a Codex thread.
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

pub mod agents;
pub mod callback;
pub mod cli;
pub mod client;
pub mod daemon;
pub mod domain;
pub mod error;
pub mod home;
pub mod install;
pub mod report;
pub mod runner;
pub mod spec;
pub mod store;

pub use domain::API_VERSION;
