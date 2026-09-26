//! Supervise long-running agent and general task workloads and report them to a Codex thread
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

pub mod agents;
pub mod callback;
pub mod cancellation;
pub mod cli;
pub mod client;
pub mod config;
pub mod container;
pub mod daemon;
pub mod digest;
pub mod domain;
pub mod error;
pub mod events;
pub mod files;
pub mod fleet;
pub mod home;
pub mod install;
pub mod invocation;
pub mod machine;
pub mod message;
pub mod report;
pub mod resource;
pub mod runner;
pub mod spec;
pub mod store;
pub mod submission;
pub mod thread_title;
pub mod update;

#[cfg(test)]
mod dispatcher_tests;

pub use domain::API_VERSION;
