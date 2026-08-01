#![forbid(unsafe_code)]

pub mod api;
pub mod catalog;
pub mod config;
pub mod domain;
pub mod gateway;
pub mod health;
pub mod history;
pub mod metrics;
pub mod protocol;
pub mod providers;
pub mod runtime;

pub const SERVICE_NAME: &str = "market-stream-gateway";
