//! The data layer proxy.
//!
//! Below are the main areas that you should be looking at to extend and work with shotover.
//!
//! Creating a transform is largely just implementing the [`transforms::Transform`] trait and registering it with
//! the [`transforms::Transforms`] enum.
//!
//! To allow your [`transforms::Transform`] to be configurable in shotover config files you will need to create
//! a serializable config struct that implements the [`transforms::TransformsFromConfig`] trait and register it in
//! the [`transforms::TransformsConfig`] enum (note the enum has an `s` after Transform).
//!
//! ## Messages
//! * [`message::Message`], the main struct that carries database queries/frames around in shotover.
//! * [`message::MessageDetails`], an enriched struct that contains computed, easy to work with information about the message
//!
//! ## Transform
//! * [`transforms::Wrapper`], used to wrap messages as they traverse the Transform chain.
//! * [`transforms::Transform`], the main extension trait for adding your own behaviour to shotover.
//! * [`transforms::TransformsFromConfig`], the main extension trait for configuring your own transform.
//! * [`transforms::Transforms`], the enum to register with (add an invariant) for enabling your own transform.
//! * [`transforms::TransformsConfig`], the enum to register with (add an invariant) for configuring your own transform.

mod admin;
mod concurrency;
pub mod config;
pub mod error;
pub mod message;
pub mod protocols;
pub mod runner;
mod server;
pub mod sources;
pub mod tls;
pub mod transforms;
