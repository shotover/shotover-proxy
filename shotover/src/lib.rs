//! This library allows the creation of custom shotover transforms.
//!
//! There are two consumers of this library:
//! ## Custom Transforms
//!
//! To create a custom transform you need to implement these traits:
//! * [`transforms::TransformConfig`] - Defines what configuration fields the transform has in the `topology.yaml`.
//! * [`transforms::TransformBuilder`] - Defines how to build the Transform for a new incoming connection. Only one instance is created over shotovers runtime.
//! * [`transforms::Transform`] - Defines the transformation logic of the transform. A new instance is created per incoming connection.
//!
//! Simple transforms can implement all of these onto a single struct but generally you need seperate structs for each.
//!
//! ## The shotover binary
//! All custom transforms the user wants to use are statically compiled into a single binary.
//! The crate for this binary is very simple, it just consists of a `main.rs` like:
//!
//! ```no_run
//! # mod transform_crate {
//! # pub type TransformConfig = shotover::transforms::null::NullSinkConfig;
//! # }
//! shotover::import_transform!(transform_crate::TransformConfig);
//!
//! fn main() {
//!     shotover::runner::Shotover::new().run_block();
//! }
//! ```

#![deny(
    unsafe_code,
    reason = r#"
If we absolutely need unsafe code, it should be isolated within a separate small crate that exposes a sound safe API.
"sound" means that it is impossible for any interaction with the public API of the crate to violate an unsafe invariant which causes UB."#
)]
#![deny(
    clippy::print_stdout,
    reason = "Accidentally printing would break json log output"
)]
#![deny(
    clippy::print_stderr,
    reason = "Accidentally printing would break json log output."
)]
// allow some clippy lints that we disagree with
// Allow dead code if any of the protocol features are disabled
#![cfg_attr(
    any(
        not(feature = "cassandra"),
        not(feature = "valkey"),
        not(feature = "kafka"),
        not(feature = "opensearch"),
    ),
    allow(dead_code, unused_imports, unused_variables, unused_mut)
)]
#[cfg(all(
    not(feature = "cassandra"),
    not(feature = "valkey"),
    not(feature = "kafka"),
    not(feature = "opensearch"),
))]
compile_error!(
    "At least one protocol feature must be enabled, e.g. `cassandra`, `valkey`, `kafka` or `opensearch`"
);

pub mod codec;
pub mod config;
pub mod connection;
mod connection_span;
pub mod frame;
pub mod hot_reload;
pub mod hot_reload_server;
mod http;
pub mod message;
mod observability;
pub mod runner;
mod server;
pub mod sources;
pub mod tcp;
pub mod tls;
mod tracing_panic_handler;
pub mod transforms;

/// Imports a custom transform into the shotover binary.
///
/// When a custom transform is defined in its own crate, typetag wont kick in unless there is some kind of `use crate_name::CustomTransformConfig`.
/// This macro does that for you while making it clear that the `use` is a little bit magic.
/// It also performs some type checks to ensure that you are actually importing an implementer of [`transforms::TransformConfig`].
#[macro_export]
macro_rules! import_transform {
    ($ty:ty) => {
        // import the type, this is required for typetag to pick up the type
        use $ty;

        // assert that the type actually implements TransformConfig, this prevents the easy mistake of accidentally importing the Transform instead of the TransformConfig
        const _: fn() = || {
            // Only callable when `$ty` implements `TransformConfig`
            fn assert_impls_transform_config<T: ?Sized + shotover::transforms::TransformConfig>() {}
            assert_impls_transform_config::<$ty>();
        };
    };
}
