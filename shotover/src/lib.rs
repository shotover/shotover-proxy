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
//!

// Accidentally printing would break json log output
#![deny(clippy::print_stdout)]
#![deny(clippy::print_stderr)]
#![allow(clippy::needless_doctest_main)]

pub mod codec;
mod config;
pub mod frame;
pub mod message;
mod observability;
pub mod runner;
mod server;
mod sources;
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
