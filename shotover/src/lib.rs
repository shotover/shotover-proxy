//! The data layer proxy.
//!
//! Below are the main areas that you should be looking at to extend and work with Shotover.
//!
//! Creating a transform is largely just implementing the [`transforms::Transform`] trait and registering it with
//! the [`transforms::Transforms`] enum.
//!
//! To allow your [`transforms::Transform`] to be configurable in Shotover config files you will need to create
//! a serializable config struct and implement [`transforms::TransformConfig`] trait.
//!
//! ## Messages
//! * [`message::Message`], the main struct that carries database queries/frames around in Shotover.
//!
//! ## Transform
//! * [`transforms::Wrapper`], used to wrap messages as they traverse the [`transforms::Transform`] chain.
//! * [`transforms::Transform`], the main extension trait for adding your own behaviour to Shotover.
//! * [`transforms::Transforms`], the enum to register with (add a variant) for enabling your own transform.
//! * [`transforms::TransformConfig`], the trait to implement for configuring your own transform.

// Accidentally printing would break json log output
#![deny(clippy::print_stdout)]
#![deny(clippy::print_stderr)]

pub mod codec;
mod config;
pub mod error;
pub mod frame;
pub mod message;
pub mod message_value;
mod observability;
pub mod runner;
mod server;
mod sources;
pub mod tcp;
pub mod tls;
mod tracing_panic_handler;
pub mod transforms;

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
