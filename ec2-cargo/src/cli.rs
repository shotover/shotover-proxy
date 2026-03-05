use clap::{Parser, Subcommand};

/// A tool for running cargo commands on an EC2 instance.
///
/// Create an instance with `create`, run commands against it with `test`/`windsock`/etc,
/// and tear it down with `cleanup`. Instance state is persisted to disk between invocations.
#[derive(Parser)]
#[clap()]
pub struct Args {
    #[command(subcommand)]
    pub command: Cmd,
}

#[derive(Subcommand)]
pub enum Cmd {
    /// Create and provision an EC2 instance, persisting node address+credentials to disk for other commands to use.
    Create {
        /// Specify the instance type to provision.
        #[clap(long, default_value = "c7g.2xlarge")]
        instance_type: String,
    },

    /// Upload changes and run `cargo nextest run <args>`.
    Test {
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        args: Vec<String>,
    },

    /// Upload changes and run `cargo windsock <args>`. Windsock results are downloaded to target/windsock_data.
    Windsock {
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        args: Vec<String>,
    },

    /// Upload changes and run `cargo windsock-kafka <args>`. Windsock results are downloaded to target/windsock_data.
    WindsockKafka {
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        args: Vec<String>,
    },

    /// Upload changes and run `cargo windsock-cassandra <args>`. Windsock results are downloaded to target/windsock_data.
    WindsockCassandra {
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        args: Vec<String>,
    },

    /// Upload changes and run `cargo windsock-valkey <args>`. Windsock results are downloaded to target/windsock_data.
    WindsockValkey {
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        args: Vec<String>,
    },

    /// Print a bash snippet that can be used to ssh into the machine.
    SshInstructions,

    /// Cleanup all AWS resources and delete the persisted state file.
    /// This includes resources created by ec2-cargo and resources created by windsock, as they both use the same aws-throwaway library.
    /// But note that windsock's cleanup logic avoids cleaning up ec2-cargo resources.
    Cleanup,
}
