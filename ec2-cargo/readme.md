# ec2-cargo

ec2-cargo is a small tool for running shotover tests on an EC2 instance.

Instance state is persisted to disk between invocations, so each command is a short-lived process that connects, does its work, and exits.

## AWS credentials

Refer to the [aws-sdk docs](https://docs.aws.amazon.com/sdk-for-rust/latest/dg/credentials.html) for full information on credentials.

But two easiest ways are:

* Setting the `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` environment variables
* Logging in from the AWS CLI

## Script setup

To run ec2-cargo authenticated with AWS through environment variables, create a script like the following *OUTSIDE* of any repository.

```bash
cd shotover-proxy/ec2-cargo
AWS_ACCESS_KEY_ID=TODO AWS_SECRET_ACCESS_KEY=TODO cargo run -- "$@"
```

Replace `TODO` with your credentials.

Then invoke like `ec2-cargo.sh your-flags-here`

## Usage

```bash
# Create and provision an EC2 instance (one-time setup)
# Before creating the new instance, any existing ec2 instances created by ec2-cargo will be destroyed
cargo run -- create
# Or,
# cargo run -- create --instance-type c7g.2xlarge

# Run tests (can be called repeatedly against the same instance)
cargo run -- test my_test_name
cargo run -- test --test-threads=1

# Run windsock benchmarks
cargo run -- windsock $args
cargo run -- windsock-kafka $args
cargo run -- windsock-cassandra $args
cargo run -- windsock-valkey $args

# Print SSH connection instructions
cargo run -- ssh-instructions

# Tear down all AWS resources and delete the persisted state
cargo run -- cleanup
```

Or using the script setup: `ec2-cargo.sh create`, `ec2-cargo.sh test my_test_name`, etc.
