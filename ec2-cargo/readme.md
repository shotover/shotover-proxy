# ec2-cargo

ec2-cargo is a small tool for running shotover tests on an EC2 instance.

## AWS credentials

Refer to the [aws-sdk docs](https://docs.aws.amazon.com/sdk-for-rust/latest/dg/credentials.html) for full information on credentials.

But two easiest ways are:

* Setting the `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` environment variables
* Logging in from the AWS CLI

## Script setup

To run ec2-cargo authenticated with AWS through environment variables, create a script like the following *OUTSIDE* of any repository.

```bash
cd shotover-proxy/ec2-cargo
AWS_ACCESS_KEY_ID=TODO AWS_SECRET_ACCESS_KEY=TODO cargo run "$@"
```

Replace `TODO` with your credentials.

Then invoke like `ec2-cargo.sh your-flags-here`

## Running

ec2-cargo has reasonable default configurations so you can just:

* `cargo run` the project to use default credentials
* `ec2-cargo.sh` to use specific env vars as described above.

You can also specify the instance type e.g. `ec2-cargo.sh --instance-type c7g.2xlarge`

## Usage

While the tool is running it will present you with a shell in which to enter commands.

Some possible commands are:

* `test $args` - Uploads shotover project source code and runs `cargo nextest run $args`
* `ssh-instructions` - Print a bash snippet that can be used to ssh into the machine
* `exit` - Exits the shell and terminates the EC2 instance.
* `help` - Display all possible commands
