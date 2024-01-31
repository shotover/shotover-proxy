# Shotover windsock

## Running locally

Just `cargo windsock` will run every bench.
Refer to the windsock docs and `cargo windsock --help` for more flags.

## Running in AWS

First ensure you have the [AWS CLI V2 installed locally](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html).

### Aws credentials

Refer to the [aws-sdk docs](https://docs.aws.amazon.com/sdk-for-rust/latest/dg/credentials.html) for full information on credentials.

But two easiest ways are:

* Setting the `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` environment variables
* Logging in from the AWS CLI

### Script setup

To run the shotover windsock benches in AWS through the environment variables, create a script like the following *OUTSIDE* of any repository.

```bash
cd shotover-proxy
AWS_ACCESS_KEY_ID=TODO AWS_SECRET_ACCESS_KEY=TODO cargo windsock "$@"
```

Replace `TODO` with your credentials.

Then invoke like `aws-windsock.sh your-windsock-flags-here`
