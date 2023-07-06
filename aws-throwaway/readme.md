# aws-throwaway

An aws-sdk wrapper to spin up temporary resources.
It was developed for the use case of benchmarking.

## Example

aws-throwaway makes it trivial to spin up an instance, interact with it, and then destroy it.

```rust
let aws = Aws::new().await;

let instance = aws.create_ec2_instance(InstanceType::T2Micro).await;
let output = instance.ssh().shell("echo 'Hello world!'").await;
println!("output from ec2 instance: {}", output.stdout);

aws.cleanup_resources().await;
```

Refer to `examples/aws-throwaway-example.rs` for more usage.

## Aws credentials

Refer to the [aws-sdk docs](https://docs.aws.amazon.com/sdk-for-rust/latest/dg/credentials.html) for full information on credentials.

But two easiest ways are:

* Setting the `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` environment variables
* Logging in from the AWS CLI

### Running the example

To run `examples/aws-throwaway-example` in this repository, create a script like the following *OUTSIDE* of any repository.

```bash
cd shotover-proxy/aws-throwaway
AWS_ACCESS_KEY_ID=TODO AWS_SECRET_ACCESS_KEY=TODO cargo run --example aws-throwaway-example "$@"
```

Replace `TODO` with your credentials.

To see a thorough log of all interactions with EC2, write your script such that it sets `RUST_LOG=info` as well.

## Under the hood

Usual "intrastructure as code" projects like terraform attempt to individually track every resource it creates.
This tracking is stored locally on your machine or in some kind of shared repository.
This makes sense when you have important production data on the line.
But when you are working with resources that contain nothing of value and will soon be destroyed, that approach is needlessly brittle and leaves it easy to have costly unaccounted for AWS resources.

Rather than attempting to individually track each resource created like terraform does, aws-throwaway attaches a single user unique [AWS tag](https://docs.aws.amazon.com/tag-editor/latest/userguide/tagging.html) to every resource it creates allowing it destroy all its resources without depending on local state.

Consider this snippet from the example earlier:

```rust
let aws = Aws::new().await;
let instance = aws.create_ec2_instance().await;
```

Behind the scenes this creates various kinds of AWS resources. e.g. keypairs, security groups, ec2 instances
aws-throwaway attaches an AWS tag to all of these resources it creates.
All of these resources have an AWS tag with the hardcoded key `aws-throwaway-23c2d22c-d929-43fc-b2a4-c1c72f0b733f:user` and value of the user's iam user name.
The uuid in the key was generated once to ensure uniqueness and is now hardcoded.

Then consider this snippet from the example:

```rust
aws.cleanup_resources().await;
```

This will destroy all AWS resources that have the AWS tag specified above.
Thie means that aws-throwaway will delete all resources created through this IAM user.
Even resources created by aws-throwaway by the same user in a different application.

## Cleanup points

It is recommended to cleanup your resources at 3 points:

1. When you start your application. This is actually done automatically for you by `Aws::new()`.
2. When your application finishes. You should manually call `aws.cleanup_resources()`
3. You should provide a cli flag (or similar) in your application that specifically triggers cleanup and nothing else. e.g. `application --cleanup-aws-resources` You can call `Aws::cleanup_resources_static()` to achieve this.

If your program never crashed then you could get away with only the 2nd point.
But that is impossible, so we have the 1st point which is useful to automatically cleanup missed resources on rerun and the 3rd point to cleanup resources when rerunning the application is undesirable.

## Security

In order to authenticate the ec2 instance, ssh keys are generated locally and uploaded via EC2 user_data field.
This means that any process on the machine that can run an http request can fetch the ssh keys.
This should not pose a problem for its intended use case, but you should never use aws-throwaway created instances to host any kind of workload that would expose such functionality publically.
For more information about this approach see: <https://alestic.com/2012/04/ec2-ssh-host-key/>
