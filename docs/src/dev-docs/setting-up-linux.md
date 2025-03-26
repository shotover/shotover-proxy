# Linux Specific setup

## Building shotover

Shotover requires a single external dependencies to build on linux.
`gcc` must be installed due to the `aws-lc-sys` and `ring` crate containing some C code.
On Ubuntu you can install it via:

```shell
sudo apt-get install gcc
```

## Integration test dependencies

Building and running integration tests and benchmarks requires many more external dependencies.
To set them up on ubuntu run the script at `shotover-proxy/build/install_ubuntu_deps.sh`.
Inspect the contents of the script to learn what it installs and why, some of the dependencies are optional, so feel free to skip those by installing the rest manually.
If you already have docker installed make sure it is not a rootless install, see `install_ubuntu_deps.sh` for more information.
