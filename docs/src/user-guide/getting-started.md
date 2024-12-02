# Getting Started

## Setup

1. **Download & Extract** - You can find the latest release of Shotover Proxy at our GitHub [release page](https://github.com/shotover/shotover-proxy/releases). So download and extract from there onto your Linux machine. Alternatively you can [build and run from source](../dev-docs/contributing.md).
2. **Run** - `cd` into the extracted `shotover` folder and run `./shotover-proxy`. Shotover will launch and display some logs.
3. **Examine Config** - Shotover has two configuration files:
    * `config/config.yaml` - This is used to configure logging and metrics.
    * `config/topology.yaml` - This defines how Shotover receives, transforms and delivers messages.
4. **Configure topology** - Open `topology.yaml` in your text editor and edit it to define the sources and transforms you need, the comments in the file will direct you to suitable documentation. Alternatively you can refer to the [Deployment Scenarios](#deployment-scenarios) section for full `topology.yaml` examples.
5. **Rerun** - Shotover currently doesn't support hot-reloading config, so first shut it down with `CTRL-C`. Then rerun `./shotover-proxy` for your new config to take effect.
6. **Test** - Send a message to Shotover as per your configuration and observe it is delivered to it's configured destination database.

To see Shotover's command line arguments run: `./shotover-proxy --help`

## Deployment scenarios

Full `topology.yaml` examples configured for a specific use case:

* [valkey clustering](../examples/valkey-clustering-unaware.md)
