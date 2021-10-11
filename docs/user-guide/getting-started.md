# Getting Started

## Quick Setup

1. **Download** - You can find the latest release of Shotover Proxy at our github [release page](https://github.com/shotover/shotover-proxy/releases).
2. **Extract** - Extract the downloaded tarball using your favourite archive tool e.g. ```tar -xzvf shotover-proxy-0.0.1.tar.gz```
3. **Configure** - Most releases contain the shotover binary named `shotover-proxy` and a configuration file. You can run shotover proxy from where
you extracted it, or you may choose to place it in your path, or create a service for it. For the moment these are left as an
exercise for the reader. To configure shotover, modify the included `config.yml` file. E.g. ```vim config.yml```
4. **Run** - To start shotover-proxy, start with the following command: ```./shotover-proxy``` 
5. **Alternate Run** - to start shotover-proxy with a specific configuration file, start with the following command:```./shotover-proxy --config-file config.yml```

To get more information about command line parameters you can pass to shotover:

```console
./shotover-proxy --help
```

## Deployment scenarios

For in depth guides to common deployment scenarios see the following examples:

* [Redis clustering](../examples/redis-clustering.md)
