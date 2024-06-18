# macOS Specific Setup

## Building shotover

There are no external dependencies required for building shotover on macOS.

## Integration test dependencies

To run the tests capable of running on macOS, install the following dependencies:

```shell
brew install --cask docker
brew install openssl@3
brew install chipmk/tap/docker-mac-net-connect
sudo brew services start chipmk/tap/docker-mac-net-connect
```

Make sure that docker desktop is running when you run the tests.

To continue running tests after a reboot, you will need to rerun:

```shell
sudo brew services start chipmk/tap/docker-mac-net-connect
```
