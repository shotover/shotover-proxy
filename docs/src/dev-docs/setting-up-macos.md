# macOS Specific Setup

## Building shotover

There are no external dependencies required for building shotover on macOS.

## Integration test dependencies

To run the tests capable of running on macOS, install the following dependencies:

```shell
brew install --cask docker
brew install openssl@3
```

Make sure that docker desktop is running when you run the tests.
