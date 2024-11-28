# Writing Custom Transforms

Shotover supports implementing your own custom transforms.
Shotover exposes an API via a rust crate from which you can both implement your own transforms and build those transforms into a final shotover binary that can be run in production.

## Required Knowledge

This approach is taken to make the most of rust's speed and type safety.
However this does come at an up front cost of you needing a basic familiarity with rust.
If you have never worked with rust before, you should first spend a day [familiarising yourself with it](https://doc.rust-lang.org/book/title-page.html).

## Start with the template

To get started writing custom transforms first clone this template:

```shell
git clone https://github.com/shotover/shotover-custom-transforms-template
```

The template comes with:

* two example transforms: the `valkey-get-rewrite` and `kafka-fetch-rewrite` crates
  * By convention, each transform is its own rust crate
* the final shotover binary: the `shotover-bin` crate
  * this also contains integration tests in `shotover-bin/tests`, make sure to utiilize them!

Use an example transform that matches the protocol you are working with as a base. e.g.

* valkey-get-rewrite - for valkey
* kafka-fetch-rewrite - for kafka

## Running the project

To run the shotover binary containing your project just run:

```shell
cargo run --release
```

This also creates a binary at `target/release/shotover-bin` which can be used in production.

To run the integration tests run:

```shell
cargo test --release
```

## A little cleanup

Feel free to delete transforms that you do not need.
That would involve deleting:

* The entire crate folder
* The `members` entry in the workspace level `Cargo.toml`
* The corresponding `shotover::import_transform!` line in `shotover-bin/src/main.rs`

## Development

To understand your transform you are using as a base you will want to consult the [shotover API documentation](https://docs.rs/crate/shotover/latest)
From there explore the API to find how to
