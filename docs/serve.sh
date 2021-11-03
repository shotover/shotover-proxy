#!/bin/sh

# make sure the version here matches with the version used in netlify.toml
cargo install mdbook --version "=0.4.13"
mdbook serve
