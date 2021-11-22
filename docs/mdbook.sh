#!/bin/bash

set -e; set -u

# Setup the the mdbook binaries if they havent already been setup
if [ ! -d "mdbook_bin" ]; then
    mkdir -p mdbook_bin
    pushd mdbook_bin
    curl -L https://github.com/rust-lang/mdBook/releases/download/v0.4.13/mdbook-v0.4.13-x86_64-unknown-linux-gnu.tar.gz | tar xvz
    wget https://github.com/Michael-F-Bryan/mdbook-linkcheck/releases/download/v0.7.6/mdbook-linkcheck.x86_64-unknown-linux-gnu.zip -O linkcheck.zip
    unzip linkcheck.zip
    chmod +x mdbook-linkcheck
    popd
fi

# Need to set the PATH so that mdbook can find the mdbook-linkcheck plugin
PATH="$PATH:$PWD/mdbook_bin"

mdbook "$@"
