#!/bin/bash

set -e

if [ "$EUID" -e 0 ]
  then echo "This script should not be run as root. It will run sudo itself as required."
  exit
fi
cd "$(dirname "$0")"

sudo apt-get update

# Install dependencies for openssl, needed by `redis` crate
sudo apt-get install -y pkg-config

# Install docker
curl -sSL https://get.docker.com/ | sudo sh
# Do not use the rootless install of docker as many of our tests rely on the user created bridge networks
# having their interface exposed to the host, which rootless install does not support.
# Instead add your user to the docker group:
usermod -aG docker $USER`

# Install dependencies for kafka java driver tests
sudo apt-get install -y default-jre-headless`

# Install dependencies for npm tests
sudo apt-get install -y npm`

# The remaining dependencies are for tests behind optional features.
# So feel free to skip them.

# Install dependencies needed for `--features kafka-cpp-driver-tests`
sudo apt-get install -y cmake g++

## Install dependencies for `--features cassandra-cpp-driver-tests`
./install_ubuntu_packages.sh
