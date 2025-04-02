#!/bin/bash

# The cassandra integration tests also support other drivers that don't rely on external dependencies.
# But if you want to test the cassandra-cpp driver, you will need to install it.

set -e

cd "$(dirname "$0")"

# Install dependencies of the cpp-driver even if they are already on CI so that we can run this locally
sudo apt-get update
sudo apt-get install -y libuv1 libuv1-dev g++ libssl-dev zlib1g-dev

PACKAGE_VERSION=2.17.1
FILE_PATH="packages/cassandra-cpp-driver.deb"
FILE_PATH_DEV="packages/cassandra-cpp-driver-dev.deb"

rm -rf packages || true
mkdir -p packages

# set $VERSION_ID environment variable
. /etc/os-release

if [ "$VERSION_ID" = "22.04" ]; then
    curl -L https://datastax.jfrog.io/artifactory/cpp-php-drivers/cpp-driver/builds/${PACKAGE_VERSION}/e05897d/ubuntu/22.04/cassandra/v${PACKAGE_VERSION}/cassandra-cpp-driver_2.17.1-1_amd64.deb -o $FILE_PATH
    curl -L https://datastax.jfrog.io/artifactory/cpp-php-drivers/cpp-driver/builds/${PACKAGE_VERSION}/e05897d/ubuntu/22.04/cassandra/v${PACKAGE_VERSION}/cassandra-cpp-driver-dev_2.17.1-1_amd64.deb -o $FILE_PATH_DEV
elif [ "$VERSION_ID" = "24.04" ]; then
    # 22.04 package on 24.04 seems to work.
    curl -L https://datastax.jfrog.io/artifactory/cpp-php-drivers/cpp-driver/builds/${PACKAGE_VERSION}/e05897d/ubuntu/22.04/cassandra/v${PACKAGE_VERSION}/cassandra-cpp-driver_2.17.1-1_amd64.deb -o $FILE_PATH
    curl -L https://datastax.jfrog.io/artifactory/cpp-php-drivers/cpp-driver/builds/${PACKAGE_VERSION}/e05897d/ubuntu/22.04/cassandra/v${PACKAGE_VERSION}/cassandra-cpp-driver-dev_2.17.1-1_amd64.deb -o $FILE_PATH_DEV
else
    echo "Ubuntu $VERSION_ID not supported by script yet"
    exit 1
fi

sudo dpkg -i $FILE_PATH
sudo dpkg -i $FILE_PATH_DEV
