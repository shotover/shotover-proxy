#!/bin/bash

# The cassandra integration tests also support other drivers that don't rely on external dependencies.
# But if you want to test the cassandra-cpp driver, you will need to install it.
#
# Upstream installation information and dependencies for the Cassandra CPP driver can be found [here](https://docs.datastax.com/en/developer/cpp-driver/2.16/).
#
# However that is likely unusable because datastax do not ship packages for modern ubuntu so we have our own script which will compile, package and install the driver on a modern ubuntu.
# So to install the driver on ubuntu run this script.

set -e

cd "$(dirname "$0")"

sudo apt-get update
# Install dependencies of the cpp-driver even if they are already on CI so that we can run this locally
# These dependencies are needed both to build the driver and to link it with the shotover tests
sudo apt-get install -y libuv1 libuv1-dev g++ libssl-dev zlib1g-dev

# set VERSION to one of the tags here: https://github.com/datastax/cpp-driver/tags
VERSION=2.16.2

PACKAGE_NAME="cassandra-cpp-driver_${VERSION}-1_amd64"
FILE_PATH="packages/${PACKAGE_NAME}.deb"

# Create package if it doesnt already exist
if [ ! -f "$FILE_PATH" ]; then
    # cmake takes a few seconds to install so only install it on a cache miss
    sudo apt-get install -y cmake

    rm -rf cpp-driver # Clean just in case the script failed halfway through last time
    git clone --depth 1 --branch $VERSION https://github.com/datastax/cpp-driver
    pushd cpp-driver

    cmake -DCMAKE_POLICY_VERSION_MINIMUM=3.5 -DCMAKE_INSTALL_PREFIX:PATH=/usr -DCMAKE_INSTALL_LIBDIR:PATH=/usr/lib -Wno-error .
    make

    mkdir -p $PACKAGE_NAME/DEBIAN
    make DESTDIR="$PACKAGE_NAME/" install

    cp ../cassandra-cpp-driver.control $PACKAGE_NAME/DEBIAN/control
    sed -i "s/VERSION/${VERSION}/g" $PACKAGE_NAME/DEBIAN/control
    dpkg-deb --build $PACKAGE_NAME

    mkdir -p ../packages
    cp ${PACKAGE_NAME}.deb ../$FILE_PATH

    popd
    rm -rf cpp-driver

    # remove cmake again, since we only install it on a cache miss it could lead to really confusing CI build failures if we leave it installed.
    sudo apt-get remove -y cmake
fi

sudo dpkg -i $FILE_PATH
