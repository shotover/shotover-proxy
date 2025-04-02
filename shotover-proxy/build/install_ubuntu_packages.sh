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

# Install dependencies of the cpp-driver even if they are already on CI so that we can run this locally
sudo apt-get update
sudo apt-get install -y libuv1 libuv1-dev cmake g++ libssl-dev zlib1g-dev

# set VERSION to one of the tags here: https://github.com/datastax/cpp-driver/tags
VERSION=2.16.2

PACKAGE_NAME="cassandra-cpp-driver_${VERSION}-1_amd64"
FILE_PATH="packages/${PACKAGE_NAME}.deb"

# Create package if it doesnt already exist
if [ ! -f "$FILE_PATH" ]; then
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
fi

sudo dpkg -i $FILE_PATH
