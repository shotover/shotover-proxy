#!/bin/bash

set -e

sudo apt-get update
sudo apt-get install -y libpcap-dev wget

. /etc/lsb-release

if [ ${DISTRIB_RELEASE} == "18.04" ]
then
  mkdir pkgs
  pushd pkgs
  wget https://downloads.datastax.com/cpp-driver/ubuntu/18.04/cassandra/v2.16.0/cassandra-cpp-driver_2.16.0-1_amd64.deb &
  wget https://downloads.datastax.com/cpp-driver/ubuntu/18.04/cassandra/v2.16.0/cassandra-cpp-driver-dev_2.16.0-1_amd64.deb &
  wget https://downloads.datastax.com/cpp-driver/ubuntu/18.04/dependencies/libuv/v1.35.0/libuv1_1.35.0-1_amd64.deb &
  wget https://downloads.datastax.com/cpp-driver/ubuntu/18.04/dependencies/libuv/v1.35.0/libuv1-dev_1.35.0-1_amd64.deb &
  wait

  sudo apt -y install ./cassandra-cpp-driver_2.16.0-1_amd64.deb \
    ./cassandra-cpp-driver-dev_2.16.0-1_amd64.deb \
    ./libuv1_1.35.0-1_amd64.deb ./libuv1-dev_1.35.0-1_amd64.deb

  popd
  rm -r pkgs
fi

