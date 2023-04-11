#!/usr/bin/env bash

set -e
set -u

TAG=$(git tag --points-at HEAD)

if [ -z "$TAG" ];
then
    echo "Failed: The current commit has no git tags"
    exit 1
fi

if [[ $TAG == *$'\n'* ]];
then
    echo "Failed: multiple git tags are on the latest commit, but only one tag is allowed"
    echo "$TAG"
    exit 1
fi

TAG_VERSION=$(echo $TAG | sed -e "s/^v//")

if [ -z "$TAG_VERSION" ];
then
    echo "Failed: git tag not valid: '$TAG'"
    exit 1
fi

BIN_VERSION="$(cargo metadata --format-version 1 --offline --no-deps | jq -c -M -r '.packages[] | select(.name == "shotover-proxy") | .version')"
if [ "$TAG_VERSION" != "$BIN_VERSION" ];
then
    echo "Failed: git tag '$TAG_VERSION' did not match shotover-proxy version '$BIN_VERSION'"
    exit 1
fi

LIB_VERSION="$(cargo metadata --format-version 1 --offline --no-deps | jq -c -M -r '.packages[] | select(.name == "shotover") | .version')"
if [ "$TAG_VERSION" != "$LIB_VERSION" ];
then
    echo "Failed: git tag '$TAG_VERSION' did not match shotover version '$LIB_VERSION'"
    exit 1
fi

echo "Shotover repository is ready for publishing"
