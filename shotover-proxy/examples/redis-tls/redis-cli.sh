#!/bin/sh

redis-cli --tls --cert tls_keys/redis.crt --key tls_keys/redis.key --cacert tls_keys/ca.crt "$@"
