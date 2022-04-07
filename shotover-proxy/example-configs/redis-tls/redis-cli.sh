#!/bin/sh

redis-cli --tls --cert certs/redis.crt --key certs/redis.key --cacert certs/ca.crt "$@"
