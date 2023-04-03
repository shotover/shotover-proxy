#!/bin/sh

redis-cli --tls --cert certs/localhost.crt --key certs/redis.key --cacert certs/localhost_CA.crt "$@"
