<p align="center">
  <img width="400px" alt="Shotover logo" src="docs/src/logo.png">
</p>

[![Crates.io](https://img.shields.io/crates/v/shotover.svg)](https://crates.io/crates/shotover)
[![Docs](https://docs.rs/shotover/badge.svg)](https://docs.rs/shotover)
[![dependency status](https://deps.rs/repo/github/shotover/shotover-proxy/status.svg)](https://deps.rs/repo/github/shotover/shotover-proxy)

## Documentation

For full documentation please go to [https://docs.shotover.io/](https://docs.shotover.io/shotover-blog/docs/user-guide/introduction.html)

## Building

Shotover is supported on Linux and macOS.
To build Shotover from source please refer to [the contributing documentation](https://docs.shotover.io/shotover-blog/docs/contributing.html)

## What is Shotover?

Shotover is a high performance, configurable and extensible L7 data-layer proxy for controlling, managing and modifying the flow of database requests in transit. It can be used to solve many different operational and interoperability challenges by transparently intercepting and transforming queries. It is transparent in the sense that it can be plugged into your architecture without requiring application change.

Shotover currently supports intercepting requests for the following technologies (sources):

* Cassandra (CQL4)
* Redis (RESP2)
* Kafka (Kafka Wire Protocol)

It currently supports writing output to the following technologies (sinks):

* Cassandra
* Redis
* Kafka

## What problems does Shotover solve?

Concrete examples where Shotover has been applied include:

* [Multi-region, active-active redis](https://github.com/shotover/shotover-examples/tree/main/redis-backup-cluster)
<!--* [Cassandra query caching in redis, with a query audit trail sent to kafka](shotover-proxy/tests/test-configs/cass-redis-kafka/)-->

More broadly, Shotover is designed to be used for a very wide ranging class of problems where it is useful to transparently intercept a database call and redirect it. This allows you to change the behaviour of running applications at the infrastructure level without change to the application code itself.
Some examples where we envisage Shotover could be deployed include:

* Moving very large or very hot tenants/customers/keys (that can cause unbalanced partition problems in some systems) to a separate data store by intercepting and redirecting queries for those particular keys
* Dual writing and/or query translation to allow the underlying storage technology to be changed (for example, from DynamoDB to Apache Cassandra)
* As an alternative to Change Data Capture technology to send writes to a message stream such as Apache Kafka in addition to the primary database
* Adding auditing, encryption or other security measures

Of course, Shotover is designed to be configurable and extensible so use your imagination and let us know what uses you find!

## Deploying Shotover

Shotover can be deployed in several different ways based on the problem you are trying to solve, but they all fall into three categories:

* As an application sidecar - Shotover is pretty lightweight, so feel free to deploy it as a sidecar to each of your application
instances.
* As a stand-alone proxy - If you are building a Service/DBaaS/Common data layer, you can deploy Shotover on standalone hardware
and really let it fly.
* As a sidecar to your database - You can also stick Shotover on the same instance/server as your database is running on; we do it, so
we won't judge you.

## Roadmap

* Support relevant xDS APIs (so Shotover can play nicely with service mesh implementations)
* Support hot-reloads and a dynamic configuration API.
* Performance metrics.
* Additional sources (DynamoDB and PostgreSQL are good first candidates).
* Add support for rate limiting, explicit back-pressure mechanisms, etc.
* Additional distributed algorithm transform primitives (e.g RAFT, 2PC, etc)
* Additional sink transforms (these generally get implemented alongside sources).
* Support user-defined / generated sources (e.g. thrift or a gRPC service from a proto definition).
* Simulation testing once tokio-rs/simulation reaches compatibility with tokio-2.0.
* Zero-copy pass-through transforms and in-place query editing (performance).

## Name

Shotover refers to the Shotover (Kimi-ākau) river in Otago, New Zealand - close to Queenstown and eventually flowing into Lake Wakatipu
via the Kawarau River, it's famous for white water rafting, bungy-jumping, fast rapids and jet boating.
.
