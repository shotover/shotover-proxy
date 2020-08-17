# Shotover

[![Rust](https://github.com/shotover/shotover-proxy/workflows/Rust/badge.svg)](https://github.com/shotover/shotover-proxy/actions?query=workflow%3ARust)


## Documentation
For full documentation please go to [https://docs.shotover.io/](https://docs.shotover.io/)

## What is Shotover?
Shotover-proxy is an open source, high performance L7 data-layer proxy for controlling, managing and modifying the flow 
of database requests in transit. It can be used to solve many different operational and interoperability challenges for 
teams where polyglot persistence (many different databases) is common.

## What problems does Shotover solve?
The majority of operational problems associated with databases come down to a mismatch in the suitability of your data 
model/queries for the workload or a mismatch in behaviour of your chosen database for a given workload. This can manifest 
in many different ways, but commonly shows up as:
* Some queries are slow for certain keys (customers/tenants etc)
* Some queries could be implemented more efficiently (queries not quite right)
* Some tables are too big or inefficient (data model not quite right)
* Some queries are occur far more than others (hot partitions)
* I have this sinking feeling I should have chosen a different database (hmmm yeah... )
* My database slows down over time (wrong indexing scheme, compaction strategy, data no longer fits in memory)
* My database slows down for a period of time (GC, autovacuum, flushes)
* I don't understand where my queries are going and how they are performing (poor observability at the driver level).

These challenges are all generally discovered in production environments rather than testing. So fixing and resolving these
quickly can be tricky, often requiring application and/or schema level changes. 

Shotover aims to make these challenges simpler by providing a point where data locality, performance and storage characteristics are 
(somewhat) decoupled from the application, allowing for on the fly, easy changes to be made queries and data storage choices 
without the need to change and redeploy your application.

Longer term, Shotover can also leverage the same capability to make operational tasks easier to solve a number of other 
challenges that come with working multiple databases. Some of these include:
* Data encryption at the field level, with a common key management scheme between databases.
* Routing the same data to databases that provide different query capabilities or performance characteristics (e.g. indexing data in Redis in 
Elasticsearch, easy caching of DynamoDB data in Redis).
* Routing/replicating data across regions for databases that don't support it natively or the functionality is gated behind
proprietary "open-core" implementations.
* A common audit and AuthZ/AuthN point for SOX/PCI/HIPAA compliance.

## Examples
For concrete examples of what you can achieve with shotover-proxy, see the following examples:
* [Multi-region, active-active redis](../examples/redis-multi)
* [Cassandra query caching in redis, with a query audit trail sent to kafka](../examples/cass-redis-kafka)
* [Field level, "In Application" encryption for Apache Cassandra with AWS Key Management Service](../examples/cassandra-encryption)

Shotover proxy currently supports the following protocols as sources:
* Cassandra (CQLv4)
* Redis (RESP2)

## Deploying Shotover
Shotover can be deployed in a number of ways, it will generally be based on the problem you are trying to solve, but they
all fall into three categories:
* As an application sidecar - Shotover is pretty lightweight, so feel free to deploy it as a sidecar to each of your application
instances.
* As a stand alone proxy - If you are building a Service/DBaaS/Common data layer, you can deploy Shotover on standalone hardware
and really let it fly.\
* As a sidecar to your database - You can also stick Shotover on the same instance/server as your database is running on, we do it, so
we won't judge you. 

## TODO/Roadmap
* Support relevant xDS APIs (so Shotover can play nicely with service mesh implementations)
* Support hot-reloads and a dynamic configuration API.
* Additional sources (DynamoDB and PostgreSQL are good first candidates).
* Add support for rate limiting, explicit back-pressure mechanisms etc
* Additional Distributed algorithm transform primitives (e.g RAFT, 2PC, etc)
* Additional destination transforms (these generally get implemented alongside sources).
* Support user-defined / generated sources (e.g. thrift or a gRPC service from a proto definition).
* Simulation testing once tokio-rs/simulation reaches compatibility with tokio-2.0
* zero-copy pass-through transforms and in-place query editing (perf)

## Name
Shotover refers to the Shotover (Kimi-ākau) river in Otago, New Zealand - close to Queenstown and eventually flowing into Lake Wakatipu
via the Kawarau River, it's famous for white water rafting, bungy-jumping, fast rapids and jet boating.
