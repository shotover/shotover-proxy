# Shotover
![Rust](https://github.com/benbromhead/proxy-poc/workflows/Rust/badge.svg)

Shotover-proxy is an open source, high performance L7 data-layer proxy for controlling, managing and modifying the flow 
of database requests in transit. It can be used to solve many different operational and interoperability challenges for 
teams where polyglot persistence (many different databases) is common.

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

## Design principals / goals
* Security, Durability, Availability and Performance (in that order).
* Data layer / database queries and operations.
* Easy extensibility by end users and the broader community
* Ecosystem compatibility
* Deployment flexability

Shotover provides a set of predefined transforms that can modify, route and control queries from any number of sources 
to a similar number of destinations. As the user you can construct chains of these transforms to acheive the behaviour required. 
Each transform is configurable and functionality can generally be extended by Lua or WASM scripts. Each chain can then be attached
to a "source" that speaks a the native protocol of you chosen database. The transform chain will process each request with access to
a unified/simplified representation of a generic query, the original raw query and optionally (for SQL like protocols) a 
parsed AST representing the query.

You can also implement your own transforms and sources using Lua, WASM (python, c, ruby, javascript etc) or natively with Rust. 
For concrete examples of what you can achieve with shotover-proxy, see the following examples:
* [Multi-region, active-active redis](../examples/redis-multi)
* [Cassandra query caching in redis, with a query audit trail sent to kafka](../examples/cass-redis-kafka)
* [Field level, "In Application" encryption for Apache Cassandra with AWS Key Management Service](../examples/cassandra-encryption)

Shotover proxy currently supports the following protocols as sources:
* Cassandra (CQLv4)
* Redis (RESP2)

## Shotover performance
Shotover compiles down to a single binary and just takes a single yaml file and some optional cmd line params to start up.
When running a small topology (5 - 10 transforms, 1 or 2 sources, 200 or so TCP connections) memory consumptions is rather 
small with a rough working set size between 10 - 20mb. 

Currently benchmarking is limited but we see around 25k req/s inbound routed to an outbound 75k req/s max out a single logical core.
However due to the way Shotover is implemented, it will largely go as fast as your upstream datastore can go. Each tcp connection
is driven by a single tokio thread and by default Shotover will use 4 to 8 OS threads for the bulk of it's work (this is user configurable). 
Occasionally it will spawn additional OS threads for long running non-async code. These are practically unbounded (as defined by Tokio) but use is rare.

Shotover will not try to pipeline, aggregate or batch requests (though feel free to write a Transform to do so!!) unless 
it is explicitly built into the source protocol (e.g. RESP2 supports cmd pipelining). This means single connection performance
will not be as good as some other proxy implementations, we simply just do one request at a time. Most client drivers support
connection pooling and multiple connections, so feel free to ramp up the number of outbound sockets to get the best throughput.
Shotover will happily work with 100's or 1000's of connections due to its threading model.

Performance hasn't been a primary focus during initial development and there are definitely some easy wins to improve things.

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
