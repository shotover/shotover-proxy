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
* My database slows down over time (wrong indexing scheme, compaction strategy, data no longer fits in memory)
* My database slows down for a period of time (GC, autovacuum, flushes)
* I don't understand where my queries are going and how they are performing (poor observability at the driver level).

These challenges are all generally discovered in production environments rather than testing. So fixing and resolving these
quickly can be tricky, often requiring application and/or schema level changes. 

Shotover aims to make these challenges simpler by providing a point where data locality, performance and storage characteristics are 
(somewhat) decoupled from the application, allowing for on the fly, easy changes to be made queries and data storage choices 
without the need to change and redeploy your application.

Longer term, shotevover can also leverage the same capability to make operational tasks easier to solve a number of other 
challenges that come with working multiple databases. Some of these include:
* Data encryption at the field level, with a common key management scheme between databases.
* Routing the same data to databases that provide different query capabilities or performance characteristics (e.g. indexing data in Redis in 
Elasticsearch, easy caching of DynamoDB data in Redis).
* Routing/replicating data across regions for databases that don't support it natively or the functionality is gated behind
proprietary "open-core" implementations.
* A common audit and AuthZ/AuthN point for SOX/PCI/HIPAA compliance.

Shotover provides a set of predefined transforms that can modify, route and control queries from any number of sources 
to a similar number of destinations. As the user you can construct chains of these transforms to acheive the behaviour required. 
Each transform is configurable and functionality can generally be extended by Lua or WASM scripts. Each chain can then be attached
to a "source" that speaks a the native protocol of you chosen database. The transform chain will process each request with access to
a unified/simplified representation of a generic query, the original raw query and optionally (for SQL like protocols) a 
parsed AST representing the query.

You can also implement your own transforms and sources using Lua, WASM (python, c, ruby, javascript etc) or natively with Rust. 
For concrete examples of what you can achieve with shotover-proxy, see the following examples:
* [Multi-region, active-active redis](../examples/redis-multi)
* [Effortless caching for Cassandra in Redis, combined with Automatic CDC streaming to Kafka from Cassandra queries](../examples/cass-redis-kafka)
* [Field level, "In Application" encryption for Apache Cassandra with AWS Key Management Service](../examples/cassandra-encryption)

Shotover proxy currently supports the following protocols as sources:
* Cassandra (CQLv4)
* Redis (RESP2)