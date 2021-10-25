# Transforms

## Concepts

### Sink

Sink transforms send data out of shotover to some other service.
This is the opposite of shotover's sources, although sources are not transforms.

### Terminating

Every transform chain must have exactly one terminating transform and it must be the final transform of the chain.
This means that terminating transforms cannot pass messages onto another transform in the same chain.
However some terminating transforms define their own subchain to allow further processing of messages.

### Debug

Debug transforms can be temporarily used to test how your shotover configuration performs.
Dont forget to remove them when you are finished.

### Implementation Status

TODO: We should define what alpha/beta/stable actually mean, is it about API stability? battletestedness?

| Transform                                           | Terminating | Implementation Status |
|-----------------------------------------------------|-------------|-----------------------|
|[CassandraSinkSingle](#cassandrasinksingle)          |✅           |Alpha                  |
|[Coalesce](#coalesce)                                |❌           |Alpha                  |
|[DebugPrinter](#debugprinter)                        |❌           |Alpha                  |
|[KafkaSink](#kafkasink)                              |✅           |Alpha                  |
|[Forwarder](#forwarder)                              |✅           |Beta                   |
|[Tee](#tee)                                          |✅           |Beta                   |
|[Null](#null)                                        |✅           |Beta                   |
|[Loopback](#loopback)                                |✅           |Beta                   |
|[ParallelMap](#parallelmap)                          |✅           |Alpha                  |
|[Protect](#protect)                                  |❌           |Beta                   |
|[QueryCounter](#querycounter)                        |❌           |Alpha                  |
|[QueryTypeFilter](#querytypefilter)                  |❌           |Alpha                  |
|[RedisCache](#rediscache)                            |❌           |Alpha                  |
|[RedisClusterPortsRewrite](#redisclusterportsrewrite)|❌           |Alpha                  |
|[RedisSinkCluster](#redissinkcluster)                |✅           |Beta                   |
|[RedisSinkSingle](#redissinksingle)                  |✅           |Beta                   |
|[RedisTimeStampTagger](#redistimestamptagger)        |❌           |Alpha                  |
|[ConsistentScatter](#consistentscatter)          |✅           |Alpha                  |

## CassandraSinkSingle

This transform will take a query, serialise it into a CQL4 compatible format and send to the Cassandra compatible database at the defined address.

* `remote_address` - A String containing the IP address and port of the upstream cassandra node/service. E.g. `remote_address: "127.0.0.1:9042"`
* `bypass_result_processing` - A boolean to disable creating an Abstract Syntax Tree for the query. Saves CPU for straight passthrough cases (no processing on the query). E.g. `bypass_result_processing: false`.

Note: this will just pass the query to the remote node. No cluster discovery or routing occurs with this transform.

## KafkaSink

This transform will take a query and push it to a given Kafka topic.

* `topic` - A String containing the name of the kafka topic. E.g. `topic: "my_kafka_topic"`
* `keys` - A map of configuration options for the Kafka driver. Supports all flags as supported by the librdkafka driver. See
 [here for details](https://docs.confluent.io/5.5.0/clients/librdkafka/md_CONFIGURATION.html) E.g `bootstrap.servers: "127.0.0.1:9092"`.

## RedisSinkSingle

This transform will take a query, serialise it into a RESP2 compatible format and send to the Redis compatible database at the defined address.

* `remote_address` - A String containing the IP address and port of the upstream redis node/service. E.g. `remote_address: "127.0.0.1:9042"`

Note: this will just pass the query to the remote node. No cluster discovery or routing occurs with this transform.

## RedisSinkCluster

This transform is a full featured redis driver that will connect to a redis-cluster and handle all discovery, sharding and routing operations.

* `first_contact_points` - A list of string containing the IP address and port of the upstream redis nodes/service. E.g. `first_contact_points: ["redis://127.0.0.1:2220/", "redis://127.0.0.1:2221/", "redis://127.0.0.1:2222/", "redis://127.0.0.1:2223/", "redis://127.0.0.1:2224/", "redis://127.0.0.1:2225/"]`

Unlike other redis-cluster drivers, this Transform does support pipelining. It does however turn each command from the pipeline into a group of requests split between the master redis node that owns them, buffering results as within different Redis nodes as needed. This is done sequentially and there is room to make this transform split requests between master nodes in a more concurrent manner.

Latency and throughput will be different from pipelining with a single Redis node, but not by much.

### Differences to real Redis

On an existing authenticated connection, a failed auth attempt will not "unauthenticate" the user. This behaviour matches Redis 6 but is different to Redis 5.

### Completeness

_Note: Currently Redis-cluster does not support the following functionality:_

* _Redis Transactions_
* _Scan based operations e.g. SSCAN_

## Forwarder

This transform pushes the query/message to the channel associated with the topic named in its configuration. It will then return an empty success response if it was able to write to the channel succesfully.

* `topic_name` - A string with the topic name to push queries/messages into. E.g. `topic_name: testtopic`

## Tee

This transform asynchronously copies the query/message to the channel associated with the topic named in its configuration. It will then call the downstream transform.

* `topic_name` - A string with the topic name to push queries/messages into. E.g. `topic_name: testtopic`

## DebugPrinter

This transform will log the query/message at an info level, then call the down-chain transform.

## RedisCache

This transform will attempt to cache values for a given primary key in a redis hash set. It is a primarily implemented as a write through cache. It currently expects an SQL based AST to figure out what to cache (e.g. CQL, PGSQL) and updates to the cache and the backing datastore are performed sequentially. 

* `config_values` - A string with the redis connection url. E.g. `config_values: "redis://127.0.0.1/"`

## Protect

This transform will encrypt specific fields before passing them down-chain, it will also decrypt those same fields from a response. The transform will create a data encryption key on an user defined basis (e.g. per primary key, per value, per table etc).

The data encryption key is encrypted by a key encryption key and persisted alongside the encrypted value (alongside other needed cryptographic material). This transform provides the basis for in-application cryptography with unified key management between datastores. The encrypted value is serialised using bincode and should then be written to a blob or bytes field by a down-chain transform.

Fields are protected using a NaCL secretbox (xsalsa20-poly1305). Modification of the field is also detected and raised as an error. DEK protection is dependent on the key manager being used.

* `keyspace_table_columns` - A mapping of keyspaces, tables and columns to encrypt.
* `key_manager` - A KeyManagerConfig that configures the protect Transform with how to look up keys.

Currently the Protect transform supports AWS KMS and or using a local Key Encryption Key on disk. See [key management](keys.md)

Note: Currently the data encryption key ID function is just defined as a static string, this will be replaced by a user defined script shortly.

## ConsistentScatter

This transform implements a distributed eventual consistent mechanism between the set of defined sub-chains. This transform will wait for a user configurable number of chains to return an OK response before returning the value up-chain. This follows a similar model as used by Cassandra for its consistency model. Strong consistency can be achieved when W + R > RF. In this case RF is always the number of chains in the route_map. 

No sharding occurs within this transform and all requests/messages are sent to all routes.

Upon receiving the configured number of responses, the transform will attempt to resolve or unify the response based on metadata about the result. Currently it will try to return the newest response based on a metadata timestamp (last write wins) or it will simply return the largest response if no timestamp information is available.

* `route_map` - A map of named chains. All chains will be used in each request.
 E.g.

```yaml
route_map:
  cluster1:
    - CassandraSinkSingle:
       remote_address: "127.0.0.1:9043"
  cluster2:
   - CassandraSinkSingle:
       remote_address: "127.1.0.2:9043"
  cluster3:
   - CassandraSinkSingle:
       remote_address: "127.2.0.3:9043"
```

* `write_consistency` - The number of chains to wait for a "write" response on.
* `read_consistency` - The number of chains to wait for a "read" response on.

## RedisTimeStampTagger

A transform that wraps each redis command in a lua script that also fetches the key for the operations idletime. This is then used to build a last modified timestamp and insert it into a responses timestamp. The response from the lua operation is unwrapped and returned to up-chain transforms looking like a normal redis response.

This is mainly used in conjunction with the `ConsistentScatter` to enable a Cassandra style consistency model within Redis.

No configuration is required for this transform.

### RedisClusterPortsRewrite

This transform should be used with the RedisSinkCluster transform. It will write over the ports of the nodes returned by `CLUSTER SLOTS` or `CLUSTER NODES` with a user supplied value (typically the port that Shotover is listening on so  cluster aware Redis drivers will direct traffic through Shotover instead of the nodes themselves).

* `new_port`- Value to write over the ports returned by `CLUSTER SLOTS` and `CLUSTER NODES`.
