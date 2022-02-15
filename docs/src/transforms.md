# Transforms

## Concepts

### Sink

Sink transforms send data out of Shotover to some other service. This is the opposite of Shotover's sources, although sources are not transforms.

### Terminating

Every transform chain must have exactly one terminating transform and it must be the final transform of the chain. This means that terminating transforms cannot pass messages onto another transform in the same chain. However some terminating transforms define their own sub-chains to allow further processing of messages.

### Debug

Debug transforms can be temporarily used to test how your Shotover configuration performs. Dont forget to remove them when you are finished.

### Implementation Status

* Alpha - Should not be used in production.
* Beta - Ready for use but is not battle tested.
* Ready - Ready for use.

Future transforms won't be added to the public API while in alpha. But in these early days we have chosen to publish these alpha transforms to demonstrate the direction we want to take the project.

## Transforms

| Transform                                             | Terminating | Implementation Status |
|-------------------------------------------------------|-------------|-----------------------|
| [CassandraSinkSingle](#cassandrasinksingle)           | ✅          | Alpha                 |
| [CassandraPeersRewrite](#cassandrapeersrewrite)       | ❌          | Alpha                 |
| [Coalesce](#coalesce)                                 | ❌          | Alpha                 |
| [ConsistentScatter](#consistentscatter)               | ✅          | Alpha                 |
| [DebugPrinter](#debugprinter)                         | ❌          | Alpha                 |
| [DebugReturner](#debugreturner)                       | ✅          | Alpha                 |
| [Null](#null)                                         | ✅          | Beta                  |
| [ParallelMap](#parallelmap)                           | ✅          | Alpha                 |
| [Protect](#protect)                                   | ❌          | Alpha                 |
| [QueryCounter](#querycounter)                         | ❌          | Alpha                 |
| [QueryTypeFilter](#querytypefilter)                   | ❌          | Alpha                 |
| [RedisCache](#rediscache)                             | ❌          | Alpha                 |
| [RedisClusterPortsRewrite](#redisclusterportsrewrite) | ❌          | Beta                  |
| [RedisSinkCluster](#redissinkcluster)                 | ✅          | Beta                  |
| [RedisSinkSingle](#redissinksingle)                   | ✅          | Beta                  |
| [RedisTimestampTagger](#redistimestamptagger)         | ❌          | Alpha                 |
| [Tee](#tee)                                           | ✅          | Alpha                 |
<!--| [DebugRandomDelay](#debugrandomdelay)                 | ❌          | Alpha                 |-->

### CassandraSinkSingle

This transform will take a query, serialise it into a CQL4 compatible format and send to the Cassandra compatible database at the defined address.

```yaml
- CassandraSinkSingle:
    # The IP address and port of the upstream cassandra node/service.
    remote_address: "127.0.0.1:9042"
```

Note: this will just pass the query to the remote node. No cluster discovery or routing occurs with this transform.


### CassandraPeersRewrite

This transform should be used with the `CassandraSinkSingle` transform. It will write over the ports of the peers returned by queries to the `system.peers_v2` table in Cassandra with a user supplied value (typically the port that Shotover is listening on so Cassandra drivers will connect to Shotover instead of the Cassandra nodes themselves).


```yaml
- CassandraPeersRewrite:
    # rewrite the peer ports to 9043
    port: 9043
```

### Coalesce

This transform holds onto messages until some requirement is met and then sends them batched together.
Validation will fail if none of the `flush_when_` fields are provided, as this would otherwise result in a Coalesce transform that never flushes.

```yaml
- Coalesce:
    # When this field is provided a flush will occur when the specified number of messages are currently held in the buffer.
    flush_when_buffered_message_count: 2000

    # When this field is provided a flush will occur when the following occurs in sequence:
    # 1. the specified number of milliseconds have passed since the last flush ocurred
    # 2. a new message is received
    flush_when_millis_since_last_flush: 10000
```

### ConsistentScatter

This transform implements a distributed eventual consistency mechanism between the set of defined sub-chains. This transform will wait for a user configurable number of chains to return an OK response before returning the value up-chain. This follows a similar model as used by Cassandra for its consistency model. Strong consistency can be achieved when W + R > RF. In this case RF is always the number of chains in the `route_map`.

No sharding occurs within this transform and all requests/messages are sent to all routes.

Upon receiving the configured number of responses, the transform will attempt to resolve or unify the response based on metadata about the result. Currently it will try to return the newest response based on a metadata timestamp (last write wins) or it will simply return the largest response if no timestamp information is available.

```yaml
- ConsistentScatter:
    # The number of chains to wait for a "write" response on.
    write_consistency: 2
    # The number of chains to wait for a "read" response on.
    read_consistency: 2
    # A map of named chains. All chains will be used in each request.
    route_map:
      instance1:
        - CassandraSinkSingle:
            remote_address: "127.0.0.1:9043"
      instance2:
        - CassandraSinkSingle:
            remote_address: "127.1.0.2:9043"
      instance3:
        - CassandraSinkSingle:
            remote_address: "127.2.0.3:9043"
```

### DebugPrinter

This transform will log the query/message at an info level, then call the down-chain transform.

```YAML
- DebugPrinter
```

### DebugReturner

This transform will drop any messages it receives and return the supplied response.

```yaml
- DebugReturner
    # return a Redis response
    Redis: "42"
   
    # To intentionally fail, use this variant 
    # Fail
```

<!-- commented out until included in the public API
### DebugRandomDelay

Delay the transform chain at the position that this transform sits at.

```yaml
- DebugRandomDelay
  # length of time to delay in milliseconds
  delay: 1000
  
  # optionally provide a distribution for a random amount to add to the base delay
  distribution: 500
```
-->

### Null

This transform will drop any messages it receives and return an empty response.

```yaml
- Null
```

### ParallelMap

This transform will send messages in a single batch in parallel across multiple instances of the chain.

If we have a parallelism of 3 then we would have 3 instances of the chain: C1, C2, C3. If the batch then contains messages M1, M2, M3, M4. Then the messages would be sent as follows:

* M1 would be sent to C1
* M2 would be sent to C2
* M3 would be sent to C3
* M4 would be sent to C1

```yaml
- ParallelMap:
    # Number of duplicate chains to send messages through.
    parallelism: 1
    # if true then responses will be returned in the same as order as the queries went out.
    # if it is false then response may return in any order.
    ordered_results: true
    # The chain that messages are sent through
    chain:
      - QueryCounter:
          name: "DR chain"
      - RedisSinkSingle:
          remote_address: "127.0.0.1:6379"
```

### Protect

This transform will encrypt specific fields before passing them down-chain, it will also decrypt those same fields from a response. The transform will create a data encryption key on an user defined basis (e.g. per primary key, per value, per table etc).

The data encryption key is encrypted by a key encryption key and persisted alongside the encrypted value (alongside other needed cryptographic material). This transform provides the basis for in-application cryptography with unified key management between datastores. The encrypted value is serialised using bincode and should then be written to a blob or bytes field by a down-chain transform.

Fields are protected using a NaCL secretbox (xsalsa20-poly1305). Modification of the field is also detected and raised as an error. DEK protection is dependent on the key manager being used.

```yaml
- Protect
  # A mapping of keyspaces, tables and columns to encrypt.
  keyspace_table_columns
  # A KeyManagerConfig that configures the protect transform with how to look up keys.
  key_manager
```

Currently the Protect transform supports AWS KMS and or using a local Key Encryption Key on disk.

Note: Currently the data encryption key ID function is just defined as a static string, this will be replaced by a user defined script shortly.

### QueryCounter

This transform will log the queries that pass through it.
The log can be accessed via the [Shotover metrics](user-guide/configuration.md#observability_interface)

```yaml
- QueryCounter:
    # this name will be logged with the query count
    name: "DR chain"
```

### QueryTypeFilter

This transform will drop messages that match the specified filter.

```yaml
- QueryTypeFilter:
    # drop messages that are read
    filter: Read

    # alternatively:
    #
    # drop messages that are write
    # filter: Write
    #
    # drop messages that are read write
    # filter: ReadWrite
    #
    # drop messages that are schema changes
    # filter: SchemaChange
    #
    # drop messages that are pub sub messages
    # filter: PubSubMessage
```

### RedisCache

This transform will attempt to cache values for a given primary key in a Redis hash set. It is a primarily implemented as a write through cache. It currently expects an SQL based AST to figure out what to cache (e.g. CQL, PGSQL) and updates to the cache and the backing datastore are performed sequentially.

```yaml
- RedisCache:
    caching_schema:
      test:
        partition_key: [test]
        range_key: [test]
    chain:
      # The chain can contain anything but must end in a Redis sink
      - RedisSinkSingle:
          # The IP address and port of the upstream redis node/service.
          remote_address: "127.0.0.1:6379"
```

### RedisClusterPortsRewrite

This transform should be used with the `RedisSinkCluster` transform. It will write over the ports of the nodes returned by `CLUSTER SLOTS` or `CLUSTER NODES` with a user supplied value (typically the port that Shotover is listening on so cluster aware Redis drivers will direct traffic through Shotover instead of the nodes themselves).

```yaml
- RedisClusterPortsRewrite:
    # rewrite the ports returned by `CLUSTER SLOTS` and `CLUSTER NODES` to use this port.
    new_port: 6380
```

### RedisSinkCluster

This transform is a full featured Redis driver that will connect to a Redis cluster and handle all discovery, sharding and routing operations.

```yaml
- RedisSinkCluster:
    # A list of IP address and ports of the upstream redis nodes/services.
    first_contact_points: ["127.0.0.1:2220", "127.0.0.1:2221", "127.0.0.1:2222", "127.0.0.1:2223", "127.0.0.1:2224", "127.0.0.1:2225"]
    # The number of connections in the connection pool for each node.
    # e.g. if connection_count is 4 and there are 4 nodes there will be a total of 16 connections.
    # When this field is not provided connection_count defaults to 1.
    connection_count: 1
    # When this field is provided TLS is used when connecting to the remote address.
    # Removing this field will disable TLS.
    tls:
      # Path to the certificate file, typically named with a .crt extension.
      certificate_authority_path: "examples/redis-tls/tls_keys/ca.crt"
      # Path to the private key file, typically named with a .key extension.
      certificate_path: "examples/redis-tls/tls_keys/redis.crt"
      # Path to the certificate authority file typically named ca.crt.
      private_key_path: "examples/redis-tls/tls_keys/redis.key"
```

Unlike other Redis cluster drivers, this transform does support pipelining. It does however turn each command from the pipeline into a group of requests split between the master Redis node that owns them, buffering results as within different Redis nodes as needed. This is done sequentially and there is room to make this transform split requests between master nodes in a more concurrent manner.

Latency and throughput will be different from pipelining with a single Redis node, but not by much.

#### Differences to real Redis

On an existing authenticated connection, a failed auth attempt will not "unauthenticate" the user. This behaviour matches Redis 6 but is different to Redis 5.

#### Completeness

_Note: Currently RedisSinkcluster does not support the following functionality:_

* _Redis Transactions_
* _Scan based operations e.g. SSCAN_

### RedisSinkSingle

This transform will take a query, serialise it into a RESP2 compatible format and send to the Redis compatible database at the defined address.

```yaml
- RedisSinkSingle:
    # The IP address and port of the upstream redis node/service.
    remote_address: "127.0.0.1:6379"

    # When this field is provided TLS is used when connecting to the remote address.
    # Removing this field will disable TLS.
    tls:
      # Path to the certificate file, typically named with a .crt extension.
      certificate_path: "tls/redis.crt"
      # Path to the private key file, typically named with a .key extension.
      private_key_path: "tls/redis.key"
      # Path to the certificate authority file typically named ca.crt.
      certificate_authority_path: "tls/ca.crt"
```

Note: this will just pass the query to the remote node. No cluster discovery or routing occurs with this transform.

### RedisTimestampTagger

A transform that wraps each Redis command in a Lua script that also fetches the key for the operations idletime. This is then used to build a last modified timestamp and insert it into a response's timestamp. The response from the Lua operation is unwrapped and returned to up-chain transforms looking like a normal Redis response.

This is mainly used in conjunction with the `ConsistentScatter` transform to enable a Cassandra style consistency model within Redis.

```yaml
- RedisTimestampTagger
```

### Tee

This transform sends messages to both the defined sub chain and the remaining down-chain transforms.
The response from the down-chain transform is returned back up-chain but various behaviours can be defined by the `behaviour` field to handle the case when the responses from the sub chain and down-chain do not match.

```yaml
- Tee:
    # Ignore responses returned by the sub chain
    behavior: Ignore

    # Alternatively:
    #
    # If the responses returned by the sub chain do not equal the responses returned by down-chain then return an error.
    # behavior: FailOnMismatch
    #
    # If the responses returned by the sub chain do not equal the responses returned by down-chain,
    # then the original message is also sent down the SubchainOnMismatch sub chain.
    # This is useful for logging failed messages.
    # behavior: 
    #   SubchainOnMismatch:
    #     - QueryTypeFilter:
    #         filter: Read
    #     - Null

    # Timeout for sending to the sub chain in microseconds
    timeout_micros: 1000
    # The number of message batches that the tee can hold onto in its buffer of messages to send.
    # If they arent sent quickly enough and the buffer is full then tee will drop new incoming messages.
    buffer_size: 10000
    # The sub chain to send duplicate messages through
    chain:
      - QueryTypeFilter:
          filter: Read
      - Null
```
