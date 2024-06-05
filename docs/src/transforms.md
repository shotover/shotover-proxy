# Transforms

## Concepts

### Sink

Sink transforms send data out of Shotover to some other service. This is the opposite of Shotover's sources, although sources are not transforms.

### Terminating

Every transform chain must have exactly one terminating transform and it must be the final transform of the chain. This means that terminating transforms cannot pass messages onto another transform in the same chain. However some terminating transforms define their own sub-chains to allow further processing of messages.

### Debug

Debug transforms can be temporarily used to test how your Shotover configuration performs. Don't forget to remove them when you are finished.

### Implementation Status

* Alpha - Should not be used in production.
* Beta - Ready for use but is not battle tested.
* Ready - Ready for use.

Future transforms won't be added to the public API while in alpha. But in these early days we have chosen to publish these alpha transforms to demonstrate the direction we want to take the project.

## Transforms

| Transform                                                | Terminating | Implementation Status |
|----------------------------------------------------------|-------------|-----------------------|
| [CassandraSinkCluster](#cassandrasinkcluster)            | ✅          | Beta                  |
| [CassandraSinkSingle](#cassandrasinksingle)              | ✅          | Alpha                 |
| [CassandraPeersRewrite](#cassandrapeersrewrite)          | ❌          | Alpha                 |
| [Coalesce](#coalesce)                                    | ❌          | Alpha                 |
| [DebugPrinter](#debugprinter)                            | ❌          | Alpha                 |
| [DebugReturner](#debugreturner)                          | ✅          | Alpha                 |
| [KafkaSinkCluster](#kafkasinkcluster)                    | ✅          | Beta                  |
| [KafkaSinkSingle](#kafkasinksingle)                      | ✅          | Beta                  |
| [NullSink](#nullsink)                                    | ✅          | Beta                  |
| [ParallelMap](#parallelmap)                              | ✅          | Alpha                 |
| [Protect](#protect)                                      | ❌          | Alpha                 |
| [QueryCounter](#querycounter)                            | ❌          | Alpha                 |
| [QueryTypeFilter](#querytypefilter)                      | ❌          | Alpha                 |
| [RedisCache](#rediscache)                                | ❌          | Alpha                 |
| [RedisClusterPortsRewrite](#redisclusterportsrewrite)    | ❌          | Beta                  |
| [RedisSinkCluster](#redissinkcluster)                    | ✅          | Beta                  |
| [RedisSinkSingle](#redissinksingle)                      | ✅          | Beta                  |
| [Tee](#tee)                                              | ✅          | Alpha                 |
| [RequestThrottling](#requestthrottling)                  |❌           | Alpha                 |
<!--| [DebugRandomDelay](#debugrandomdelay)                 | ❌          | Alpha                 |-->

### CassandraSinkCluster

This transform will route Cassandra messages to a node within a Cassandra cluster based on:

* a configured `data_center` and `rack`
* token aware routing

The fact that Shotover is routing to multiple destination nodes will be hidden from the client.
Instead Shotover will pretend to be either a single Cassandra node or part of a cluster of Cassandra nodes consisting entirely of Shotover instances.

This is achieved by rewriting `system.local` and `system.peers`/`system.peers_v2` query results.
The `system.local` will make Shotover appear to be its own node.
While `system.peers`/`system.peers_v2` will be rewritten to list the configured Shotover peers as the only other nodes in the cluster.

```yaml
- CassandraSinkCluster:
    # contact points must be within the configured data_center and rack.
    # If this is not followed, Shotover will still function correctly but Shotover will communicate with a
    # node outside of the specified data_center and rack.
    first_contact_points: ["172.16.1.2:9042", "172.16.1.3:9042"]

    # A list of every Shotover node that will be proxying to the same Cassandra cluster.
    # This field should be identical for all Shotover nodes proxying to the same Cassandra cluster.
    shotover_nodes:
        # Address of the Shotover node.
        # This is usually the same address as the Shotover source that is connected to this sink.
        # But it may be different if you want Shotover to report a different address.
      - address: "127.0.0.1:9042"
        # The data_center the Shotover node will report as and route messages to.
        # For performance reasons, the Shotover node should be physically located in this data_center.
        data_center: "dc1"
        # The rack the Shotover node will report as and route messages to.
        # For performance reasons, the Shotover node should be physically located in this rack.
        rack: "rack1"
        # The host_id that Shotover will report as.
        # Does not affect message routing.
        # Make sure to set this to a unique value for each Shotover node, maybe copy one from: https://wasteaguid.info
        host_id: "2dd022d6-2937-4754-89d6-02d2933a8f7a"

      # If you only have a single Shotover instance then you only want a single node.
      # Otherwise if you have multiple Shotover instances then add more nodes e.g.
      #- address: "127.0.0.2:9042"
      #  data_center: "dc1"
      #  rack: "rack2"
      #  host_id: "3c3c4e2d-ba74-4f76-b52e-fb5bcee6a9f4"
      #- address: "127.0.0.3:9042"
      #  data_center: "dc2"
      #  rack: "rack1"
      #  host_id: "fa74d7ec-1223-472b-97de-04a32ccdb70b"

    # Defines which entry in shotover_nodes this Shotover instance will become.
    # This affects:
    # * the shotover_nodes data_center and rack fields are used for routing messages
    #     + Shotover will never route messages outside of the specified data_center
    #     + Shotover will always prefer to route messages to the specified rack
    #         but may route outside of the rack when nodes in the rack are unreachable
    # * which shotover_nodes entry is included in system.local and excluded from system.peers
    local_shotover_host_id: "2dd022d6-2937-4754-89d6-02d2933a8f7a"

    # Number of milliseconds to wait for a connection to be created to a destination cassandra instance.
    # If the timeout is exceeded then connection to another node is attempted
    # If all known nodes have resulted in connection timeouts an error will be returned to the client.
    connect_timeout_ms: 3000

    # When this field is provided TLS is used when connecting to the remote address.
    # Removing this field will disable TLS.
    #tls:
    #  # Path to the certificate authority file, typically named with a .crt extension.
    #  certificate_authority_path: "tls/localhost_CA.crt"
    #  # Path to the certificate file, typically named with a .crt extension.
    #  certificate_path: "tls/localhost.crt"
    #  # Path to the private key file, typically named with a .key extension.
    #  private_key_path: "tls/localhost.key"
    #  # Enable/disable verifying the hostname of the certificate provided by the destination.
    #  #verify_hostname: true

    # Timeout in seconds after which to give up waiting for a response from the destination.
    # This field is optional, if not provided, timeout will never occur.
    # When a timeout occurs the connection to the client is immediately closed.
    # read_timeout: 60
```

#### Error handling

If Shotover sends a request to a node and never gets a response, (maybe the node went down), Shotover will return a Cassandra `Server` error to the client.
This is because the message may or may not have succeeded, so only the client can attempt to retry as the retry may involve checking if the original query did in fact complete successfully.

If no nodes are capable of receiving the query then Shotover will return a Cassandra `Overloaded` error indicating that the client should retry the query at some point.

All other connection errors will be handled internally by Shotover.
And all Cassandra errors will be passed directly back to the client.

#### Metrics

This transform emits a metrics [counter](user-guide/observability.md#counter) named `failed_requests` and the labels `transform` defined as `CassandraSinkCluster` and `chain` as the name of the chain that this transform is in.

### CassandraSinkSingle

This transform will send/receive Cassandra messages to a single Cassandra node.
This will just pass the query directly to the remote node.
No cluster discovery or routing occurs with this transform.

```yaml
- CassandraSinkSingle:
    # The IP address and port of the upstream Cassandra node/service.
    remote_address: "127.0.0.1:9042"

    # Number of milliseconds to wait for a connection to be created to the destination cassandra instance.
    # If the timeout is exceeded then an error is returned to the client.
    connect_timeout_ms: 3000

    # When this field is provided TLS is used when connecting to the remote address.
    # Removing this field will disable TLS.
    #tls:
    #  # Path to the certificate authority file, typically named with a .crt extension.
    #  certificate_authority_path: "tls/localhost_CA.crt"
    #  # Path to the certificate file, typically named with a .crt extension.
    #  certificate_path: "tls/localhost.crt"
    #  # Path to the private key file, typically named with a .key extension.
    #  private_key_path: "tls/localhost.key"
    #  # Enable/disable verifying the hostname of the certificate provided by the destination.
    #  #verify_hostname: true

    # Timeout in seconds after which to give up waiting for a response from the destination.
    # This field is optional, if not provided, timeout will never occur.
    # When a timeout occurs the connection to the client is immediately closed.
    # read_timeout: 60
```

This transform emits a metrics [counter](user-guide/observability.md#counter) named `failed_requests` and the labels `transform` defined as `CassandraSinkSingle` and `chain` as the name of the chain that this transform is in.

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

### KafkaSinkCluster

This transform will route kafka messages to a broker within a Kafka cluster:

* produce messages are routed to the partition leader
* fetch messages are routed to a random partition replica
* heartbeat, syncgroup, offsetfetch, joingroup and leavegroup are all routed to the group coordinator
* all other messages go to a random node.

The fact that Shotover is routing to multiple destination nodes will be hidden from the client.
Instead Shotover will pretend to be either a single Kafka node or part of a cluster of Kafka nodes consisting entirely of Shotover instances.

This is achieved by rewriting the FindCoordinator, Metadata and DescribeCluster messages to contain the nodes in the shotover cluster instead of the kafka cluster.

#### SASL SCRAM

By default KafkaSinkCluster does not support SASL SCRAM authentication, if a client attempts to use SCRAM it will appear as if its not enabled in the server.
SCRAM can not be supported normally as it is protected against replaying of auth messages, preventing shotover from opening multiple outgoing connections.

However, SCRAM support can be achieved by enabling the `authorize_scram_over_mtls` option.
This will, hidden from the client, generate delegation tokens over an mTLS connection that correspond to the username sent over the SCRAM auth requests.
First the clients SCRAM auth requests are routed to a single kafka broker to verify the user has the correct credentials.
Once authentication is confirmed, shotover creates new outgoing connections to different brokers via delegation token authentication. (Outgoing connections are accessible only to the one incoming connection)
If SCRAM authentication against the first kafka broker fails, shotover will terminate the connection before processing any non-auth requests, to ensure the client can not escalate privileges.

```yaml
- KafkaSinkCluster:
    # Addresses of the initial kafka brokers to connect to.
    first_contact_points: ["172.16.1.2:9092", "172.16.1.3:9092"]

    # A list of every Shotover node that will be proxying to the same kafka cluster.
    # This field should be identical for all Shotover nodes proxying to the same kafka cluster.
    shotover_nodes:
        # Address of the Shotover node.
        # This is usually the same address as the Shotover source that is connected to this sink.
        # But it may be different if you want Shotover to report a different address.
      - address: "127.0.0.1:9092"
        # The rack the Shotover node will report as and route messages to.
        # For performance reasons, the Shotover node should be physically located in this rack.
        rack: "rack0"
        # The broker ID the Shotover node will report as.
        # Does not affect how shotover will route the requests it receives.
        # Make sure to set this to a unique value for each Shotover node.
        # This must be done to allow the client to properly tell the shotover instances apart.
        broker_id: 0
      # If you only have a single Shotover instance then you only want a single node.
      # Otherwise if you have multiple Shotover instances then add more nodes e.g.
      #- address: "127.0.0.2:9092"
      #  rack: "rack1"
      #  broker_id: 1

    # Defines which entry in shotover_nodes this Shotover instance will become.
    # This determines which rack shotover will route to.
    local_shotover_broker_id: 0

    # Number of milliseconds to wait for a connection to be created to a destination kafka broker.
    # If the timeout is exceeded then connection to another node is attempted
    # If all known nodes have resulted in connection timeouts an error will be returned to the client.
    connect_timeout_ms: 3000

    # Timeout in seconds after which to give up waiting for a response from the destination.
    # This field is optional, if not provided, timeout will never occur.
    # When a timeout occurs the connection to the client is immediately closed.
    # read_timeout: 60

    # When this field is provided TLS is used when connecting to the remote address.
    # Removing this field will disable TLS.
    #tls:
    #  # Path to the certificate authority file, typically named with a .crt extension.
    #  certificate_authority_path: "tls/localhost_CA.crt"
    #  # Path to the certificate file, typically named with a .crt extension.
    #  certificate_path: "tls/localhost.crt"
    #  # Path to the private key file, typically named with a .key extension.
    #  private_key_path: "tls/localhost.key"
    #  # Enable/disable verifying the hostname of the certificate provided by the destination.
    #  #verify_hostname: true

    # When this field is provided authorization of SCRAM over mTLS is enabled.
    # Removing this field will disable the feature.
    #authorize_scram_over_mtls:
    #  # This must point at a kafka port that exposes mTLS authentication for a user capable of creating delegation tokens.
    #  mtls_port_contact_points: ["172.16.1.2:9094"]
    #  # The TLS certs for an mTLS user capable of creating delegation tokens
    #  tls:
    #    certificate_authority_path: "tls/mtls_localhost_CA.crt"
    #    certificate_path: "tls/mtls_localhost.crt"
    #    private_key_path: "tls/mtls_localhost.key"
    #    verify_hostname: true

```

### KafkaSinkSingle

This transform will send/receive Kafka messages to a single Kafka node running on the same machine as shotover.
All kafka brokers in the cluster must be configured with a shotover instance in front of them.
All shotover instances must be on the same port X and all kafka instances must use another port Y.
The client will then connect via shotover's port X.

In order to force clients to connect through shotover the FindCoordinator, Metadata and DescribeCluster messages are rewritten to use the shotover port.

```yaml
- KafkaSinkSingle:
    # The port of the upstream Kafka node/service.
    destination_port: 9092

    # Number of milliseconds to wait for a connection to be created to the destination kafka instance.
    # If the timeout is exceeded then an error is returned to the client.
    connect_timeout_ms: 3000

    # Timeout in seconds after which to give up waiting for a response from the destination.
    # This field is optional, if not provided, timeout will never occur.
    # When a timeout occurs the connection to the client is immediately closed.
    # read_timeout: 60

    # When this field is provided TLS is used when connecting to the remote address.
    # Removing this field will disable TLS.
    #tls:
    #  # Path to the certificate authority file, typically named with a .crt extension.
    #  certificate_authority_path: "tls/localhost_CA.crt"
    #  # Path to the certificate file, typically named with a .crt extension.
    #  certificate_path: "tls/localhost.crt"
    #  # Path to the private key file, typically named with a .key extension.
    #  private_key_path: "tls/localhost.key"
    #  # Enable/disable verifying the hostname of the certificate provided by the destination.
    #  #verify_hostname: true
```

This transform emits a metrics [counter](user-guide/observability.md#counter) named `failed_requests` and the labels `transform` defined as `CassandraSinkSingle` and `chain` as the name of the chain that this transform is in.

### NullSink

This transform will drop any messages it receives and return an empty response.

```yaml
- NullSink
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
          connect_timeout_ms: 3000
```

### Protect

This transform will encrypt specific fields before passing them down-chain, it will also decrypt those same fields from a response. The transform will create a data encryption key on an user defined basis (e.g. per primary key, per value, per table etc).

The data encryption key is encrypted by a key encryption key and persisted alongside the encrypted value (alongside other needed cryptographic material). This transform provides the basis for in-application cryptography with unified key management between datastores. The encrypted value is serialised using bincode and should then be written to a blob field by a down-chain transform.

Fields are protected using ChaCha20-Poly1305. Modification of the field is also detected and raised as an error. DEK protection is dependent on the key manager being used.

#### Local

```yaml
- Protect:
    # A key_manager config that configures the protect transform with how to look up keys.
    key_manager:
      Local: 
        kek: Ht8M1nDO/7fay+cft71M2Xy7j30EnLAsA84hSUMCm1k=
        kek_id: ""

    # A mapping of keyspaces, tables and columns to encrypt.
    keyspace_table_columns:
      test_protect_keyspace:
        test_table:
          - col1
```

#### AWS

```yaml
- Protect:
    # A key_manager config that configures the protect transform with how to look up keys.
    key_manager:
      AWSKms:
        endpoint: "http://localhost:5000"
        region: "us-east-1"
        cmk_id: "alias/aws/secretsmanager"
        number_of_bytes: 32
            
    # A mapping of keyspaces, tables and columns to encrypt.
    keyspace_table_columns:
      test_protect_keyspace:
        test_table:
          - col1
```

Note: Currently the data encryption key ID function is just defined as a static string, this will be replaced by a user defined script shortly.

### QueryCounter

This transform will log the queries that pass through it.
The log can be accessed via the [Shotover metrics](user-guide/configuration.md#observability_interface)

```yaml
- QueryCounter:
    # this name will be logged with the query count
    name: "DR chain"
```

This transform emits a metrics [counter](user-guide/observability.md#counter) named `query_count` with the label `name` defined as the name from the config, in the example it will be `DR chain`.

### QueryTypeFilter

This transform will drop messages that match the specified filter. You can either filter out all messages that do not match those on the `AllowList` or filter the messages that match those on the `DenyList`.

```yaml

# Possible message types to filter: [Read, Write, ReadWrite, SchemaChange, PubSubMessage]

- QueryTypeFilter:
    # only allow read messages through with an allow list
    AllowList: [Read]

    # only allow read messages by blocking all other messages with a deny list
    # DenyList: [Write, ReadWrite, SchemaChange, PubSubMessage]
```

### RedisCache

This transform will attempt to cache values for a given primary key in a Redis hash set. It is a primarily implemented as a read behind cache. It currently expects an SQL based AST to figure out what to cache (e.g. CQL, PGSQL) and updates to the cache and the backing datastore are performed sequentially.

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
          connect_timeout_ms: 3000

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

    # By default RedisSinkCluster will attempt to emulate a single non-clustered redis node by completely hiding the fact that redis is a cluster.
    # However, when this field is provided, this cluster hiding is disabled.
    # Instead other nodes in the cluster will only be accessed when performing a command that accesses a slot.
    # All other commands will be passed directly to the direct_connection node.
    # direct_connection: "127.0.0.1:2220"

    # The number of connections in the connection pool for each node.
    # e.g. if connection_count is 4 and there are 4 nodes there will be a total of 16 connections.
    # When this field is not provided connection_count defaults to 1.
    connection_count: 1

    # Number of milliseconds to wait for a connection to be created to a destination redis instance.
    # If the timeout is exceeded then connection to another node is attempted
    # If all known nodes have resulted in connection timeouts an error will be returned to the client.
    connect_timeout_ms: 3000

    # When this field is provided TLS is used when connecting to the remote address.
    # Removing this field will disable TLS.
    #tls:
    #  # Path to the certificate authority file, typically named ca.crt.
    #  certificate_authority_path: "tls/ca.crt"
    #  # Path to the certificate file, typically named with a .crt extension.
    #  certificate_path: "tls/redis.crt"
    #  # Path to the private key file, typically named with a .key extension.
    #  private_key_path: "tls/redis.key"
    #  # Enable/disable verifying the hostname of the certificate provided by the destination.
    #  #verify_hostname: true
```

Unlike other Redis cluster drivers, this transform does support pipelining. It does however turn each command from the pipeline into a group of requests split between the master Redis node that owns them, buffering results as within different Redis nodes as needed. This is done sequentially and there is room to make this transform split requests between master nodes in a more concurrent manner.

Latency and throughput will be different from pipelining with a single Redis node, but not by much.

This transform emits a metrics [counter](user-guide/observability.md#counter) named `failed_requests` and the labels `transform` defined as `RedisSinkCluster` and `chain` as the name of the chain that this transform is in.

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

    # Number of milliseconds to wait for a connection to be created to the destination redis instance.
    # If the timeout is exceeded then an error is returned to the client.
    connect_timeout_ms: 3000

    # When this field is provided TLS is used when connecting to the remote address.
    # Removing this field will disable TLS.
    #tls:
    #  # Path to the certificate authority file, typically named ca.crt.
    #  certificate_authority_path: "tls/ca.crt"
    #  # Path to the certificate file, typically named with a .crt extension.
    #  certificate_path: "tls/redis.crt"
    #  # Path to the private key file, typically named with a .key extension.
    #  private_key_path: "tls/redis.key"
    #  # Enable/disable verifying the hostname of the certificate provided by the destination.
    #  #verify_hostname: true
```

Note: this will just pass the query to the remote node. No cluster discovery or routing occurs with this transform.

This transform emits a metrics [counter](user-guide/observability.md#counter) named `failed_requests` and the labels `transform` defined as `RedisSinkSingle` and `chain` as the name of the chain that this transform is in.

### Tee

This transform sends messages to both the defined sub chain and the remaining down-chain transforms.
The response from the down-chain transform is returned back up-chain but various behaviours can be defined by the `behaviour` field to handle the case when the responses from the sub chain and down-chain do not match.

Tee also exposes an optional HTTP API to switch which chain to use as the "result source", that is the chain to return responses from.

`GET` `/transform/tee/result-source` will return `regular-chain` or `tee-chain` indicating which chain is being used for the result source.

`PUT` `/transform/tee/result-source` with the body content as either `regular-chain` or `tee-chain` to set the result source.

```yaml
- Tee:
    # Ignore responses returned by the sub chain
    behavior: Ignore

    # Alternatively:
    #
    # If the responses returned by the sub chain do not equal the responses returned by down-chain then return an error.
    # behavior: FailOnMismatch
    #
    # If the responses returned by the sub chain do not equal the responses returned by down-chain then log the mismatch at the warning level.
    # behavior: LogWarningOnMismatch
    #
    # If the responses returned by the sub chain do not equal the responses returned by down-chain,
    # then the original message is also sent down the SubchainOnMismatch sub chain.
    # This is useful for logging failed messages.
    # behavior: 
    #   SubchainOnMismatch:
    #     - QueryTypeFilter:
    #         DenyList: [Read]
    #     - NullSink

    # The port that the HTTP API will listen on.
    # When this field is not provided the HTTP API will not be run.
    # http_api_port: 1234
    #
    # Timeout for sending to the sub chain in microseconds
    timeout_micros: 1000
    # The number of message batches that the tee can hold onto in its buffer of messages to send.
    # If they arent sent quickly enough and the buffer is full then tee will drop new incoming messages.
    buffer_size: 10000
    # The sub chain to send duplicate messages through
    chain:
      - QueryTypeFilter:
          DenyList: [Read]
      - NullSink
```

This transform emits a metrics [counter](user-guide/observability.md#counter) named `tee_dropped_messages` and the label `chain` as `Tee`.

### RequestThrottling

This transform will backpressure requests to Shotover, ensuring that throughput does not exceed the `max_requests_per_second` value.`max_requests_per_second` has a minimum allowed value of 50 to ensure that drivers such as Cassandra are able to complete their startup procedure correctly. In Shotover, a "request" is counted as a query/statement to upstream service. In Cassandra, the list of queries in a BATCH statement are each counted as individual queries. It uses a [Generic Cell Rate Algorithm](https://en.wikipedia.org/wiki/Generic_cell_rate_algorithm).

```yaml
- RequestThrottling
    max_requests_per_second: 20000
```
