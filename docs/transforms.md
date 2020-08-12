# Transforms
Currently shotover supports the following transforms:
* CodecDestination
* KafkaDestination
* RedisCodecDestination
* RedisCluster
* RedisCache
* MPSCTee
* MPSCForwarder
* Route
* Scatter
* Printer
* Null
* Lua
* Protect
* RepeatMessage
* RandomDelay
* TuneableConsistency
* RedisTimeStampTagger

## Terminating Transforms
The following transforms will all return a response, any transform after them in the chain won't ever
get a request.

### CodecDestination
This transform will take a query, serialise it into a CQL4 compatible format and send to the Cassandra compatible
database at the defined address.
* `remote_address` - A String containing the IP address and port of the upstream cassandra node/service. E.g. `remote_address: "127.0.0.1:9042"`
* `bypass_result_processing` - A boolean to disable creating an Abstract Syntax Tree for the query. Saves CPU for straight
passthrough cases (no processing on the query). E.g. `bypass_result_processing: false`.

Note: this will just pass the query to the remote node. No cluster discovery or routing occurs with this transform.

### KafkaDestination
This transform will take a query and push it to .
* `topic` - A String containing the name of the kafka topic. E.g. `topic: "my_kafka_topic"`
* `keys` - Configuration options for the Kafka driver. Supports all flags as supported by the librdkafka driver. See
 [here for details](https://docs.confluent.io/5.5.0/clients/librdkafka/md_CONFIGURATION.html) E.g `bootstrap.servers: "127.0.0.1:9092"`.
 
 ### RedisCodecDestination
 This transform will take a query, serialise it into a RESP2 compatible format and send to the Redis compatible
 database at the defined address.
 * `remote_address` - A String containing the IP address and port of the upstream redis node/service. E.g. `remote_address: "127.0.0.1:9042"`
 
 Note: this will just pass the query to the remote node. No cluster discovery or routing occurs with this transform.

 ### RedisCluster
 This transform is a full featured redis driver that will connect to a redis-cluster and handle all discovery, sharding and routing operations.
 * `first_contact_points` - A list of string containing the IP address and port of the upstream redis nodes/service. 
 E.g. `first_contact_points: ["redis://127.0.0.1:2220/", "redis://127.0.0.1:2221/", "redis://127.0.0.1:2222/", "redis://127.0.0.1:2223/", "redis://127.0.0.1:2224/", "redis://127.0.0.1:2225/"]
`
Note: Currently Redis-cluster does not support the following functionality:
* Redis Transactions

Unlike other redis-cluster drivers, this Transform does support pipelining. It does however turn each command from the pipeline into a single
request/response operation, buffering results as within different Redis nodes as needed. Latency will be different from pipelining with a single
Redis node.

