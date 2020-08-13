# Transforms
Currently shotover supports the following transforms:
* CodecDestination
* KafkaDestination
* RedisCodecDestination
* RedisCluster
* MPSCForwarder
* MPSCTee
* Route
* Scatter
* Printer
* RedisCache
* Protect
* TuneableConsistency
* RedisTimeStampTagger
* Lua

## Terminating Transforms
The following transforms will all return a response, any transform after them in the chain won't ever
get a request.

### CodecDestination
*State: Alpha*

This transform will take a query, serialise it into a CQL4 compatible format and send to the Cassandra compatible
database at the defined address.
* `remote_address` - A String containing the IP address and port of the upstream cassandra node/service. E.g. `remote_address: "127.0.0.1:9042"`
* `bypass_result_processing` - A boolean to disable creating an Abstract Syntax Tree for the query. Saves CPU for straight
passthrough cases (no processing on the query). E.g. `bypass_result_processing: false`.

Note: this will just pass the query to the remote node. No cluster discovery or routing occurs with this transform.

### KafkaDestination
*State: Alpha*

This transform will take a query and push it to .
* `topic` - A String containing the name of the kafka topic. E.g. `topic: "my_kafka_topic"`
* `keys` - A map of configuration options for the Kafka driver. Supports all flags as supported by the librdkafka driver. See
 [here for details](https://docs.confluent.io/5.5.0/clients/librdkafka/md_CONFIGURATION.html) E.g `bootstrap.servers: "127.0.0.1:9092"`.
 
 ### RedisCodecDestination
 *State: Alpha*

 This transform will take a query, serialise it into a RESP2 compatible format and send to the Redis compatible
 database at the defined address.
 * `remote_address` - A String containing the IP address and port of the upstream redis node/service. E.g. `remote_address: "127.0.0.1:9042"`
 
 Note: this will just pass the query to the remote node. No cluster discovery or routing occurs with this transform.

 ### RedisCluster
 *State: Alpha*


 This transform is a full featured redis driver that will connect to a redis-cluster and handle all discovery, sharding and routing operations.
 * `first_contact_points` - A list of string containing the IP address and port of the upstream redis nodes/service. 
 E.g. `first_contact_points: ["redis://127.0.0.1:2220/", "redis://127.0.0.1:2221/", "redis://127.0.0.1:2222/", "redis://127.0.0.1:2223/", "redis://127.0.0.1:2224/", "redis://127.0.0.1:2225/"]
`

Unlike other redis-cluster drivers, this Transform does support pipelining. It does however turn each command from the pipeline into a single
request/response operation, buffering results as within different Redis nodes as needed. Latency will be different from pipelining with a single
Redis node.

_Note: Currently Redis-cluster does not support the following functionality:_
* _Redis Transactions_



### MPSCForwarder
*State: Alpha*

This transform pushes the query/message to the channel associated with the topic named in its configuration. It will then return an
empty success response if it was able to write to the channel succesfully. 

* `topic_name` - A string with the topic name to push queries/messages into. E.g. `topic_name: testtopic`

## Standard Transforms
These Transforms will all perform some action on the query/message, before calling the down-chain transform. Optionally
they may also perform some action on the response returned by the transform down-chain.


### MPSCTee
*State: Alpha*

This transform asynchronously copies the query/message to the channel associated with the topic named in its configuration. It will then call the 
downstream transform.

* `topic_name` - A string with the topic name to push queries/messages into. E.g. `topic_name: testtopic`

### Route
*State: Alpha*

This transform will route a query to one of the sub-TransformChains defined in its configuration based on the route chosen by
a user defined Lua script. Once the transform has looked up the right chain, it will then call it and wait for its response.
If the transform cannot find a route in its route_map, it will default to calling the next chain in it's own TransformChain (not
it's route_map).

* `route_map` - A map of named chains. The name will be the lookup key in which the Route transform expects from the lua script.
 E.g. 
 ```yaml
route_map:
  main_cluster:
    - CodecDestination:
       remote_address: "127.0.0.1:9043"
  customer1_cluster:
   - CodecDestination:
      remote_address: "127.1.0.2:9043"
  customer2_cluster:
   - CodecDestination:
      remote_address: "127.2.0.3:9043"
 ```
* `route_script` - A ScriptConfigurator object that expects a `script_type`, `function_name` and `script_definition`.The route script can 
either by a Lua script or a WASM function. See [user defined functions](functions.md) for more details. The function expects a
`QueryMessage` (the request/query) and a `Vec<String>` (a list of possible routes) and needs to return a string containing the name of the chosen sub-chain. E.g.
```yaml
route_script:
  script_type: "lua"
  function_name: "route"
  script_definition: "
function route(queryMessage, routes)
   return 'main_cluster'
end
"
```

### Scatter
*State: Alpha*

This transform will route a query to one or more of the sub-TransformChains defined in its configuration based on the route chosen by
a user defined Lua script. Once the transform has looked up the right chains, it will then call each chain with a copy of the request/message
and wait for each response. The transform will then collate each responses values and return it as a FragmentedResponse (a list of responses)
to the up-chain transform.

If the transform cannot find a route in its route_map, it will default to calling the next chain in it's own TransformChain (not
it's route_map).

* `route_map` - A map of named chains. The name will be the lookup key in which the Route transform expects from the lua script.
 E.g. 
 ```yaml
route_map:
  main_cluster:
    - CodecDestination:
       remote_address: "127.0.0.1:9043"
  customer1_cluster:
   - CodecDestination:
      remote_address: "127.1.0.2:9043"
  customer2_cluster:
   - CodecDestination:
      remote_address: "127.2.0.3:9043"
 ```
* `route_script` - A ScriptConfigurator object that expects a `script_type`, `function_name` and `script_definition`.The route script can 
either by a Lua script or a WASM function. See [user defined functions](functions.md) for more details. The function expects a
`QueryMessage` (the request/query) and a `Vec<String>` (a list of possible routes) and needs to return array of strings (the chain names) to use. 
E.g.
```yaml
route_script:
  script_type: "lua"
  function_name: "route"
  script_definition: "
function route(queryMessage)
   return {'main_cluster', 'customer1_cluster'}
end
"
```

### Printer
*State: Beta*

This transform will log the query/message at an info level, then call the down-chain transform.


### RedisCache
*State: Alpha*

This transform will attempt to cache values for a given primary key in a redis hash set. It is a primarily implemented as a write
through cache. It currently expects an SQL based AST to figure out what to cache (e.g. CQL, PGSQL) and updates to the cache and the backing
datastore are performed sequentially. 

* `config_values` - A string with the redis connection url. E.g. `config_values: "redis://127.0.0.1/"`

### Protect
*State: Alpha*

This transform will encrypt specific fields before passing them down-chain, it will also decrypt those same fields from a 
response. The transform will create a data encryption key on an user defined basis (e.g. per primary key, per value, 
per table etc). 

The data encryption key is encrypted by a key encryption key and persisted alongside the encrypted value (alongside other 
needed cryptographic material). This transform provides the basis for in-application cryptography with unified key 
management between datastores. The encrypted value is serialised using bincode and should then be written to a blob or bytes
field by a down-chain transform. 

Fields are protected using a NaCL secretbox (xsalsa20-poly1305). Modification of the field is also detected and raised as 
an error. DEK protection is dependent on the key manager being used.

* `keyspace_table_columns` - A mapping of keyspaces, tables and columns to encrypt.
* `key_manager` - A KeyManagerConfig that configures the protect Transform with how to look up keys.

Currently the Protect transform supports AWS KMS and or using a local Key Encryption Key on disk. See [key management](keys.md)

Note: Currently the data encryption key ID function is just defined as a static string, this will be replaced by a user defined script
shortly. 

### TuneableConsistency
*State: Alpha*

This transform implements a distributed eventual consistent mechanism between the set of defined sub-chains. This transform
will wait for a user configurable number of chains to return an OK response before returning the value up-chain. This follows
a similar model as used by Cassandra for its consistency model. Strong consistency can be achieved when W + R > RF. In this case
RF is always the number of chains in the route_map. 

No sharding occurs within this transform and all requests/messages are sent to all routes. 

Upon receiving the configured number of responses, the transform will attempt to resolve or unify the response based on metadata
about the result. Currently it will try to return the newest response based on a metadata timestamp (last write wins) or it will
simply return the largest response if no timestamp information is available. 

* `route_map` - A map of named chains. All chains will be used in each request.
 E.g. 
 ```yaml
route_map:
  cluster1:
    - CodecDestination:
       remote_address: "127.0.0.1:9043"
  cluster2:
   - CodecDestination:
      remote_address: "127.1.0.2:9043"
  cluster3:
   - CodecDestination:
      remote_address: "127.2.0.3:9043"
 ```
* `write_consistency` - The number of chains to wait for a "write" response on.
* `read_consistency` - The number of chains to wait for a "read" response on.


### RedisTimeStampTagger
*State: Alpha*

A transform that wraps each redis command in a lua script that also fetches the key for the operations idletime. This is then
used to build a last modified timestamp and insert it into a responses timestamp. The response from the lua operation is unwrapped
and returned to up-chain transforms looking like a normal redis response. 

This is mainly used in conjunction with the `TuneableConsistency` to enable a Cassandra style consistency model within Redis.

No configuration is required for this transform.
 
### Lua
This transform executes a user defined lua function passing in the query/message. Within the global user environment you 
have access to the following:

* A function `call_next_transform` that calls the down-chain transforms and returns a response.
* CoRoutine std lib
* Table std lib
* IO std lib
* OS std lib
* string std lib
* Bit std lib
* math std lib
* package std lib

* `function_def` - The Lua script. The example script will change the namespace for the query 
(e.g. keyspace/table in cassandra or database/table in MySQL). E.g. 
```lua
qm.namespace = {"another_keyspace", "another_table"}
return call_next_transform(qm)
```
* `function_name` - The lua function to call, this can be blank and the lua script will just be run. The `QueryMessage` will be available as 
 a global. E.g. `function_name: ""` or `function_name: "my_function"`
 
 Note: Currently the Lua transform stores a new Lua VM on a per transform basis, so no state is shared between connections or other transforms,
 in the future, a Lua VM will likely be created on a per connection basis. The Lua transform will also likely be migrated over to using 
 ScriptHolders which will manage the script definition and Lua VM interaction. This will be likely released as a seperate transform and 
 also support WASM defined modules. 