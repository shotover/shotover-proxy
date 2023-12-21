# User Facing Changelog

Any breaking changes to the `topology.yaml` or `shotover` rust API should be documented here.
This assists us in knowing when to make the next release a breaking release and assists users with making upgrades to new breaking releases.

## 0.3.0

### metrics

The prometheus metrics were renamed to better follow the official reccomended naming scheme: <https://prometheus.io/docs/practices/naming/>
This included ensuring all meterics were prefixed with `shotover_` and all metrics were suffixed with an appropriate `_unit`.
This includes:

* `shotover_transform_total` -> `shotover_transform_total_count`
* `shotover_transform_failures` -> `shotover_transform_failures_count`
* `shotover_transform_latency`  -> `shotover_transform_latency_seconds`
* `shotover_chain_total` -> `shotover_chain_total_count`
* `shotover_chain_failures`  -> `shotover_chain_failures_count`
* `shotover_chain_latency`  -> `shotover_chain_latency_seconds`
* `shotover_available_connections` -> `shotover_available_connections_count`
* `shotover_chain_failures` -> `shotover_chain_failures_count`
* `out_of_rack_requests`  -> `shotover_out_of_rack_requests_count`
* `client_protocol_version` -> `shotover_client_protocol_version_count`
* `failed_requests` -> `shotover_failed_requests_count`
* `query_count` -> `shotover_query_count`

## 0.2.0

### topology.yaml

#### Configuration of transform chains to sources change

The root level of the topology.yaml is completely overhauled.
We have not observed the source_to_chain_mapping ever being used, so to simplify the topology.yaml format root level chains have been inlined sources.

For example, a topology.yaml that looked like this:

```yaml
---
sources:
  redis_prod:
    Redis:
      listen_addr: "127.0.0.1:6379"
chain_config:
  redis_chain:
    - RedisSinkSingle:
        remote_address: "127.0.0.1:1111"
        connect_timeout_ms: 3000
source_to_chain_mapping:
  redis_prod: redis_chain
```

Should now be rewritten like this:

```yaml
---
sources:
  - Redis:
      name: "redis_prod"
      listen_addr: "127.0.0.1:6379"
      chain:
        - RedisSinkSingle:
            remote_address: "127.0.0.1:1111"
            connect_timeout_ms: 3000
```

#### Configuration options for Filter transform changed

The usage of the Filter transform has been changed to use either an allow list or deny list instead of just a deny list previously.

```yaml
    - QueryTypeFilter:
        # old config: 
        # filter: Read

        # new config:
        DenyList: [Read]
```

## 0.1.11

### shotover rust api

* `shotover::message_value` is now `shotover::frame::value`
* `shotover::message_value::MessageValue` is now `shotover::frame::value::GenericValue`

## 0.1.10

### topology.yaml

* source TLS configurations will now enable client authentication when the `certificate_authority_path` field is present.
  * Previously this field existed and was mandatory but had no affect on the TLS connection and client authentication was never enabled.
  * If you are getting TLS failures after upgrading remove the `certificate_authority_path` field.
    * Alternatively if you wish to use client authentication you can keep the field and setup your client to properly use client authentication.

### shotover rust api

* untracked
