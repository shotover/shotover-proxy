# User Facing Changelog

Any breaking changes to the `topology.yaml` or `shotover` rust API should be documented here.
This assists us in knowing when to make the next release a breaking release and assists users with making upgrades to new breaking releases.

## 0.5.0

### shotover rust API

`Transform::transform` now takes `&mut Wrapper` instead of `Wrapper`.

## 0.4.0

### shotover rust API

#### Debug/Clone

`TransformBuilderAndMetrics`, `TransformChainBuilder`, the `TransformBuilder` trait, as well as many `Transform` structs and `TransformBuilder` structs no longer implement `Derive` or `Clone`.

#### Transform trait

The transform invariants have been changed significantly.
A system of message IDs is now used to match responses to requests instead of relying on the message order.
For full details refer to the [documentation of the invariants](https://github.com/shotover/shotover-proxy/blob/204d315b769e300176dea137dff047a369022498/shotover/src/transforms/mod.rs).

The `transform_pushed` method has been removed.
The messages that were previously sent through that method will now go through the regular `transform` method.

#### TransformConfig trait

Mandatory methods `TransformConfig::up_chain_protocol` and `TransformConfig::down_chain_protocol` are added.
Implement these to define what protocols your transform requires and outputs.

## 0.3.0

### shotover rust API

`TransformBuilder::build` now returns `Box<dyn Transform>` instead of `Transforms`.
This means that custom transforms should implement the builder as:

```rust
impl TransformBuilder for CustomBuilder {
  fn build(&self) -> Box<dyn Transform> {
    Box::new(CustomTransform::new())
  }
}
```

Instead of:

```rust
impl TransformBuilder for CustomBuilder {
  fn build(&self) -> Transforms {
    Transforms::Custom(CustomTransform::new())
  }
}
```

### metrics

The prometheus metrics were renamed to better follow the official recommended naming scheme: <https://prometheus.io/docs/practices/naming/>
This included ensuring all metrics were prefixed with `shotover_` and all metrics were suffixed with an appropriate `_unit`.
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
