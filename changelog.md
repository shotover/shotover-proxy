# User Facing Changelog

Any breaking changes to the `topology.yaml` or `shotover` rust API should be documented here.
This assists us in knowing when to make the next release a breaking release and assists users with making upgrades to new breaking releases.

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



### shotover rust api

## 0.1.11

### topology.yaml

* No recorded changes

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
