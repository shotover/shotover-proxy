# User Facing Changelog

Any breaking changes to the `topology.yaml` or `shotover` rust API should be documented here.
This assists us in knowing when to make the next release a breaking release and assists users with making upgrades to new breaking releases.

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
