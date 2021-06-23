# Core Concepts
Shotover has a small number of core concepts or components that make up the bulk of it's architecture. Once understood, 
quite complex behaviour and environments can be managed with shotover-proxy.

## Source
A source is the main component that listens for traffic from your application and decodes it into an internal object that
all shotover transforms can understand. Under the hood it consists of an open socket and a thread that listens for messages
and converts them via a Codec. 

If you want to implement your own source. You will generally need to build the following:
* A Tokio codec that returns shotover `Messages` - See the [Cassandra codec](../../src/protocols/cassandra_protocol2.rs) as an example.
* A configuration struct that can be deserialised from YAML and generate and run a TcpCodecListener configured with your
Codec (The trait to implement is `SourcesFromConfig`). - See the [Cassandra source](../../src/sources/cassandra_source.rs) as an example

With these two in place, shotover can generally wire-in any transform chain to your Source. To support passing messages
to the upstream database (e.g. the database your application would normally talk to directly), you would implement a 
`Transform` that opens a connection upstream and uses your Tokio codec to convert shotover `Messages` to the correct protocol
representation. 
 
## Transform
Transforms a where the bulk of shotover does its work. A transform is a struct that implements the `Transform` trait. The trait
has one function where you implement the majority of your logic (transfrom), however it also includes a setup and naming method:

```rust
#[async_trait]
pub trait Transform: Send {
 async fn transform<'a>(&'a mut self, message_wrapper: Wrapper<'a>) -> ChainResponse;

 fn get_name(&self) -> &'static str;

 async fn prep_transform_chain(&mut self, _t: &mut TransformChain) -> Result<()> {
  Ok(())
 }
}
``` 
- Wrapper (message_wrapper) contains the Query/Message you want to operate on. 
- The transform chain (t) is the ordered list of transforms operating on message. 

To call the downstream transform, simply call: 
```rust
#[async_trait]
impl Transform for NoOp {
 async fn transform<'a>(&'a mut self, message_wrapper: Wrapper<'a>) -> ChainResponse {
  message_wrapper.call_next_transform().await
 }

 fn get_name(&self) -> &'static str {
  self.name
 }
}
```
This will return a ChainResponse which will include the upstream databases reponse. This means
your transform can operate on both queries and responses. Once your transform is done handling the request and response, it will return 
passing control to the upstream transform. 

## TransformChain
A transform chain is a ordered list of transforms that a message will pass through. Transform chains can be of arbitary complexity 
and a transform can even have its own set of child transform chains. Transform chains are defined by the user in shotovers
configuration file and are linked to sources. 

The transform chain is a vector of mutable references to the enum Transforms (which is an enum dispatch wrapper around the various transform types).
 
 ## Topology
 A topology is the final constructed set of transforms, transformchains and sources in their final state, ready to receive messages.
 
# Other concepts
## Topics
When a transform is first created, or cloned for a new connection. It gets access to a topic holder. A topic holder provides access
to a map of multi-producer, single consumer channels. The transform can only access the sender part of the channel and can freely
clone and share it as it sees fit. 
A special source type called an MPSC source gets access to receiver side of the channel. This source can have a transform chain attached to it
like any other source. This allows for complex routing and asynchonous passing of messages between trnasform chains in a topology.
See [the cassandra and kafka example](/examples/cass-redis-kafka) as an example.

Generally if you want to build blocking behaviour in your chain, you will use transforms that have child transform chains.
For non-blocking behaviour (e.g. copying a query to a kafka queue while sending it the upstream service) use topic based transforms.

## Scripts
Some transforms let you specify a script that gets called by the transform to define its internal behaviour. Currently shotover
supports Lua (5.3) with std libs enabled and WebAssembly (WASI ABI). For Lua scripts, you can define the script itself and the entry
function to call within the yaml file itself. For WebAssembly you can define the location of your compiled wasm file and the entry point
to call. Each transform will document the function signature you need to implement.

Script support is currently rudimentary, but fairly performant. A no-op transform in rust generally takes up a few NS of cpu time, 
a no-op Lua script adds about 50NS to the critical path. 