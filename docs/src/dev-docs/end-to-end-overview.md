# End to end overview

This document will use valkey as an example for explaining the end to end flow of messages through shotover.
The same flow within shotover is used for all protocols, so this document should still be useful if you are working with another protocol.

The general flow of messages though shotover looks like:

![
Client -> ValkeyCodec -> ValkeySource -> Some transform -> Another transform -> ValkeySinkCluster -> ValkeyCodec -> Valkey
](end-to-end-overview.png)

## The client

A user sends a valkey command through their client:

1. The user calls: `client.set("foo", "bar")`.
2. The client translates the `set(..)` arguments into a RESP request that looks like: ["SET", "foo", "bar"]
3. A hash is taken of the key "foo" which is used to choose which shotover node to send the request to.
4. The RESP request is converted into the RESP wire format, which is purely ascii except for user data: 
```
*3
$3
SET
$3
foo
$3
bar
```
`*3` means an array with 3 elements.
The first element is `$3\nSET`, which means a string of length 3 containing `SET`.
The second and third arguments are also strings of length 3: `$3\nfoo` and `$3\nbar`

5. The bytes of the message are sent over a TCP connection to the chosen shotover node. In this example, no such connection exists so a new one is made.

## Shotover accepts a new connection

When [ValkeySource](https://github.com/shotover/shotover-proxy/blob/de0d1a3fafb92cf1875dd9ca79b277faf3cb3e77/shotover/src/sources/valkey.rs#L54) is created during shotover startup, it creates a `TcpCodecListener` and then calls [TcpCodecListener::run](https://github.com/shotover/shotover-proxy/blob/de0d1a3fafb92cf1875dd9ca79b277faf3cb3e77/shotover/src/server.rs#L160) which listens in a background task for incoming TCP connections on the sources configured port.
`TcpCodecListener` accepts a new connection from the valkey client and constructs and runs a `Handler` type, which manages the connection.
The Handler type creates:
* read/write tasks around the TCP connection.
   + A `ValkeyEncoder` and `ValkeyDecoder` pair is created from [ValkeyCodecBuilder](https://github.com/shotover/shotover-proxy/blob/de0d1a3fafb92cf1875dd9ca79b277faf3cb3e77/shotover/src/server.rs#L449).
   + The `ValkeyEncoder` is given to the [write task](https://github.com/shotover/shotover-proxy/blob/de0d1a3fafb92cf1875dd9ca79b277faf3cb3e77/shotover/src/server.rs#L517)
   + The `ValkeyDecoder` is given to the [read task](https://github.com/shotover/shotover-proxy/blob/de0d1a3fafb92cf1875dd9ca79b277faf3cb3e77/shotover/src/server.rs#L467)
* a new [transform chain](https://github.com/shotover/shotover-proxy/blob/de0d1a3fafb92cf1875dd9ca79b277faf3cb3e77/shotover/src/server.rs#L208) instance to handle the requests coming in from this connection.
   + This transform chain instance handles a single connection passing from the client to valkey and isolates it from other connections.

The handler type then [continues to run](https://github.com/shotover/shotover-proxy/blob/de0d1a3fafb92cf1875dd9ca79b277faf3cb3e77/shotover/src/server.rs#L677), routing requests and responses between the transform chain and the client connection read/write tasks.

## ValkeyDecoder

The `tokio_util` crate provides an [Encoder](https://docs.rs/tokio-util/latest/tokio_util/codec/trait.Encoder.html) trait and a [Decoder](https://docs.rs/tokio-util/latest/tokio_util/codec/trait.Decoder.html) trait.

Through this interface:
* we provide the logic for how to encode and decode messages into and out of a buffer of bytes by implementing the traits.
* tokio provides the logic for reading and writing the bytes from the actual TCP connection via the `FramedWrite` and `FramedRead` types.

Since TCP itself does not provide any kind of framing it is up to the database protocol itself to implement framing on top of TCP.
So the logic of a Decoder implementation must gracefully handle incomplete messages. Leaving any half received messages in the buffer.

The `ValkeyDecoder` is an example of a `Decoder` implementation.

TODO: Step through the ValkeyDecoder implementation

## incoming read write tasks

## Some Transform

## Another Transform

## ValkeySinkCluster

## SinkConnection

## ValkeyEncoder

## ValkeyDecoder