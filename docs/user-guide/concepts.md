# Core Concepts

Shotover has a small number of core concepts or components that make up the bulk of it's architecture. Once understood, quite complex behaviour and environments can be managed with shotover-proxy.

## Source

A source is the main component that listens for traffic from your application and decodes it into an internal object that all Shotover transforms can understand. The source will then send the message to a TransformChain for processing / routing. 

## Transform

Transforms are where Shotover does the bulk of it's work. A transform is a single unit of operation that does something to the database request that's in flight. This may be logging it, modifying it, sending it to an external system or anything else you can think of. Transforms can either be terminating (pass messages on to subsequent transforms on the chain) or non-terminating (return a response without calling the rest of the chain). Transforms that send messages to external systems are called sinks. 

## Transform Chain

A transform chain is a ordered list of transforms that a message will pass through. Messages are received from a source. Transform chains can be of arbitary complexity and a transform can even have its own set of sub chains. Transform chains are defined by the user in shotovers configuration file and are linked to sources.


## Topology

A topology is how you configure Shotover. You define your sources, your transforms in a transform chain and then assign the chain to a source.

# Other concepts

## Topics

Shotover has a basic, built-in topic based messaging capability. A transform can access topic channels to publish messages on to. To receive messages, there is a message source that you can assign a chain to. This allows for complex routing and asynchronous passing of messages between transform chains in a topology. See [the cassandra and kafka example](/examples/cass-redis-kafka) as an example.

Generally if you want to build blocking behaviour in your chain, you will use transforms that have sub chains. For non-blocking behaviour (e.g. copying a query to a kafka queue while sending it the upstream service) use topic based transforms.
