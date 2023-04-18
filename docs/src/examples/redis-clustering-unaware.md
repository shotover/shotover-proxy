# Redis Clustering

The following guide shows you how to configure Shotover Proxy to support transparently proxying Redis cluster _unaware_ clients to a [Redis cluster](https://redis.io/topics/cluster-spec).

## General Configuration

First you need to setup a Redis cluster and Shotover.

The easiest way to do this is with this example [docker-compose.yaml](https://github.com/shotover/shotover-examples/blob/main/redis-cluster-1-many/docker-compose.yaml)
You should first inspect the `docker-compose.yaml` to understand what the cluster looks like and how its exposed to the network.

Then run:

```shell
curl -L https://raw.githubusercontent.com/shotover/shotover-examples/main/redis-cluster-1-many/docker-compose.yaml --output docker-compose.yaml
```

Alternatively you could spin up a hosted Redis cluster on [any cloud provider that provides it](https://www.instaclustr.com/products/managed-redis).
This more accurately reflects a real production use but will take a bit more setup.
And reduce the docker-compose.yaml to just the shotover part

```yaml
version: '3.3'
services:
  shotover-0:
    networks:
      cluster_subnet:
        ipv4_address: 172.16.1.9
    image: shotover/shotover-proxy:v0.1.10
    volumes:
      - .:/config
networks:
  cluster_subnet:
    name: cluster_subnet
    driver: bridge
    ipam:
      driver: default
      config:
        - subnet: 172.16.1.0/24
          gateway: 172.16.1.1
```

## Shotover Configuration

```yaml
---
sources:
  redis_prod:
    # define how shotover listens for incoming connections from our client application (`redis-benchmark`).
    Redis:
      listen_addr: "0.0.0.0:6379"
chain_config:
  redis_chain:
    # configure Shotover to connect to the Redis cluster via our defined contact points
    - RedisSinkCluster:
        first_contact_points:
          - "172.16.1.2:6379"
          - "172.16.1.3:6379"
          - "172.16.1.4:6379"
          - "172.16.1.5:6379"
          - "172.16.1.6:6379"
          - "172.16.1.7:6379"
        connect_timeout_ms: 3000
source_to_chain_mapping:
  # connect our Redis source to our Redis cluster sink (transform).
  redis_prod: redis_chain
```

Modify an existing `topology.yaml` or create a new one and place the above example as the file's contents.

If you didnt use the standard `docker-compose.yaml` setup then you will need to change `first_contact_points` to point to the Redis instances you used.

You will also need a [config.yaml](https://raw.githubusercontent.com/shotover/shotover-examples/main/redis-cluster-1-1/config.yaml) to run Shotover.

```shell
curl -L https://raw.githubusercontent.com/shotover/shotover-examples/main/redis-cluster-1-1/config.yaml --output config.yaml
```

## Starting

We can now start the services with:

```shell
docker-compose up -d
```

## Testing

With your Redis Cluster and Shotover now up and running, we can test out our client application. Let's start it up!

```console
redis-benchmark -h 172.16.1.9 -t set,get
```

Running against local containerised Redis instances on a Ryzen 9 3900X we get the following:

```console
user@demo ~$ redis-benchmark -t set,get
====== SET ======                                                     
  100000 requests completed in 0.69 seconds
  50 parallel clients
  3 bytes payload
  keep alive: 1
  host configuration "save": 
  host configuration "appendonly": 
  multi-thread: no

Latency by percentile distribution:
0.000% <= 0.079 milliseconds (cumulative count 2)
50.000% <= 0.215 milliseconds (cumulative count 51352)
75.000% <= 0.231 milliseconds (cumulative count 79466)
87.500% <= 0.247 milliseconds (cumulative count 91677)
93.750% <= 0.255 milliseconds (cumulative count 94319)
96.875% <= 0.271 milliseconds (cumulative count 97011)
98.438% <= 0.303 milliseconds (cumulative count 98471)
99.219% <= 0.495 milliseconds (cumulative count 99222)
99.609% <= 0.615 milliseconds (cumulative count 99613)
99.805% <= 0.719 milliseconds (cumulative count 99806)
99.902% <= 0.791 milliseconds (cumulative count 99908)
99.951% <= 0.919 milliseconds (cumulative count 99959)
99.976% <= 0.967 milliseconds (cumulative count 99976)
99.988% <= 0.991 milliseconds (cumulative count 99992)
99.994% <= 1.007 milliseconds (cumulative count 99995)
99.997% <= 1.015 milliseconds (cumulative count 99998)
99.998% <= 1.023 milliseconds (cumulative count 99999)
99.999% <= 1.031 milliseconds (cumulative count 100000)
100.000% <= 1.031 milliseconds (cumulative count 100000)

Cumulative distribution of latencies:
0.007% <= 0.103 milliseconds (cumulative count 7)
33.204% <= 0.207 milliseconds (cumulative count 33204)
98.471% <= 0.303 milliseconds (cumulative count 98471)
99.044% <= 0.407 milliseconds (cumulative count 99044)
99.236% <= 0.503 milliseconds (cumulative count 99236)
99.571% <= 0.607 milliseconds (cumulative count 99571)
99.793% <= 0.703 milliseconds (cumulative count 99793)
99.926% <= 0.807 milliseconds (cumulative count 99926)
99.949% <= 0.903 milliseconds (cumulative count 99949)
99.995% <= 1.007 milliseconds (cumulative count 99995)
100.000% <= 1.103 milliseconds (cumulative count 100000)

Summary:
  throughput summary: 144092.22 requests per second
  latency summary (msec):
          avg       min       p50       p95       p99       max
        0.222     0.072     0.215     0.263     0.391     1.031
====== GET ======                                                     
  100000 requests completed in 0.69 seconds
  50 parallel clients
  3 bytes payload
  keep alive: 1
  host configuration "save": 
  host configuration "appendonly": 
  multi-thread: no

Latency by percentile distribution:
0.000% <= 0.079 milliseconds (cumulative count 1)
50.000% <= 0.215 milliseconds (cumulative count 64586)
75.000% <= 0.223 milliseconds (cumulative count 77139)
87.500% <= 0.239 milliseconds (cumulative count 90521)
93.750% <= 0.255 milliseconds (cumulative count 94985)
96.875% <= 0.287 milliseconds (cumulative count 97262)
98.438% <= 0.311 milliseconds (cumulative count 98588)
99.219% <= 0.367 milliseconds (cumulative count 99232)
99.609% <= 0.495 milliseconds (cumulative count 99613)
99.805% <= 0.583 milliseconds (cumulative count 99808)
99.902% <= 0.631 milliseconds (cumulative count 99913)
99.951% <= 0.647 milliseconds (cumulative count 99955)
99.976% <= 0.663 milliseconds (cumulative count 99978)
99.988% <= 0.679 milliseconds (cumulative count 99990)
99.994% <= 0.703 milliseconds (cumulative count 99995)
99.997% <= 0.711 milliseconds (cumulative count 99997)
99.998% <= 0.751 milliseconds (cumulative count 99999)
99.999% <= 0.775 milliseconds (cumulative count 100000)
100.000% <= 0.775 milliseconds (cumulative count 100000)

Cumulative distribution of latencies:
0.009% <= 0.103 milliseconds (cumulative count 9)
48.520% <= 0.207 milliseconds (cumulative count 48520)
98.179% <= 0.303 milliseconds (cumulative count 98179)
99.358% <= 0.407 milliseconds (cumulative count 99358)
99.626% <= 0.503 milliseconds (cumulative count 99626)
99.867% <= 0.607 milliseconds (cumulative count 99867)
99.995% <= 0.703 milliseconds (cumulative count 99995)
100.000% <= 0.807 milliseconds (cumulative count 100000)

Summary:
  throughput summary: 143884.89 requests per second
  latency summary (msec):
          avg       min       p50       p95       p99       max
        0.214     0.072     0.215     0.263     0.335     0.775
```
