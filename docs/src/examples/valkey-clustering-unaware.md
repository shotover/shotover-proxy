# Valkey Clustering

The following guide shows you how to configure Shotover Proxy to support transparently proxying Valkey cluster _unaware_ clients to a [Valkey cluster](https://valkey.io/topics/cluster-spec).

## General Configuration

First you need to setup a Valkey cluster and Shotover.

The easiest way to do this is with this example [docker-compose.yaml](https://github.com/shotover/shotover-examples/blob/main/valkey-cluster-1-many/docker-compose.yaml)
You should first inspect the `docker-compose.yaml` to understand what the cluster looks like and how its exposed to the network.

Then run:

```shell
curl -L https://raw.githubusercontent.com/shotover/shotover-examples/main/valkey-cluster-1-many/docker-compose.yaml --output docker-compose.yaml
```

Alternatively you could spin up a hosted Valkey cluster on [any cloud provider that provides it](https://www.instaclustr.com/products/managed-valkey).
This more accurately reflects a real production use but will take a bit more setup.
And reduce the docker-compose.yaml to just the shotover part

```yaml
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
  - Valkey:
      name: "valkey"
      # define where shotover listens for incoming connections from our client application (`valkey-benchmark`).
      listen_addr: "0.0.0.0:6379"
      chain:
        # configure Shotover to connect to the Valkey cluster via our defined contact points
        - ValkeySinkCluster:
            first_contact_points:
              - "172.16.1.2:6379"
              - "172.16.1.3:6379"
              - "172.16.1.4:6379"
              - "172.16.1.5:6379"
              - "172.16.1.6:6379"
              - "172.16.1.7:6379"
            connect_timeout_ms: 3000
```

Modify an existing `topology.yaml` or create a new one and place the above example as the file's contents.

If you didnt use the standard `docker-compose.yaml` setup then you will need to change `first_contact_points` to point to the Valkey instances you used.

You will also need a [config.yaml](https://raw.githubusercontent.com/shotover/shotover-examples/main/valkey-cluster-1-1/config.yaml) to run Shotover.

```shell
curl -L https://raw.githubusercontent.com/shotover/shotover-examples/main/valkey-cluster-1-1/config.yaml --output config.yaml
```

## Starting

We can now start the services with:

```shell
docker-compose up -d
```

## Testing

With your Valkey Cluster and Shotover now up and running, we can test out our client application. Let's start it up!

```console
valkey-benchmark -h 172.16.1.9 -t set,get
```
