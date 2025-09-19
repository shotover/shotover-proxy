# Valkey Clustering with cluster aware client

The following guide shows you how to configure Shotover to support proxying Valkey cluster *aware* clients to [Valkey cluster](https://valkey.io/topics/cluster-spec).

## Overview

In this example, we will be connecting to a Valkey cluster that has the following topology:

* `172.16.1.2:6379`
* `172.16.1.3:6379`
* `172.16.1.4:6379`
* `172.16.1.5:6379`
* `172.16.1.6:6379`
* `172.16.1.7:6379`

Shotover will be deployed as a sidecar to each node in the Valkey cluster, listening on `6380`. Use the following [docker-compose.yaml](https://github.com/shotover/shotover-examples/blob/main/valkey-cluster-1-1/docker-compose.yaml) to run the Valkey cluster and Shotover sidecars.

```console
curl -L https://raw.githubusercontent.com/shotover/shotover-examples/main/valkey-cluster-1-1/docker-compose.yaml --output docker-compose.yaml
```

Below we can see an example of a Valkey node and it's Shotover sidecar. Notice they are running on the same network address (`172.16.1.2`) and the present directory is being mounted to allow Shotover to access the config and topology files.

```YAML

valkey-node-0:
  image: bitnamilegacy/valkey-cluster:7.2.5-debian-12-r4
  networks:
    cluster_subnet:
      ipv4_address: 172.16.1.2
  environment:
    - 'ALLOW_EMPTY_PASSWORD=yes'
    - 'VALKEY_NODES=valkey-node-0 valkey-node-1 valkey-node-2'

shotover-0:
  restart: always
  depends_on:
    - valkey-node-0
  image: shotover/shotover-proxy
  network_mode: "service:valkey-node-0"
  volumes:
    - type: bind
      source: $PWD
      target: /config

```

In this example we will use `valkey-benchmark` with cluster mode enabled as our Valkey cluster aware client application.

## Configuration

First we will modify our `topology.yaml` file to have a single Valkey source. This will:

* Define how Shotover listens for incoming connections from our client application (`valkey-benchmark`).
* Configure Shotover to connect to the Valkey node via our defined remote address.
* Configure Shotover to rewrite all Valkey ports with our Shotover port when the cluster aware driver is talking to the cluster, through Shotover.
* Connect our Valkey Source to our Valkey cluster sink (transform).

```yaml
---
sources:
  - Valkey:
      name: "valkey"
      listen_addr: "0.0.0.0:6380"
      chain:
        - ValkeyClusterPortsRewrite:
            new_port: 6380
        - ValkeySinkSingle:
            remote_address: "0.0.0.0:6379"
            connect_timeout_ms: 3000
```

Modify an existing `topology.yaml` or create a new one and place the above example as the file's contents.

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

With everything now up and running, we can test out our client application. Let's start it up!

First we will run `valkey-benchmark` directly on our cluster.

```console
valkey-benchmark -h 172.16.1.2 -p 6379 -t set,get --cluster 
```

If everything works correctly you should see the following, along with the benchmark results which have been omitted for brevity. Notice all traffic is going through the Valkey port on `6379`.

```console
Cluster has 3 master nodes:

Master 0: d5eaf45804215f80cfb661928c1a84e1da7406a9 172.16.1.3:6379
Master 1: d774cd063e430d34a71bceaab851d7744134e22f 172.16.1.2:6379
Master 2: 04b301f1b165d81d5fb86e50312e9cc4898cbcce 172.16.1.4:6379
```

Now run it again but on the Shotover port this time.

```console
valkey-benchmark -h 172.16.1.2 -p 6380 -t set,get --cluster 
```

You should see the following, notice that all traffic is going through Shotover on `6380` instead of the Valkey port of `6379`:

```console
Cluster has 3 master nodes:

Master 0: 04b301f1b165d81d5fb86e50312e9cc4898cbcce 172.16.1.4:6380
Master 1: d5eaf45804215f80cfb661928c1a84e1da7406a9 172.16.1.3:6380
Master 2: d774cd063e430d34a71bceaab851d7744134e22f 172.16.1.2:6380
```
