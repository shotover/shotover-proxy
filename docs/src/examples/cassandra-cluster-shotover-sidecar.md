# Cassandra Cluster
  
The following guide shows you how to configure Shotover with support for proxying to a Cassandra Cluster.

## Overview

In this example, we will be connecting to a Cassandra cluster that has the following topology:

* `172.16.1.2:9042`
* `172.16.1.3:9042`
* `172.16.1.4:9042`

### Rewriting the peer ports

Shotover will be deployed as a sidecar to each node in the Cassandra cluster, listening on `9043`. Use the following [docker-compose.yml](https://raw.githubusercontent.com/shotover/shotover-proxy/cassandra-docs/shotover-proxy/example-configs-docker/cassandra-rewrite-peers/docker-compose.yml) to run the Cassandra cluster and Shotover sidecars. In this example we want to ensure that all our traffic to Cassandra goes through Shotover.

```console
curl -L https://raw.githubusercontent.com/shotover/shotover-proxy/main/shotover-proxy/example-configs-docker/cassandra-rewrite-peers/docker-compose.yml --output docker-compose.yml
```

Below we can see an example of a Cassandra node and it's Shotover sidecar, notice that they are running on the same network address (`172.16.1.2`) and the present directory is being mounted to allow Shotover to access the config and topology files.

```YAML
  cassandra-two:
    image: bitnami/cassandra:4.0
    networks:
      cassandra_subnet:
        ipv4_address: 172.16.1.3
    healthcheck: *healthcheck
    environment: *environment
    depends_on:
      - cassandra-one

  shotover-one:
    restart: always
    depends_on:
      - cassandra-two
    image: shotover/shotover-proxy
    network_mode: "service:cassandra-two"
    volumes:
      - type: bind
        source: $PWD
        target: /config
```

In this example we will use `cqlsh` to connect to our cluster.

#### Configuration

First we will modify our `topology.yaml` file to have a single Cassandra source. This will:

* Define how Shotover listens for incoming connections from our client (`cqlsh`).
* Configure Shotover to connect to the Cassandra node via our defined remote address.
* Configure Shotover to rewrite all Cassandra ports with our Shotover port when the client connects
* Connect our Cassandra source to our Cassandra sink (transform).

```yaml
{{#include ../../../shotover-proxy/example-configs-docker/cassandra-rewrite-peers/topology.yaml}}
```

Modify an existing `topology.yaml` or create a new one and place the above example as the file's contents.

You will also need a [config.yaml](https://raw.githubusercontent.com/shotover/shotover-proxy/main/shotover-proxy/config/config.yaml) to run Shotover.

```console
curl -L https://raw.githubusercontent.com/shotover/shotover-proxy/main/shotover-proxy/config/config.yaml --output config.yaml
```

#### Starting

We can now start the services with:

```console
docker-compose up -d
```

#### Testing

With everything now up and running, we can test it out with our client. Let's start it up!

First we will run `cqlsh` directly on our cluster with the command:

```console
cqlsh 172.16.1.2 9042 -u cassandra -p cassandra
```

and check the `system.peers_v2` table with the following query:

```sql
SELECT peer, native_port FROM system.peers_v2;
```

You should see the following results returned:

```console
 peer       | native_port
------------+-------------
 172.16.1.3 |        9042
 172.16.1.4 |        9042
```

Now run it again but on the Shotover port this time, run:

```console
cqlsh 172.16.1.2 9043 -u cassandra -p cassandra
```

and use the same query again. You should see the following results returned, notice how the `native_port` column is now the Shotover port of `9043`:

```console
 peer       | native_port
------------+-------------
 172.16.1.3 |        9043
 172.16.1.4 |        9043
```

If everything has worked, you will be able to use Cassandra, with your connection going through Shotover!
