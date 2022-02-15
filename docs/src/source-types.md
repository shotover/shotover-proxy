# Source Types

| Source Type                         | Implementation Status |
|-------------------------------------|-----------------------|
|[Cassandra](#cassandra)              |Alpha                  |
|[MPSC](#mpsc)                        |Alpha                  |
|[Redis](#redis)                      |Beta                   |

## Cassandra

```yaml
Cassandra:
  # The address to listen from.
  listen_addr: "127.0.0.1:6379"

  # The number of concurrent connections the source will accept.
  connection_limit: 1000

  # Defines the behaviour that occurs when Once the configured connection limit is reached:
  # * when true: the connection is dropped.
  # * when false: the connection will wait until a connection can be made within the limit.
  hard_connection_limit: false
```

## MPSC

```yaml
Mpsc:
  topic_name: "test_topic",

  coalesce_behavior:
    Count:
      10000
```

## Redis

```yaml
Redis:
  # The address to listen from
  listen_addr: "127.0.0.1:6379"

  # The number of concurrent connections the source will accept.
  connection_limit: 1000

  # Defines the behaviour that occurs when Once the configured connection limit is reached:
  # * when true: the connection is dropped.
  # * when false: the connection will wait until a connection can be made within the limit.
  hard_connection_limit: false

  # When this field is provided TLS is used when connecting to the remote address.
  # Removing this field will disable TLS.
  tls:
    # Path to the certificate file, typically named with a .crt extension.
    certificate_path: "tls/redis.crt"
    # Path to the private key file, typically named with a .key extension.
    private_key_path: "tls/redis.key"
    # Path to the certificate authority file typically named ca.crt.
    certificate_authority_path: "tls/ca.crt"
```
