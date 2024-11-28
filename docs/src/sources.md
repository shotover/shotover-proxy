# Sources

| Source                              | Implementation Status |
|-------------------------------------|-----------------------|
|[Cassandra](#cassandra)              |Beta                   |
|[Valkey](#valkey)                    |Beta                   |

## Cassandra

```yaml
Cassandra:
  # The address to listen from.
  listen_addr: "127.0.0.1:6379"

  # The number of concurrent connections the source will accept.
  # If not provided defaults to 512
  connection_limit: 512

  # Defines the behaviour that occurs when Once the configured connection limit is reached:
  # * when true: the connection is dropped.
  # * when false: the connection will wait until a connection can be made within the limit.
  # If not provided defaults to false
  hard_connection_limit: false

  # When this field is provided TLS is used when the client connects to Shotover.
  # Removing this field will disable TLS.
  #tls:
  #  # Path to the certificate file, typically named with a .crt extension.
  #  certificate_path: "tls/localhost.crt"
  #  # Path to the private key file, typically named with a .key extension.
  #  private_key_path: "tls/localhost.key"
  #  # Path to the certificate authority file, typically named with a .crt extension.
  #  # When this field is provided client authentication will be enabled.
  #  #certificate_authority_path: "tls/localhost_CA.crt"
 
  # Timeout in seconds after which to terminate an idle connection. This field is optional, if not provided, idle connections will never be terminated.
  # timeout: 60

  # The transport that cassandra communication will occur over.
  # TCP is the only Cassandra protocol conforming transport.
  transport: Tcp
  
  # alternatively:
  #
  # Use the Cassandra protocol over WebSockets using a Shotover compatible driver.
  # transport: WebSocket

  chain:
    Transform1
    Transform2
    ...
```

## Valkey

```yaml
Valkey:
  # The address to listen from
  listen_addr: "127.0.0.1:6379"

  # The number of concurrent connections the source will accept.
  # If not provided defaults to 512
  connection_limit: 512

  # Defines the behaviour that occurs when Once the configured connection limit is reached:
  # * when true: the connection is dropped.
  # * when false: the connection will wait until a connection can be made within the limit.
  # If not provided defaults to false
  hard_connection_limit: false

  # When this field is provided TLS is used when the client connects to Shotover.
  # Removing this field will disable TLS.
  #tls:
  #  # Path to the certificate file, typically named with a .crt extension.
  #  certificate_path: "tls/valkey.crt"
  #  # Path to the private key file, typically named with a .key extension.
  #  private_key_path: "tls/valkey.key"
  #  # Path to the certificate authority file typically named ca.crt.
  #  # When this field is provided client authentication will be enabled.
  #  #certificate_authority_path: "tls/ca.crt"
    
  # Timeout in seconds after which to terminate an idle connection. This field is optional, if not provided, idle connections will never be terminated.
  # timeout: 60

  chain:
    Transform1
    Transform2
    ...
```

## Kafka

```yaml
Kafka:
  # The address to listen from
  listen_addr: "127.0.0.1:6379"

  # The number of concurrent connections the source will accept.
  # If not provided defaults to 512
  connection_limit: 512

  # Defines the behaviour that occurs when Once the configured connection limit is reached:
  # * when true: the connection is dropped.
  # * when false: the connection will wait until a connection can be made within the limit.
  # If not provided defaults to false
  hard_connection_limit: false

  # When this field is provided TLS is used when the client connects to Shotover.
  # Removing this field will disable TLS.
  #tls:
  #  # Path to the certificate file, typically named with a .crt extension.
  #  certificate_path: "tls/localhost.crt"
  #  # Path to the private key file, typically named with a .key extension.
  #  private_key_path: "tls/localhost.key"
  #  # Path to the certificate authority file, typically named with a .crt extension.
  #  # When this field is provided client authentication will be enabled.
  #  #certificate_authority_path: "tls/localhost_CA.crt"

  # Timeout in seconds after which to terminate an idle connection. This field is optional, if not provided, idle connections will never be terminated.
  # timeout: 60

  chain:
    Transform1
    Transform2
    ...
```
