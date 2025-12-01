# Hot Reload

Hot reload enables zero-downtime updates by transferring network listeners between Shotover instances. When you start a new Shotover instance with hot reload enabled, it requests the listening TCP sockets from the running instance. Once they are received, the new instance immediately starts accepting connections. Once the new shotover instance is fully operational, it sends a shutdown request to the old shotover instace and the original instance gradually drains its existing connections before shutting down. This allows you to deploy configuration changes or upgrade Shotover versions without dropping client connections.


## How It Works

When a new Shotover instance starts with hot reload enabled, it checks if a Unix socket exists at the configured path. If the socket exists, the new Shotover starts in hot reload mode and attempts to hot reload from the old Shotover instance.

The hot reload process:

1. The new instance connects to the Unix socket and requests the listening TCP socket file descriptors.

2. The original instance transfers the file descriptors over the Unix socket using SCM_RIGHTS which is a Unix mechanism for sharing file descriptors between processes. After transferring, the original instance stops accepting new connections but continues to serve its existing connections.

3. The new instance recreates the TCP listeners from the file descriptors received from the old shotover instance. These listeners uses the same IP addresses and ports as before.

4. The new instance begins accepting new connections. At this point, the old instance still handles the existing connections. Once the new instance is fully functional, it sends a gradual shutdown request over the Unix socket to the old instance, specifying the shutdown duration.

5. The original instance drains its existing connections gradually in chunks (see Gradual Shutdown below).

6. Once all connections are drained, the original instance terminates and removes the Unix socket.

7. Once the old instance terminates, the new instance will be able to create its own Unix socket. Once this is done, the new instance will be ready to be hot reloaded in the future.

### Gradual Shutdown

If all the connections in the old shotover instance are closed at once, all clients will try to reconnect at the same time. This will be too much for the new instance to handle and will cause issues. Instead of closing all connections at once, the original instance drains connections in chunks, distributed evenly across the shutdown duration. By default, the shutdown duration is 60 seconds, but it can be configured with `--hotreload-gradual-shutdown-seconds`.

Connections are closed in chunks at fixed 200ms intervals. The chunk size is calculated to evenly distribute all connections across the total duration. When a connection is closed, the connection handler terminates and clients will detect the closure and tries to reconnect.

For example, with 1000 active connections and a 60-second shutdown duration:
- Total chunks: 60 seconds ÷ 0.2 seconds = 300 chunks
- Connections per chunk: ceiling(1000 ÷ 300) = 4 connections
- Every 200ms, 4 connections will be closed until all are drained

## Configuration

To enable hot reload, use the `--hotreload-socket` flag when starting Shotover:

```bash
shotover-proxy --hotreload-socket /tmp/shotover-hotreload.sock
```

You can also configure the gradual shutdown duration:

```bash
shotover-proxy \
  --hotreload-socket /tmp/shotover-hotreload.sock \
  --hotreload-gradual-shutdown-seconds 120
```

To perform a hot reload, start a second Shotover instance pointing to the same socket path:

```bash
shotover-proxy \
  --topology-file topology-v2.yaml \
  --config-file config-v2.yaml \
  --hotreload-socket /tmp/shotover.sock
```


## Considerations

### Connection State

Hot reload only transfers listening sockets, not active connections or session state. Existing connections remain with the original instance until drained. When connections are closed during shutdown, clients must reconnect to the new instance.

### Shutdown Duration

The configuration of gradual shutdown duration based on your connection volume and client behavior. By default, it will be 60 seconds. If it is not tuned correctly it can lead to various issues. If the duration is too short, it can lead to a situation where too many clients (more than the new instance could handle) try to reconnect to the new instance at once. If the duration is unnecessarily long, it will be a wastage of resources.


## Failure Modes

If the hot reload process fails (e.g., socket connection timeout, file descriptor transfer error):
- The new instance exits with an error
- The original instance remains running and unaffected
- No connections are disrupted
- Retry the hot reload after investigating the failure

If the original instance has crashed or is no longer running:
- The Unix socket will not exist or will not respond
- The new instance detects this and starts normally by binding to the configured ports
- This is a standard startup, not a hot reload


## Limitations

- Linux Only: Hot reload uses Unix domain sockets with the SEQPACKET socket type and file descriptor passing via SCM_RIGHTS. It might be possible to adapt this for other Unix systems but requires further investigation. Windows is not supported.

- Same Host Only: File descriptor transfer only works between processes on the same host. You cannot hot reload across network boundaries.
