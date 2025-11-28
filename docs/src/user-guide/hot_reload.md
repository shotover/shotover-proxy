# Hot Reload

Hot reload enables zero-downtime updates of Shotover instances by transferring active network listeners between processes. This allows you to deploy configuration changes, upgrade Shotover versions, or restart the proxy without dropping client connections.

## Overview

When hot reload is enabled, Shotover instances communicate over a Unix socket to coordinate the handoff of listening TCP sockets. The new Shotover instance inherits the network listeners from the running instance, allowing it to immediately start accepting connections on the same ports. Once the handoff is complete, the original instance gradually drains its active connections before shutting down.

## How It Works

### Socket Handoff Process

Hot reload operates through a multi-step coordination process between the original (old) and replacement (new) Shotover instances:

1. **Detection**: When a new Shotover instance starts with hot reload enabled, it checks if a Unix socket exists at the configured path. If the socket exists, another Shotover instance is already running.

2. **Connection**: The new instance connects to the Unix socket and sends a request for the listening TCP socket file descriptors.

3. **File Descriptor Transfer**: The original instance extracts the file descriptors of its active TCP listeners and transfers them to the new instance over the Unix socket using ancillary data (SCM_RIGHTS). This is a special Unix mechanism that allows processes to share open file descriptors. Once the file descriptors are transferred, the original instance stops accepting new connections.

4. **Listener Reconstruction**: The new instance receives the file descriptors and reconstructs fully functional TCP listeners from them. These listeners are bound to the same IP addresses and ports as the original listeners.

5. **Service Start**: The new instance begins accepting new connections on the transferred listeners. The old instance is handling only its existing connections (no longer accepting new ones), while the new instance accepts all new connections.

6. **Gradual Shutdown Request**: Once the new instance is fully operational, it sends a shutdown request to the original instance over the Unix socket.

7. **Connection Draining**: The original instance enters a gradual shutdown phase where it begins draining its existing connections.

8. **Cleanup**: Once all connections are drained, the original instance terminates, and the Unix socket is removed.

9. **Server Mode**: The new instance creates its own Unix socket at the same path, making itself ready to be hot reloaded in the future.

### Gradual Shutdown

The gradual shutdown process ensures that existing client connections are handled gracefully without abrupt termination:

- **Chunked Draining**: Instead of closing all connections at once, the original instance drains connections in small batches distributed evenly across the shutdown duration.

- **Configurable Duration**: You can configure how long the shutdown process takes (default: 60 seconds) using the `--hotreload-gradual-shutdown-seconds` flag.

- **Fixed Intervals**: Connections are closed in chunks at fixed 200ms intervals. The number of connections closed per chunk is calculated to distribute the load evenly.

- **Connection Termination**: Each connection receives a shutdown signal. The connection handler stops accepting new requests and terminates, which closes the connection from Shotover's side. Clients will detect the connection closure and need to reconnect.

For example, if you have 1000 active connections and set a 60-second shutdown duration:
- Total chunks: 60 seconds ÷ 0.2 seconds = 300 chunks
- Connections per chunk: ceiling(1000 ÷ 300) = 4 connections
- Every 200ms, 4 connections will be closed (until all are drained)

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


### Hot Reload to New Configuration

To deploy a new configuration or version, start a second Shotover instance pointing to the same socket path but with updated configuration files:

```bash
shotover-proxy \
  --topology-file topology-v2.yaml \
  --config-file config-v2.yaml \
  --hotreload-socket /tmp/shotover.sock
```

The new instance will:
1. Connect to the existing instance via `/tmp/shotover.sock`
2. Request and receive the listening socket file descriptors
3. Begin accepting new connections immediately
4. Request the original instance to shut down gracefully
5. Take over as the primary instance


### State

**Connection State**: Each Shotover connection handler maintains its own state. When connections are drained during shutdown:
- Connection handler state (such as pending request tracking) is lost when the connection closes
- Clients must re-establish connections to the new instance
- The new instance starts with fresh transform chains and their own internal state

**No Session Transfer**: Hot reload transfers listening sockets only, not active connections or session state. Existing connections remain with the original instance until they are drained.


### Shutdown Duration

**Connection Volume**: The gradual shutdown duration should be tuned based on your connection volume and client behavior:
- **Too Short**: Aggressive connection draining may cause connection churn and reconnection storms
- **Too Long**: Extended shutdown means the original instance consumes resources longer

**Client Reconnection**: Clients must handle connection closures gracefully:
- Implement connection retry logic


## Failure Modes

### Hot Reload Failure

If the hot reload process fails (e.g., socket connection timeout, file descriptor transfer error):
- The new instance will exit with an error
- The original instance remains running and unaffected
- No connections are disrupted
- You can retry the hot reload after investigating the failure

### Original Instance Unavailable

If the original instance has crashed or is no longer running when you attempt a hot reload:
- The Unix socket will not exist or will not respond
- The new instance will detect this and start normally by binding to the configured ports
- No hot reload occurs - this is a standard startup


## Limitations

- **Linux Only**: Hot reload uses Unix domain sockets with the SEQPACKET socket type. This socket type, combined with Unix-specific file descriptor passing via SCM_RIGHTS ancillary data. It might be possible to adapt the underlying FD passing mechanism for other unixes but requires further investigation. It is not at all supported on Windows.

- **Same Host Only**: File descriptor transfer only works between processes on the same host. Hot reload cannot transfer listeners across network boundaries.
