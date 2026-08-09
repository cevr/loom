# Daemon connection

Loom runs one daemon for each Workspace.

The default socket is `<workspace>/.loom/daemon.sock`.
Set `LOOM_SOCKET_PATH` to replace this path.
Set `LOOM_WORKSPACE_ROOT` to replace the process working directory.
Set `LOOM_DB_PATH` to replace the default SQLite path.

The Pi Client Adapter owns daemon start when no daemon answers.
The Workspace owns the running daemon.
A Pi exit does not stop the daemon.
An explicit administration action owns daemon stop.

The client sends `Connection.Handshake` before another operation.
The handshake checks the Workspace root and protocol version range.
The server selects the highest shared protocol version.
The server rejects a different Workspace.
The server rejects an incompatible protocol range.

Effect RPC provides the local Unix socket transport.
Effect RPC provides reconnect after a socket failure.
Loom retries a failed handshake within a bounded operation timeout.

The NDJSON parser limits one incomplete frame to 1 MiB.
The client limits Cell source to 1 MiB minus 4096 code units.
The reserve holds the RPC envelope and identifiers.

The socket routes the client to one Workspace.
Each RPC request carries its Session and Agent identifiers when needed.
The daemon does not infer Session identity from one socket connection.

The client returns `DaemonUnavailableError` when it cannot connect in time.
The handshake returns `WorkspaceMismatchError` for a wrong Workspace.
The handshake returns `IncompatibleProtocolError` for a version mismatch.
The client returns `MessageTooLargeError` before it sends oversized Cell source.

The daemon removes a stale socket before it starts.
The daemon rejects start when a live process owns the socket.
