# Use one daemon per Workspace

Loom uses one daemon for each Workspace.
The Workspace owns the daemon socket and the orchestration store.
The default socket path is `<workspace>/.loom/daemon.sock`.

A Client Adapter must complete a version handshake before it uses other Loom operations.
The handshake verifies the Workspace and selects the highest common protocol version.

A Client Adapter can start a missing daemon.
The daemon does not stop when that Client Adapter exits.
An explicit administration operation owns daemon stop.

This design makes Workspace routing explicit.
It also prevents one Workspace from receiving another Workspace's Session data.
