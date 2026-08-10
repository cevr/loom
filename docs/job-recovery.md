# Job process recovery

Loom stores each live Job process in SQLite.

The record contains the PID, the process group ID, and the process start identity.
It also contains the output file paths and the recovery state.

The process start identity protects against PID reuse.
Loom does not adopt or signal a process when any identity field differs.

## Restart outcomes

| Observation                                         | Result                                 | Stored state         |
| --------------------------------------------------- | -------------------------------------- | -------------------- |
| The full identity matches.                          | The Job reconnects.                    | `Recovered`          |
| The PID is absent.                                  | The Job exited while Loom was offline. | `ExitedWhileOffline` |
| The PID exists with another PGID or start identity. | Loom does not adopt the process.       | `IdentityMismatch`   |
| Process inspection fails.                           | Loom tries again on the next restart.  | Unchanged            |

A Job command waits on a parent-owned input gate during launch.
Loom stores the process identity before it opens this gate.
An interrupted launch closes the gate and stops the process before the command starts.
Daemon startup changes every stale `Starting` claim to `Failed`.
It does not adopt a process from that incomplete launch.
The next Activity attempt can claim the launch again.
Each Job command starts in the Workspace root.

SQLite write errors remain failures.
Loom does not report a successful recovery when it cannot save the recovery state.

The daemon Scope owns one `FiberMap` of Job process monitors.
A new Job enters this map after Loom stores its identity and detaches its process handle.
A recovered Job enters the same map after an exact identity match.
The monitor records `Exited` when the process ends.
The monitor records `IdentityMismatch` when the PID identity changes.
Daemon shutdown interrupts all monitors before SQLite closes.
An inspection or SQLite update failure keeps its monitor active for the next pass.

## Live Pi test

Start Pi with the Loom extension.

```sh
bun run dev:pi
```

Get the Pi PID from Herdr.

```sh
herdr pane process-info --pane <pane-id>
```

Run the registration probe in one process.

```sh
LOOM_PROBE_PID=<pid> LOOM_PROBE_DB=/tmp/loom-pi.sqlite bun run probe:process-register
```

Run the reconciliation probe in a second process.

```sh
LOOM_PROBE_DB=/tmp/loom-pi.sqlite bun run probe:process-reconcile
```

The second process must report `Recovered` with the same PID, PGID, and start identity.
