# Job recovery

Loom stores every Job in the `jobs` table.

The Job record contains its lifecycle state.
It also contains its command, Session owner, attachment mode, output paths, result path, and process identity.

The process identity contains the PID, process group ID, and process start value.
The start value protects against PID reuse.
Loom does not adopt or signal a process when an identity field differs.

## Launch commit

Loom writes an `Accepted` Job before it starts a process.
The Job changes to `Starting` before the process starts.
The process waits on a parent-owned input gate.
Loom stores the full process identity before it opens this gate.
The Job then changes to `Running`.

An interrupted setup closes the process Scope or the input pipe before the gate opens.
The command does not start.
Loom records a `Failed` outcome.

The detached shell writes standard output and standard error to files from process start.
It writes the exit code to a temporary result file.
It then renames the file to the durable result path.

## Restart outcomes

| Durable state           | Observation                             | Result                                                     |
| ----------------------- | --------------------------------------- | ---------------------------------------------------------- |
| `Accepted`              | No launch owns the Job.                 | Loom starts the Job.                                       |
| `Starting`              | The launch did not commit.              | Loom records `Failed`.                                     |
| `Running`               | The full process identity matches.      | Loom monitors the process.                                 |
| `Stopping`              | The full process identity matches.      | Loom resumes cancellation.                                 |
| `Running`               | The PID is absent and a result exists.  | Loom records `Succeeded` or `Failed`.                      |
| `Running`               | The PID is absent and no result exists. | Loom records `Lost`.                                       |
| `Stopping`              | The PID is absent.                      | Loom records `Cancelled`.                                  |
| `Running` or `Stopping` | The identity differs.                   | Loom records `Lost`. It does not signal the process.       |
| `Running` or `Stopping` | Inspection fails.                       | Loom leaves the Job active. A later restart can try again. |

The daemon Scope owns one `FiberMap` of Job supervisors.
A live process waits in this map through its Effect child-process handle.
A recovered process uses the same map and a bounded inspection schedule.
Daemon shutdown interrupts the supervisors.
It does not stop detached Jobs.

## Cancellation

Cancellation changes an active Job to `Stopping`.
Loom verifies the full process identity before `SIGTERM`.
It sends `SIGTERM` to the process group.
It monitors the process group after the leader exits.
It sends `SIGKILL` after five seconds when any group member still exists.
It then records `Cancelled`.

Closing a Session cancels its attached Jobs.
A detached Job survives Session close.
