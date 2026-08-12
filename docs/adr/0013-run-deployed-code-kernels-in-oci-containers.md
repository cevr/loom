# Run deployed Code Kernels in OCI containers

The local runner stays as defined in [ADR 0005](./0005-supervise-code-kernels-as-bun-processes.md).

A deployment runner must start one OCI container for each Code Kernel.
The runner must use Linux control groups through the OCI resource contract.
It must set a memory limit in bytes.
It must set the total memory and swap limit to the same value.
It must set a CPU quota and period.
It must keep the out-of-memory killer enabled.

The container keeps the existing Code Kernel entry point and JSONL protocol.
The deployment runner supplies its own launch adapter.
It does not overload the local executable setting.
The daemon does not inspect live process usage.
The container runtime owns resource enforcement for the full process tree.
The runner reads the control group `memory.events` file once after exit and before container removal.
It maps a positive `oom_kill` count to `MemoryLimitExceeded`.
The runner adds this reason to `CodeKernelProcessError` and `CellInterruptedError` when it implements the mapping.
It retains the exit code and stderr in `CodeKernelDiagnostic`.
It keeps the existing `TimedOut` failure for a Cell that exceeds its time limit.
CPU quota limits execution rate.
It does not create a separate CPU-exhaustion event.

The deployment runner owns the container identity and its terminal state.
The Loom daemon owns Code Kernel supervision and diagnostic storage.
The existing Cell interruption path carries the typed termination reason.
The existing diagnostic contract carries the exit code, stderr tail, and retained stderr path.

Process resource limits are not the deployment boundary.
They have different behavior across operating systems.
They do not give Loom one portable process-tree contract.

A custom resource polling monitor is not part of Loom.
Bun resource usage is useful after exit.
It is not an enforcement mechanism.

Loom does not need an Effect or Bun fork for this boundary.
The OCI runtime already owns the required limits.
