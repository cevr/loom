# Code Kernel recovery

Loom starts one Bun process for each active Code Kernel.
The daemon supervises that process.
The process owns one persistent TypeScript evaluation context.

The daemon and process exchange typed JSONL frames.
Each frame has a request ID.
The worker handles one request at a time.
The daemon rejects a response with a different request ID.
The worker sends a typed ready frame after startup.
The daemon uses a separate startup limit.
The Cell limit starts after the ready frame.
The ready frame has a 10-second limit by default.

A Cell has a 30-second execution limit by default.
Long work must become a Job.
The orchestrator can set a different limit.

A compilation error does not replace the process.
An execution error does not replace the process.
A timeout replaces the process.
A process exit replaces the process.
A protocol failure replaces the process.
The next Cell starts a new process.

The daemon opens a crash breaker after three replacements in 30 seconds.
The breaker stays open for 30 seconds.
A timeout counts as a replacement failure.

The daemon keeps the last 65,536 stderr characters in memory.
It stores at most 1 MiB of stderr for each worker.
A truncated file gives priority to the final stderr tail.
It keeps a marker and the available start bytes.
It keeps at most 20 stderr files for each Agent.
It keeps at most 256 stderr files in total.
Global cleanup can remove a recent Agent file before the per-Agent limit.
The orchestrator can set these limits.
The process failure contains available exit and stderr details.

The daemon limits a Cell display to 65,536 characters.
It limits a Cell result to 1,024 binding names.

The daemon sends `SIGTERM` during replacement.
Effect sends the force-kill signal after one second.

Request IDs start at one for each worker process.
An interrupted Cell closes its worker before the next Cell starts.

The daemon stays alive when a Code Kernel fails.
The daemon does not replay old Cells into the replacement process.
Replay can repeat file, network, and process effects.

The daemon owns the Cell Ledger.
The child process does not own the ledger.
The daemon stores Cell source before evaluation.
The Cell Ledger survives process replacement.
Mutable bindings do not survive process replacement.

The daemon stores one Process Identity for each Session ID and Agent ID.
It stores the PID, process group ID, and process start ID after the ready frame.
It stores this identity before the first Cell request.
It removes the exact record after the process exits.

The Loom orchestrator gives an idle Agent a five-minute lease by default.
Each Agent message restarts the lease.
The same scoped Code Kernel keeps its live bindings during this lease.
Effect Cluster passivation closes the exact process after the lease ends.

Daemon startup inspects every stored identity.
It sends `SIGKILL` only when all identity fields match.
It waits for that process group to stop before it removes the record.
It removes a record when the process is absent.
It removes a record when the PID now identifies a different process.
It never signals that different process.

The process boundary is a recovery boundary.
It is not a security boundary.
See [Code Kernel limits](./adr/0007-bound-code-kernel-failures-and-diagnostics.md).
