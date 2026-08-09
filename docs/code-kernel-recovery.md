# Code Kernel recovery

Loom starts one Bun process for each active Code Kernel.
The daemon supervises that process.
The process owns one persistent TypeScript evaluation context.

The daemon and process exchange typed JSONL frames.
Each frame has a request ID.
The worker handles one request at a time.
The daemon rejects a response with a different request ID.

A Cell has a 30-second execution limit by default.
Long work must become a Job.
The orchestrator can set a different limit.

A compilation error does not replace the process.
An execution error does not replace the process.
A timeout replaces the process.
A process exit replaces the process.
A protocol failure replaces the process.
The next Cell starts a new process.

The daemon stays alive when a Code Kernel fails.
The daemon does not replay old Cells into the replacement process.
Replay can repeat file, network, and process effects.

The daemon owns the Cell journal.
The child process does not own the journal.
The daemon stores Cell source before evaluation.
The journal survives process replacement.
Mutable bindings do not survive process replacement.

The process boundary is a recovery boundary.
It is not a security boundary.
