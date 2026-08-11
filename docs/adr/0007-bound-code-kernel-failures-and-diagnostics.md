# Bound Code Kernel failures and diagnostics

The Loom daemon owns the Code Kernel supervision policy.
The Bun worker owns Cell execution.

The worker has 10 seconds to send its ready frame.
A Cell has 30 seconds to finish by default.
The orchestrator can set both limits.

The daemon sends `SIGTERM` when it closes a worker.
Effect sends the force-kill signal after one second.

The daemon retains the last 65,536 stderr characters in memory.
The daemon stores at most 1 MiB of stderr for each worker.
The file gives priority to the end of a truncated stream.
It keeps the available start bytes.
The file includes a truncation marker.
The daemon retains at most 20 stderr files for each Agent.
The daemon retains at most 256 stderr files in total.
The orchestrator can set these limits.
The daemon removes old oversized files during startup cleanup.
Typed process failures can include the request ID, exit code, stderr tail, and file path.

The daemon limits a Cell display to 65,536 characters.
The daemon limits a Cell result to 1,024 binding names.

The daemon opens the crash breaker after three replacements in 30 seconds.
The breaker stays open for 30 seconds.
A Cell timeout counts as a replacement failure.
This rule prevents one Agent from replacing workers without a bound.

Request IDs increase inside one worker lifetime.
A replacement worker starts again at request ID one.
Effect Cluster passivation also resets the request sequence.
The Agent activation owns the sequence state.
Passivation drops that activation state.
The Loom orchestrator gives an idle Agent a five-minute lease by default.
Effect Cluster entity termination timeout bounds Scope shutdown after passivation.
It does not define the idle lease.

The local Code Kernel is a trusted single-user recovery boundary.
It is not a security boundary.
Bun reports process resource use after process exit.
Effect Child Process does not expose a live Bun resource monitor.
Loom does not add a custom polling monitor.
Hard CPU and memory limits belong to a later isolated deployment runner.
