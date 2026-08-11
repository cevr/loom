# Key Agent actors by Session and Agent

Loom runs one Effect Cluster entity activation for each Agent owner.
The entity ID encodes the `SessionId` and `AgentId` tuple.
The encoding prevents collisions between tuple components.

Each activation acquires one Code Kernel through a scoped factory.
The activation serializes Cell evaluation and Code Kernel control operations.
The Code Kernel starts its Bun process when the first Cell needs it.
The activation Scope closes the Code Kernel process during passivation.

The Loom orchestrator passes a five-minute idle lease to the Agent actor by default.
Effect Encore passes that policy to Effect Cluster as the Agent entity maximum idle time.
The lease restarts after each Agent message.
The activation Scope closes the exact Code Kernel process when the lease ends.
The first local runtime uses `SingleRunner` with SQL runner storage.
SQLite stores cluster messages and Cell Ledger entries.

Cell operations do not use persisted entity messages.
Loom does not replay arbitrary Cell effects.
The Cell Ledger records source before evaluation.

This design prevents two Agents from sharing mutable Code Kernel bindings.
It also keeps the actor model compatible with a future multi-runner runtime.
