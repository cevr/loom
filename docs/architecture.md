# Loom Architecture

## Goal

Loom provides a durable local kernel for coding agents.
It prevents long execution from blocking an agent.
It supports persistent TypeScript cells and durable dynamic workflows.

## Runtime shape

```text
Pi client or another client
  |
  | local typed RPC
  v
Loom daemon
  |
  +-- Session entity
  |     |
  |     +-- Agent entity
  |     |     |
  |     |     +-- Bun code-kernel process
  |     |
  |     +-- Job entities
  |     |     |
  |     |     +-- operating-system process groups
  |     |
  |     +-- Workflow Run entities
  |
  +-- SQLite message and workflow storage
  +-- artifact and log files
```

## Core decisions

- Use Effect Cluster entities as logical actors.
- Use Effect Cluster `SingleRunner` for the first local release.
- Keep entity definitions compatible with several runners.
- Use Effect Cluster Workflow Engine for durable workflows.
- Use `effect-encore` as a thin actor and workflow interface.
- Upgrade `effect-encore` before Loom depends on it.
- Use a separate Bun process for each code kernel.
- Use Effect Child Process for jobs and process groups.
- Use Effect RPC with schema-checked NDJSON.
- Use SQLite as the source of orchestration state.
- Use files for complete logs and large artifacts.
- Keep Pi as the first client adapter.
- Do not fork upstream Pi for the kernel.

## OTP mapping

| Loom need               | Effect part                         |
| ----------------------- | ----------------------------------- |
| Logical actor           | Cluster `Entity`                    |
| Actor mailbox           | Cluster entity mailbox              |
| Actor location          | `Sharding`                          |
| Actor restart           | Entity defect policy and `Schedule` |
| Child ownership         | `Scope` and `FiberMap`              |
| Durable message         | Persisted entity message            |
| Durable process         | `ClusterWorkflowEngine`             |
| External workflow work  | `Activity`                          |
| Durable reply or signal | `DurableDeferred`                   |
| Durable timer           | `DurableClock`                      |
| Durable work queue      | `DurableQueue`                      |

Effect does not provide a complete supervisor tree for operating-system processes.
Loom owns that small policy layer.

## Domain ownership

### Session

A Session owns its session tree.
It owns agents, attached jobs, and workflow runs.
Closing a session stops its attached work.

### Agent

An Agent owns one Code Kernel.
The Agent entity ID encodes its Session ID and Agent ID.
One entity activation owns one scoped Code Kernel factory result.
The entity mailbox serializes Cell and Code Kernel control operations.
The Code Kernel starts when the first cell needs it.
The Code Kernel remains active until the Agent ends or Effect Cluster passivates it.
The Loom orchestrator gives an idle Agent a five-minute lease by default.
Other local entities retain Effect Cluster's one-minute idle policy.
See [Agent actor ownership](./adr/0006-key-agent-actors-by-session-and-agent.md).

### Code Kernel

The Code Kernel runs in a separate Bun process.
It uses `Bun.Transpiler` with `replMode`.
It evaluates transformed TypeScript in one persistent VM context.
It supports top-level `await`.
The process boundary provides recovery.
The VM context does not provide a security boundary.
The daemon and Code Kernel use a typed JSONL process protocol.
The daemon replaces the process after a timeout, exit, or protocol failure.
Closing an Agent entity Scope closes its Code Kernel process.
See [Code Kernel recovery](./code-kernel-recovery.md).
See [Code Kernel limits](./adr/0007-bound-code-kernel-failures-and-diagnostics.md).

Loom stores the Cell Ledger.
Loom does not replay cells automatically after a daemon restart.
Automatic replay could repeat file, network, and process effects.
Agent operations use live Effect entity messages.
They do not use persisted mailbox redelivery.
The Cell Ledger owns Cell retry and recovery facts.

### Job

Every execution starts as a Job.
A foreground caller receives a short Foreground Lease.
The Job continues when that lease ends.
The caller receives a Job ID and can poll, stream, await, or cancel it.

Cancellation targets the full process group.
It sends `SIGTERM` first.
It sends `SIGKILL` after a bounded grace period.

Attached jobs stop with their session.
Detached jobs survive session closure.
Background jobs write complete output to files from process start.
This allows safe recovery after a daemon restart.

The Job launcher must store the PID, process group ID, and process start identity before it reports a Job as live.
The Job process waits on a parent-owned input gate until Loom commits that identity.
The Job command starts in the Workspace root.
After a restart, Loom adopts only an exact identity match.
It does not signal a process after an identity mismatch.
See [Job process recovery](./job-recovery.md).

### Workflow Run

A Workflow Run does not use mutable Code Kernel state.
It uses one immutable accepted request.
The request stores exact source, JSON input, version, capabilities, signals, budgets, and interpreter version.
The daemon derives one digest from the complete request.
It does not store a second source hash.
The daemon mints a Workflow Incarnation ID when it accepts a new identity tuple.
Effect derives the Workflow Run ID from that idempotency key.
The accepted request, signal declarations, and workflow send commit in one storage transaction.
Each external Step becomes an Effect Workflow Activity.
Every external Step requires an explicit and unique Step ID.
A child Agent Step derives one attached Job from its Activity key.
The Job owns the Pi process, output, terminal outcome, and restart recovery.
The Step waits for that terminal outcome and stores a bounded Agent result for replay.
A replay reuses the same Agent ID and Job ID.

Workflow source receives a declared capability set.
It does not receive unrestricted Bun APIs.
It does not receive wall-clock time or random values.
Each replay pass uses a fresh VM context.
Suspension discards the current VM context.
See [Dynamic workflow replay](./adr/0009-use-effect-replay-for-dynamic-workflows.md).

The orchestrator sets one Workflow State Lease.
The default lease is five minutes.
Success, interruption, failure, and defect remain public during this lease.
The acceptance row stores the fixed retirement deadline.
Retirement marks the acceptance as `Retiring` before it stops child Agents and their Jobs.
New acceptance cannot attach to a `Retiring` run.
Loom then removes the accepted request, signal declarations, child Agent rows, and Effect Cluster messages.
The durable record removal uses one SQLite transaction.
Suspended and compensating runs remain recoverable.
Daemon startup removes terminal runs only after their stored retirement deadline.
Audit records and artifacts do not belong to startup recovery.

## Storage ownership

One store owns each fact.

| Fact                                        | Owner                         |
| ------------------------------------------- | ----------------------------- |
| Persisted actor mailboxes and replies       | SQLite through Effect Cluster |
| Workflow state and replay                   | SQLite through Effect Cluster |
| Workflow acceptance and retirement deadline | Loom SQLite store             |
| Session transcript                          | Client transcript store       |
| Session closure and retention deadline      | Loom SQLite store             |
| Cell Ledger                                 | Loom SQLite store             |
| Job state and process identity              | Loom SQLite store             |
| Plugin state                                | Loom SQLite store             |
| Complete job output                         | Log files                     |
| Large cell and workflow results             | Artifact files                |

Do not copy job or workflow state into JSONL.
JSONL is a transport or transcript format.
It is not the orchestration source of truth.
See [Orchestration storage and restart recovery](./orchestration-recovery.md).
See [Single ownership of orchestration facts](./adr/0011-assign-one-owner-to-each-orchestration-fact.md).

## Transport

The first client transport is a local Unix socket.
The protocol uses Effect RPC and Effect Schema.
The wire format uses bounded NDJSON framing.
The maximum incomplete frame size is 1 MiB.

The client performs a version and Workspace handshake before another operation.
The client reconnects within a bounded operation timeout after a daemon restart.
See [Daemon connection](./daemon-connection.md).

The public protocol does not expose Effect Cluster addresses.
Cluster transport remains an internal runtime detail.

## Plugin model

A Plugin has one manifest and one Component for each supported host.
Each Component declares its Plugin Grants and Plugin Contributions.
The first hosts are the daemon and a Client Adapter process.

The Plugin Host validates the complete set before it starts a Component.
It builds each Component as an Effect Layer in a private Scope.
It publishes Contributions only after all validation and startup work succeeds.
It supervises Event Consumer fibers with bounded restart policy.

Plugins can contribute typed tools, commands, Workflow capabilities, and event consumers.
Plugins use narrow Effect services.
They do not receive raw stores, Code Kernel processes, or the full daemon Context.
A Plugin failure does not change actor-owned state.

The Plugin host validates each Plugin State value with the Plugin-owned Effect Schema.
The daemon stores only `Schema.Json` and one compare-and-set revision.
Session admission prevents a Plugin State write from racing Session close.

The Herdr Plugin consumes the latest actor-state projection for one Session.
It maps Agent, Job, and Workflow Run activity into Herdr pane state.
It uses a sliding latest-value buffer.
It releases its Herdr agent when the Plugin Scope closes.
See [Herdr Plugin](./herdr-plugin.md).
See [Plugin Contract](./plugin-contract.md).
See [Capability-based Plugin Components](./adr/0010-use-capability-based-plugin-components.md).

## Package direction

```text
protocol     -> domain
client       -> domain + protocol
runtime      -> domain + protocol
platform-node -> client + domain + protocol
platform-bun -> client + domain + protocol + runtime
pi-extension -> client + domain + platform-node + protocol
plugin-herdr -> domain + runtime
code-kernel  -> platform-bun
daemon       -> client + domain + platform-bun + protocol + runtime
```

Imports flow from definitions to implementations.
The domain package has no platform dependency.
The protocol package owns boundary schemas.
The runtime package owns entities and workflows.
The Bun package owns unmatched Bun and VM APIs.
The Node package owns the Pi socket and process adapters.
The daemon composes layers.
Client adapters depend on the shared client contract.

Create each package only when its first working boundary exists.

## Delivery order

See [the first release boundary](./first-release.md) for the exact beta scope and release gates.

1. Establish the domain package and daemon composition root.
2. Upgrade `effect-encore` to the pinned Effect version.
3. Add the RPC protocol and local daemon transport.
4. Add the Bun Code Kernel process.
5. Add durable Jobs and foreground leases.
6. Add durable dynamic workflows.
7. Add the Pi client adapter and Shift+Enter behavior.
8. Add several cluster runners only after local recovery tests pass.

## Required recovery tests

- A long pipeline must return a Job ID when its Foreground Lease ends.
- Cancelling a pipeline must stop its full process group.
- A job must keep writing output after its caller stops waiting.
- A daemon restart must reconnect to a safe background job.
- A bad Code Kernel must not stop the daemon.
- A Code Kernel restart must keep its Cell Ledger without automatic replay.
- Two Agents must not share Code Kernel bindings.
- A transient Cell retry must not repeat a terminal Cell.
- A daemon restart must interrupt an in-flight Cell without replay.
- A daemon restart must terminate an exact-match orphan Code Kernel process.
- A Workflow Artifact must appear only after its complete file is durable.
- Two Cells for one Agent must share Code Kernel bindings.
- A Workflow Run must resume after a full daemon restart.
- A completed Step must not run again during workflow replay.
- Two callers in one Session with one Workflow name, version, and key must share one Workflow Run.
- The same Workflow identity with a different accepted request must fail before execution.
- A duplicate Step ID must stop a Workflow Run.
- A daemon restart during compensation must not repeat a completed compensation.
- A failed compensation must wait for an operator decision before it continues.
- Workflow suspension must discard the current VM pass.
- Terminal Workflow storage must leave startup recovery after its Workflow State Lease.
