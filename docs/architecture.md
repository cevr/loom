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
The Code Kernel starts when the first cell needs it.
The Code Kernel remains active until the agent ends or policy removes it.

### Code Kernel

The Code Kernel runs in a separate Bun process.
It uses `Bun.Transpiler` with `replMode`.
It evaluates transformed TypeScript in one persistent VM context.
It supports top-level `await`.
The process boundary provides recovery.
The VM context does not provide a security boundary.

Loom stores the cell journal.
Loom does not replay cells automatically after a daemon restart.
Automatic replay could repeat file, network, and process effects.

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
After a restart, Loom adopts only an exact identity match.
It does not signal a process after an identity mismatch.
See [Job process recovery](./job-recovery.md).

### Workflow Run

A Workflow Run does not use mutable Code Kernel state.
It uses stored source, input, source hash, version, and stable Step IDs.
Each external Step becomes an Effect Workflow Activity.
Every external Step requires an explicit stable key.

Workflow source receives a declared capability set.
It does not receive unrestricted Bun APIs.

## Storage ownership

One store owns each fact.

| Fact                            | Owner                         |
| ------------------------------- | ----------------------------- |
| Actor mailboxes and replies     | SQLite through Effect Cluster |
| Workflow state and replay       | SQLite through Effect Cluster |
| Session transcript              | Client transcript store       |
| Cell journal                    | Loom SQLite store             |
| Job state and process identity  | Loom SQLite store             |
| Complete job output             | Log files                     |
| Large cell and workflow results | Artifact files                |

Do not copy job or workflow state into JSONL.
JSONL is a transport or transcript format.
It is not the orchestration source of truth.

## Transport

The first client transport is a local Unix socket.
The protocol uses Effect RPC and Effect Schema.
The wire format uses bounded NDJSON framing.

The public protocol does not expose Effect Cluster addresses.
Cluster transport remains an internal runtime detail.

## Plugin model

Plugins declare an API version and capabilities.
Plugins register typed tools, commands, workflow steps, or event consumers.
Plugins use narrow Effect service interfaces.
Plugins do not receive the full daemon runtime by default.

Plugin order is explicit.
Plugin lifecycle belongs to an Effect Scope.
A plugin failure does not corrupt kernel state.

The Herdr Plugin consumes the latest actor-state projection for one Session.
It maps Agent, Job, and Workflow Run activity into Herdr pane state.
It uses a sliding latest-value buffer.
It releases its Herdr agent when the Plugin Scope closes.
See [Herdr Plugin](./herdr-plugin.md).

## Package direction

```text
@cvr/loom-domain
        ^
        |
@cvr/loom-protocol
        ^
        |
@cvr/loom-runtime      @cvr/loom-platform-bun
        ^                         ^
        |                         |
        +-----------+-------------+
                    |
              @cvr/loom-daemon

@cvr/loom-pi-extension -> @cvr/loom-protocol

@cvr/loom-plugin-herdr -> @cvr/loom-runtime
```

Imports flow from definitions to implementations.
The domain package has no platform dependency.
The protocol package owns boundary schemas.
The runtime package owns entities and workflows.
The Bun package owns unmatched Bun and VM APIs.
The daemon composes layers.
Client adapters depend on the protocol only.

Create each package only when its first working boundary exists.

## Delivery order

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
- A Code Kernel restart must keep its cell journal without automatic replay.
- A Workflow Run must resume after a full daemon restart.
- A completed Step must not run again during workflow replay.
- Two callers with one workflow key must share one Workflow Run.
