# First Release Boundary

Loom `0.1.0` is the first local beta.
It proves one durable coding-agent kernel in one Workspace.

## Included capabilities

- One Loom daemon per Workspace.
- One local Unix socket with a typed handshake and bounded NDJSON frames.
- One Pi Client Adapter.
- One Code Kernel per Agent.
- Persistent TypeScript cells with top-level `await`.
- Durable Jobs with foreground leases, complete output, cancellation, and restart recovery.
- Durable dynamic Workflows with explicit capabilities, Steps, Signals, budgets, compensation, and terminal state leases.
- One Herdr Plugin that publishes the current Session actor state.
- SQLite orchestration storage.
- File storage for complete logs, diagnostics, and large artifacts.
- Effect Cluster `SingleRunner`.

## Excluded capabilities

- Several cluster runners.
- Remote daemon access.
- Hard CPU or memory isolation.
- Automatic Cell replay.
- A general third-party Plugin SDK.
- Audit-history retention in the startup recovery store.

These exclusions keep the first release focused on local durability.
They do not change the domain model.

## Package graph

Imports flow in this order:

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

The packages have these owners:

| Package                   | Owner                                    |
| ------------------------- | ---------------------------------------- |
| `@cvr/loom-domain`        | Domain values and identities             |
| `@cvr/loom-protocol`      | RPC and process boundary schemas         |
| `@cvr/loom-client`        | Shared daemon client contract            |
| `@cvr/loom-runtime`       | Actors, Workflows, and runtime policies  |
| `@cvr/loom-platform-bun`  | Bun, SQLite, VM, and process adapters    |
| `@cvr/loom-platform-node` | Pi-compatible Node transport adapters    |
| `@cvr/loom-pi-extension`  | Pi commands, tools, and key behavior     |
| `@cvr/loom-plugin-herdr`  | Herdr state projection                   |
| `@cvr/loom-code-kernel`   | Code Kernel process entry point          |
| `@cvr/loom-daemon`        | Layer composition and daemon entry point |

## Ordered build slices

Each slice must compile and pass the full gate before the next slice starts.
Each slice uses one Conventional Commit when its change is small.
A high-blast-radius slice uses reviewable sub-commits.

1. Establish the domain model and package boundaries.
2. Establish the daemon connection and RPC protocol.
3. Build the persistent Bun Code Kernel and its recovery boundary.
4. Build durable Jobs, foreground leases, output retention, and process reconciliation.
5. Build durable Workflow acceptance, replay, capabilities, Signals, compensation, and terminal retention.
6. Complete the Pi Client Adapter and Shift+Enter input behavior.
7. Define the Plugin contract around the working Herdr Plugin.
8. Run the release gate and publish the `0.1.0` beta.

Slices 1 through 5 have working implementations.
The Herdr Plugin proves one narrow Plugin lifecycle before slice 7 defines the public contract.

## Required recovery proof

The first release must pass every recovery test in [the architecture](./architecture.md#required-recovery-tests).
It must also pass these live checks:

- Start Pi with the Loom extension through Herdr.
- Run a persistent Code Kernel Cell.
- Start a Job that outlives its Foreground Lease.
- Stop and restart the daemon.
- Read the Job output after restart.
- Run a Workflow across a daemon restart.
- Confirm that Herdr shows current Agent, Job, and Workflow Run state.
- Use Pi `/reload` and run another Loom operation.

## Release gate

The release commit must meet all of these conditions:

- `bun run gate` passes.
- The live Herdr Pi checks pass in a temporary Workspace.
- Counsel reports no blocking correctness or code-quality finding.
- The worktree is clean.
- The release commit exists on `origin/main`.
- The release notes list known beta limits.

## Schema policy

Feature development does not use migrations before `0.1.0`.
The daemon creates the current schema for a new local store.
Developers can replace development stores when the schema changes.

The `0.1.0` release fixes the first supported schema version.
Later releases must migrate user-owned stores without data loss.

## Several-runner entry gate

Work on several cluster runners can start only after `0.1.0` meets these conditions:

- All local recovery tests pass without timing exceptions.
- Entity and Workflow code does not depend on `SingleRunner` internals.
- SQLite ownership is isolated behind runtime services.
- Job process ownership has one clear runner authority.
- Workflow acceptance and Effect Cluster messages commit atomically.
- Terminal Workflow storage leaves startup recovery after its state lease.
- The transport does not expose internal cluster addresses.

The several-runner design must then define leases, fencing, runner loss, and storage concurrency before implementation starts.
