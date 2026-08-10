# Orchestration storage and restart recovery

Loom restores coordination from facts that have one durable owner.
It never reconstructs orchestration state from logs, Artifacts, or live actor projections.

## Ownership

| Fact                                                                             | Owner                                                         | Durable identity                                              |
| -------------------------------------------------------------------------------- | ------------------------------------------------------------- | ------------------------------------------------------------- |
| Session namespace                                                                | Client transcript store and records that contain a Session ID | Session ID                                                    |
| Normal Agent namespace                                                           | Effect Cluster entity address                                 | Session ID and Agent ID                                       |
| Cell request and outcome                                                         | Loom Orchestration Store                                      | Session ID, Agent ID, and Cell ID                             |
| Code Kernel process identity                                                     | Loom Orchestration Store                                      | Session ID and Agent ID                                       |
| Job request, state, and process identity                                         | Loom Orchestration Store                                      | Job ID                                                        |
| Workflow Run acceptance, Workflow Incarnation ID, retirement state, and deadline | Loom Orchestration Store                                      | Session ID, Workflow name, Workflow version, and Workflow Key |
| Workflow mailbox, request, Step results, timers, compensation, and Signal values | Effect Cluster storage                                        | Workflow Run ID and Effect message identity                   |
| Public Signal declaration projection                                             | Loom Orchestration Store                                      | Workflow Run ID and Signal name                               |
| Workflow child Agent ownership                                                   | Loom Orchestration Store                                      | Activity key                                                  |
| Plugin state                                                                     | Loom Orchestration Store                                      | Plugin ID, scope, key, and revision                           |
| Complete Job output                                                              | Log files                                                     | Job ID and stream                                             |
| Large Cell and Workflow results                                                  | Artifact files                                                | Artifact ID                                                   |
| Live actor state                                                                 | In-memory Actor State Hub projection                          | None. This state is derived.                                  |

A Session does not need a separate row.
Its ID is a namespace in owned records.
Session close is idempotent cleanup.
It is not a durable tombstone.
A later client can use the same Session ID.

A normal Agent does not need a separate row.
Its Effect Cluster address supplies its identity.
Workflow child Agent ownership is durable because a replayed Activity must find the same child Agent.

## Cell Ledger

The ledger stores the request source, state, terminal result, and terminal error.
It has one unique key for Session ID, Agent ID, and Cell ID.

The Cell states are `Accepted`, `Evaluating`, `Succeeded`, `Failed`, and `Interrupted`.

The daemon accepts a new Cell as `Accepted` before evaluation.
The daemon rejects the same Cell ID when the stored source differs.
A retry of `Succeeded`, `Failed`, or `Interrupted` returns the stored outcome.
The daemon changes `Accepted` to `Evaluating` before it sends work to a Code Kernel.
A duplicate request never starts a second evaluation.
It waits for the owned evaluation when that evaluation is active in the current daemon.
If no owned evaluation is active, it returns the stored interruption state after reconciliation.

Daemon startup changes each `Accepted` or `Evaluating` Cell to `Interrupted` with reason `DaemonRestart`.
It never replays that Cell.
The Cell can have file, network, or process effects that Loom cannot roll back.

## Code Kernel process recovery

The daemon stores the Code Kernel Process Identity before it sends the first Cell request to that process.
The daemon removes this record after confirmed process exit.

Daemon startup inspects each stored Process Identity.
It terminates a process only when every identity field matches.
It treats that process as an unadoptable orphan because its VM context and request channel cannot be restored.
It never signals a process when the identity differs.
It removes an absent or mismatched record after inspection.
It completes this reconciliation before the daemon accepts a connection.

## Job recovery

The `jobs` table owns the complete Job request, lifecycle state, paths, terminal result, and Process Identity.
The Job ID is the idempotency key.
The immutable request must match when a caller reuses a Job ID.
The launch gate commits Process Identity before the command starts.
An `Accepted` Job is durable authorization to run the command.
Startup can start it before a client reconnects because no command ran before the launch claim.
A Cell differs because its authorization belongs to one live Agent operation.

Startup follows the outcomes in [Job process recovery](./job-recovery.md).
It adopts only an exact Process Identity match.
It never infers a Job result from log contents.

## Workflow recovery

The Loom acceptance row maps the caller identity tuple to one Workflow Incarnation ID, one Workflow Run ID, and one canonical request digest.
The daemon mints a new Workflow Incarnation ID for each new acceptance.
Effect derives the Workflow Run ID from the static `LoomDynamicWorkflow` name and the Workflow Incarnation ID.
A retry shares both stored IDs only while that acceptance row exists.
Reusing the identity tuple after retirement creates a new Workflow Incarnation ID and Workflow Run ID.
The immutable request lives in the accepted Effect Workflow message.
Its idempotency key is the Workflow Incarnation ID.
The request digest covers caller-provided fields.
It does not cover the Workflow Incarnation ID or the derived Workflow Run ID.
The acceptance row, public Signal declarations, and Workflow send commit in one SQLite transaction.

Effect Cluster storage owns Workflow execution state.
It also owns Activity results, Step results, timers, compensation, and durable Signal values.
Loom does not copy these facts into a second table.

The Workflow identity tuple is the acceptance idempotency key.
The Workflow Incarnation ID separates later uses of that tuple.
The Activity key is the child Agent idempotency key.
The Activity key derives each Workflow Job ID and Workflow Artifact ID.

Workflow child Agent rows remain until terminal Workflow retirement.
Retirement first changes the acceptance row to `Retiring` in one committed transaction.
New acceptance cannot attach to a `Retiring` run.
It returns a typed retryable error.
Startup resumes each `Retiring` run.
Retirement then stops each active child Agent and records it as `Stopped`.
A stop failure keeps the acceptance and ownership rows for a later retry.
Retirement then deletes Effect messages, Signal declarations, acceptance, and stopped child Agent rows in one transaction.
It does not delete logs or Artifacts.

The acceptance row owns the Workflow State Lease deadline.
The first terminal observation stores one `retire_after` value when no value exists.
Later observations and daemon restarts do not extend it.
Startup retires only terminal Workflow Runs whose stored deadline has passed.
Success, interruption, failure, and defect use the same rule.

Session close interrupts each active Workflow Run in that Session.
It then stops attached Jobs and child Agents.
The operation is idempotent.
A Workflow Run cannot create new Session-owned work after close begins.

## Files

Job log files own complete standard output and standard error.
Artifact files own large results.
Each Artifact write uses a temporary file in the target directory.
Loom renames that file to the final path before the Activity reports success.
This makes publication atomic.

Log retention and Artifact retention are separate policies.
Startup does not read either file type to recover actor or Workflow state.
A database record can refer to a missing file.
That condition is a typed storage failure.
It is not permission to guess a result.

## Startup order

The daemon uses this order:

1. Open the Orchestration Store and create the current schema.
2. Reconcile stored Code Kernel Process Identities.
3. Interrupt stale `Accepted` and `Evaluating` Cells.
4. Reconcile active Jobs.
5. Resume Retiring Workflow Runs and begin retirement for expired terminal Workflow Runs.
6. Start Workflow recovery and state watchers.
7. Publish the connection endpoint as ready.

A schema or store-open failure stops startup.
A recovery failure for one record does not stop other records.
Loom keeps the failed record and logs its identity and typed failure.
A later daemon start can inspect it again.

## Derived state

The Actor State Hub is a live in-memory projection.
Plugins can consume it for displays and event reactions.
It is not a recovery authority.
The daemon rebuilds it from actor-owned state after startup.

## Required implementation work

- [Enforce the complete daemon recovery phase order](https://github.com/cevr/loom/issues/37).
- [Store one Workflow State Lease deadline for each terminal Workflow Run](https://github.com/cevr/loom/issues/28).
- [Publish Workflow Artifacts with a temporary file and atomic rename](https://github.com/cevr/loom/issues/30).
- [Stop and delete Workflow child Agent ownership during terminal Workflow retirement](https://github.com/cevr/loom/issues/29).
- [Interrupt active Workflow Runs during Session close](https://github.com/cevr/loom/issues/36).
- [Complete the missing Job recovery outcomes and Job lifecycle model](https://github.com/cevr/loom/issues/31).
- [Add Plugin State storage and RPC before the first stateful Plugin](https://github.com/cevr/loom/issues/34).
