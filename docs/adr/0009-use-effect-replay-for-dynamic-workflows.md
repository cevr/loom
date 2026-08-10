# Use Effect replay for dynamic workflows

## Decision

Loom uses one static `LoomDynamicWorkflow` definition.
Effect Cluster Workflow Engine owns each Workflow Run.
Loom does not register one Effect Workflow for each source file.

The accepted Workflow Run request contains these facts:

- Session ID.
- Workflow name.
- Workflow version.
- Workflow Key.
- Interpreter contract version.
- Exact source.
- JSON input.
- Declared capabilities.
- Declared signal names.
- Resolved budgets.

The Effect workflow payload stores the accepted request.
The daemon mints a Workflow Incarnation ID when it accepts a new identity tuple.
The Effect workflow payload contains that idempotency key.
Effect derives the Workflow Run ID from the static `LoomDynamicWorkflow` name and the Workflow Incarnation ID.
Two callers share one Workflow Run only while one accepted identity row exists.
A new use of the identity tuple after retirement receives a new Workflow Incarnation ID and Workflow Run ID.

The daemon derives one canonical SHA-256 digest from the caller-provided request.
The digest includes the source, input, capabilities, signals, budgets, and interpreter version.
The daemon sorts and deduplicates capability names and signal names before it computes the digest.
The daemon uses `effect-encore` `canonicalJsonSha256` to sort object keys and compute the digest.
The request does not store a second source hash.

The Loom SQLite store owns the accepted digest for each workflow identity.
The daemon inserts the identity and digest before it calls the Effect workflow.
The accepted request, signal declarations, and Effect workflow send use one storage transaction.
This transaction serializes a new claim against terminal retirement for the same identity.
The same identity and digest attach to the existing Workflow Run.
The same identity and a different digest fail with `WorkflowIdentityConflictError`.
A `Retiring` acceptance returns a typed retryable error.

## Replay contract

Effect re-runs workflow source after recovery.
Effect reuses stored Activity results during that replay.
Every external operation is an Activity.
Every Activity has an explicit Step ID.
A Step ID is unique during one workflow pass.
The interpreter records each Step ID during the current pass.
The interpreter stops the run as a defect when a Step ID repeats.

The workflow interpreter creates a fresh VM context for each pass.
It provides only the declared capability services.
It does not provide Bun, file, process, network, module loading, the Agent Code Kernel, wall-clock time, or random values.
Durable time uses Effect Durable Clock.
Seeded random values use a declared deterministic capability.

The interpreter decodes host calls once with Effect Schema.
Step inputs and results use `Schema.Json`.
Source reads the accepted JSON value from `input`.
Source calls `step.run({ stepId, capability, input })` for each external operation.
The interpreter checks the call against the declared capability set before it starts the Activity.
The Activity result stores the JSON value, child Agent count, and token count used for replayed budget accounting.
An inline result that exceeds its byte budget becomes an Artifact reference when the workflow has the Artifact capability.
The Step fails when that capability is absent.
The interpreter does not use `Schema.Unknown`, hand-written record guards, or AST scans for determinism.
The production host must use the Effect Encore Step options form with explicit `Schema.Json` schemas.

Effect interruption or suspension invalidates the current VM pass.
The interpreter stops all host calls from that context.
It discards the context without waiting for user source to finish.
This rule prevents user `try` and `catch` code from swallowing workflow suspension.

## Steps and retries

Activity storage owns completed Step results.
Replay does not run a completed Step again.
Effect owns Activity retry schedules and retry attempt numbers.

Each external service accepts the Activity idempotency key.
The Job store deduplicates Job launch by this key.
The Agent store deduplicates child Agent creation by this key.
An Activity retry cannot create a second logical Job or child Agent.
The Agent claim stores the prompt and Workflow Run parent with the Activity key.
The Job claim moves through Accepted, Starting, Running, and Failed states.
A failed launch can claim the launch right again.
The Bun adapter keeps the child process scoped and behind an input gate during launch setup.
It stores the process identity before it marks the Job as Running.
It releases the input gate and process only after durable launch setup succeeds.
Interruption closes the gate and stops the process before it can run the command.
Daemon startup changes every stale Starting claim to Failed before it accepts work.
A Stopping claim without Process Identity becomes Cancelled because the launch gate prevented command start.
Artifact identity also derives from the Activity key.

Effect owns parallel fibers, durable races, and concurrency primitives.
Loom applies the resolved parallelism budget with an Effect semaphore.
Loom does not add a promise limiter.

## Compensation

Effect owns compensation registration and reverse ordering.
Effect Encore wraps each external compensation in a separate Activity named from its Step ID.
A completed compensation does not run again after recovery.
Effect Encore catches a compensation Activity defect and waits on a named Durable Deferred.
An operator resolves that deferred to retry or stop the compensation.
Loom uses this existing Effect Encore compensation contract.

## Signals

The request declares signal names.
All signal values use `Schema.Json`.
The public protocol addresses a signal by Workflow Run ID and signal name.
It does not expose Effect deferred tokens.
Loom writes declared names to `workflow_signal_declarations` when it starts or executes a run.
SQLite stores only the public Workflow Run ID and declared name.
The daemon rejects an undeclared signal name.
An unknown Workflow Run and an undeclared name return the same typed error.
Effect Durable Deferred owns signal storage and wake-up.
A signal delivery resumes its suspended Workflow Run.
Loom does not expose a manual resume operation.

## Terminal retention

The orchestrator owns one Workflow State Lease.
The default lease is five minutes.
The lease lets clients read a terminal result before cleanup.
Failure and defect remain visible as failed actor state during the lease.
Success and interruption remain available for inspection during the lease.

The acceptance row stores one fixed retirement deadline after the first terminal observation.
After the deadline, Loom marks the acceptance as `Retiring` in one committed transaction.
A new acceptance cannot attach to the retiring Workflow Run.
Daemon startup resumes an interrupted retirement.
Loom then stops active child Agents.
Loom then removes the Effect Workflow messages, Durable Clock messages, signal declarations, child Agent rows, and accepted request in one SQLite transaction.
The transaction makes cleanup safe to retry after a daemon restart.
It also prevents a new accepted request from sharing an identity with old Effect Cluster state.
Suspended and compensating runs are not terminal retention candidates.

Daemon startup reads accepted runs once.
It removes terminal runs only when their stored retirement deadline has passed.
It starts watchers for the remaining runs.
A daemon restart does not extend a Workflow State Lease.
Audit records and artifacts have separate retention owners.

## Child Agents

A child Agent records its Workflow Run parent as a durable fact.
The Agent belongs to the same Session as the Workflow Run.
Compensation stops child Agents when the Workflow reverses their Steps.
Terminal retirement stops any child Agent that remains active after success.
Session closure can also find these Agents from their durable parent facts.
Session closure interrupts active Workflow Runs before it stops their attached Jobs and child Agents.
The Agent store stops all active child Agents for one Session with one idempotent operation.
The daemon exposes that operation through the typed Session close RPC.
The Pi extension calls it when Pi closes or replaces a Session.
An extension reload does not close the Session.

## Budgets

The accepted request resolves these budgets:

- Maximum Step count.
- Maximum child Agent count.
- Maximum parallel work count.
- Maximum inline Step result bytes.
- Optional token count.
- Optional duration.

The interpreter derives spend from replayed Activity results.
It does not store a second spend counter.
A duration budget uses a durable race against Effect Durable Clock.
The Bun interpreter accepts that race as a host Effect and does not expose a clock to source.
The Bun VM timeout stops synchronous source that would block the Effect race.
Budget failure uses a typed workflow error.

## Error contract

The static workflow error union contains these run failures:

- Source failure.
- Step failure.
- Budget exceeded.
- Capability denied.
- Duplicate Step ID.
- Undeclared signal.
- Interpreter version mismatch.

`WorkflowIdentityConflictError` belongs to daemon acceptance.
It does not belong to the Workflow Run error union.
Loom keeps Effect defect capture enabled.
Loom does not suspend every source failure.
The VM bridge preserves host failures with host-owned object identity.
Source cannot forge a typed Workflow Run failure with a matching JSON shape.

## Consequences

Workflow Runs survive daemon restart without a second replay engine.
Dynamic source remains isolated from mutable Code Kernel state.
The accepted request is immutable for one workflow identity.
Effect owns workflow status, replay, clocks, races, signals, and compensation order.
Loom owns only acceptance, the deterministic interpreter, capability adapters, and bounded policy.
Effect Encore owns the thin Workflow definition and Step interface.
