# Plugin Contract

This document defines the Loom Plugin contract.
The first beta uses this contract for built-in Plugins.
The first beta does not publish a third-party Plugin SDK.

## Shape

A Plugin has one static manifest.
The manifest contains these fields:

- Plugin ID.
- Plugin version.
- Plugin API version.
- One Plugin Component for each supported host.

The Plugin ID is a stable package-style name.
The Plugin version uses semantic versioning.
The Plugin API version is one positive integer.
The first Plugin API version is `1`.

A host supports exact Plugin API versions.
The host does not use semantic version ranges for the Plugin API.
A new Plugin API version can exist with an old version in the same host.

A Plugin Component has these parts:

- Its host type.
- Its required Plugin Grants.
- Its Plugin Contributions.
- One Effect Layer that starts the Component.

The first host types are `Daemon` and `Client`.
A Daemon Component runs in the Loom daemon.
A Client Component runs in a Client Adapter process.
A host loads only the Component code for its host type.
A Client Adapter is not a Plugin.
It supplies client services and maps Client Contributions to its user interface.

A Daemon Component can contribute Workflow capabilities and event consumers.
A Client Component can contribute tools, commands, and event consumers.

## Plugin Grants

A Plugin Grant gives the least authority that one Component needs.
The host creates the grant from its public services.
The host limits a grant to the active Workspace or Session.

The contract defines these first grants:

| Plugin Grant       | Authority                                                                 |
| ------------------ | ------------------------------------------------------------------------- |
| `ActorStateRead`   | Read the latest typed actor-state projection.                             |
| `JobControl`       | Start and control Jobs through the public Job contract.                   |
| `WorkflowControl`  | Start and control Workflow Runs through the public Workflow contract.     |
| `PluginState`      | Read and update schema-checked Plugin state in one namespace.             |
| `SideConversation` | Run one scoped, tool-free model call against the current Session context. |
| `AgentTurnControl` | Queue a typed follow-up turn and read completed turn usage.               |

A Plugin does not receive a raw SQLite connection.
A Plugin does not receive `JobStore` or `CellJournal`.
A Plugin does not receive the full daemon Context.
A Plugin does not receive direct access to a Code Kernel process.
A Plugin does not receive Bun APIs through a grant.
Plugin API version `1` does not have a Code Kernel grant.

`PluginState` owns one namespace for each Plugin ID.
Its key includes the Workspace or Session owner.
Each state key has one Plugin-owned Effect Schema.
One read returns the encoded value and its revision.
One write includes the expected revision.
The store applies the write only when the revision matches.
The Component host validates the Plugin-owned Schema.
The daemon validates the encoded value as `Schema.Json`.
Reload and isolation do not delete Plugin state.
A Client Component uses typed Plugin State read and write RPCs.
The Loom SQLite store owns the encoded state.
A Session close deletes Session-owned Plugin state.
Workspace-owned state stays until an operator removes it.

`JobControl` and `WorkflowControl` use the public Loom contracts.
They do not expose internal actor addresses or storage records.
The invocation context limits client calls to the current Session.
The host gives each tool and command invocation one stable invocation ID.
Each start operation also has one caller-selected operation ID.
Job and Workflow start operations derive their idempotency keys from both IDs.
A Daemon Component receives only the Session or Workspace scope in its host configuration.

## Plugin Contributions

A Plugin Contribution has a stable name.
It declares a subset of its Component Plugin Grants.
Its handler receives only that subset.
The host rejects duplicate names.
The host does not let load order override an existing Contribution.

### Tools

A Tool Contribution has Effect Schemas for input, output, and typed errors.
It can add one Effect Schema for progress updates.
Its handler uses only declared Plugin Grants.
The Client Adapter maps the Tool Contribution to the client tool API.
The adapter keeps display rendering outside the handler.

### Commands

A Command Contribution has one command name and one typed invocation schema.
Its handler returns one typed client action or a typed client action stream.
The Client Adapter owns text parsing, completion, and visual display.
The handler does not receive an unbounded client API.

### Workflow capabilities

A Workflow Contribution implements one external Step capability.
It has Effect Schemas for input, output, and typed errors.
Its stable name includes the Plugin ID and contract version.
For example, `example.plugin/search@1` identifies version `1` of one Step contract.

The accepted Workflow Run stores the exact capability name.
Recovery requires the same capability contract version.
A Plugin upgrade can add a new contract version without removing the old version.
The host rejects recovery when the accepted version is not available.

The handler receives one immutable Workflow Activity context.
The context includes the Activity idempotency key.
The handler can use only grants that declare replay-safe Activity behavior.
It cannot use `JobControl`, `WorkflowControl`, `PluginState`, or client grants.
An external operation that can change state must accept the Activity idempotency key.

The Workflow Activity owns retry and replay.
The Plugin handler does not add a second replay engine.

### Event consumers

An Event Consumer Contribution names one public event contract and its version.
It receives an immutable event or state projection.
It cannot stop, change, or delay the source actor operation.

Each event contract states its delivery rule.
Actor-state delivery uses the latest projection.
It can merge intermediate values.
Loom does not provide one generic event bus with implicit delivery rules.

## Start order

The host completes these steps:

1. It loads all manifests.
2. It checks Plugin API versions.
3. It checks grant policy.
4. It checks Contribution names and contract versions.
5. It sorts Components by Plugin ID.
6. It builds each Component Layer in a private Scope.
7. It publishes all successful Contributions in one operation.

The host does not publish Contributions during validation or start work.
It publishes one registry after that work ends.
Plugins do not depend on other Plugins.
Components depend only on host grants.
Workflow and event declarations refer to versioned public contracts.

The host configuration marks an enabled Plugin as required or optional.
The Plugin cannot mark itself as required.
A required Component start failure stops host startup.
An optional Component start failure isolates that Component.
The host closes all started Scopes when a required Component fails.
A name collision is a Component validation failure.
The required or optional rule applies to that failure.
All Components in the collision fail validation.

## Scope and supervision

The Plugin Host owns every Plugin Component Scope.
The Component Scope owns its services, event fibers, and finalizers.
The host closes Components in reverse start order.
A Client Adapter reload closes its Client Component Scopes.
It does not close the Loom Session.
The first beta reloads Daemon Components only when the daemon restarts.

Each command, tool, and event delivery runs in a child fiber of the Component Scope.
One expected failure stays in the typed error channel.
One defect stops only that invocation.
The host reports the defect as a Plugin failure.

The host restarts a long-lived Event Consumer with a bounded Effect Schedule.
Repeated defects isolate the Component.
Isolation stops dispatch to its Contributions.
Isolation does not stop a Loom actor or corrupt actor state.
A Client Adapter can keep a static client binding after isolation.
That binding returns a typed Plugin unavailable error.

Durable Jobs and Workflow Runs keep their own owners.
They do not become children of a Plugin Scope.
A Plugin reload does not cancel them.
An unavailable Workflow Contribution can stop Workflow recovery with a typed compatibility error.
The Workflow Activity fiber owns one Workflow Contribution call.
Component isolation makes a new or retried call fail with a typed unavailable error.
The Component Scope does not own the Activity fiber.

## `/btw` and `/goal`

The contract does not make `/btw` or `/goal` daemon internals.
They can be Client Components.

An implementation of `/btw` needs these parts:

- One Command Contribution.
- The `SideConversation` grant.
- A typed command-action stream.

The Client Adapter supplies an immutable snapshot of the current transcript to the side model call.
The Plugin does not read the transcript directly.
The side model call does not write its turns to the main transcript.
Closing the command Scope stops the side model call.

An implementation of `/goal` needs these parts:

- One Command Contribution.
- One typed Tool Contribution for goal-state control.
- The `PluginState` grant for Session-owned goal state.
- The `AgentTurnControl` grant for follow-up turns and usage.
- Typed client actions for status display.

Goal state is one tagged state machine.
The Plugin stores each transition before it requests another turn.
The Client Adapter accounts model usage and supplies it through `AgentTurnControl`.

These features do not need raw transcript storage, raw model objects, or the full Pi extension API.

## Herdr

The Herdr Plugin is one Daemon Component.
It declares only the `ActorStateRead` grant.
It contributes one Actor State event consumer.
Its private Scope owns the state stream and the Herdr client finalizer.
The host filters actor state to the configured Session before it supplies the grant.

The current Herdr package proves the Scope and delivery rules.
The daemon still composes it directly in the first beta.
A later public Plugin Host can load the same Component declaration.

`ActorStateRead` is a Session-filtered view of the Loom `ActorStateHub`.
The hub merges Agent, Job, and Workflow Run projections.
Effect Encore actor-state observation covers Effect Cluster entities.
It does not replace this Loom projection owner.
