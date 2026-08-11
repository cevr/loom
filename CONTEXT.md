# Loom

Loom coordinates coding agents, their execution environments, and their durable work.

## Language

**Session**:
A user-visible conversation that owns agents, jobs, and workflow runs.
_Avoid_: Thread, chat

**Closing Session**:
A Session whose close operation has started and which rejects new Session-owned work.
_Avoid_: Closed flag, teardown mode

**Workspace**:
A directory tree that owns one Loom daemon, one local connection endpoint, and one orchestration store.
_Avoid_: Project, repository

**Orchestration Store**:
The Workspace SQLite database that owns durable local coordination facts.
_Avoid_: State database, metadata database

**Daemon**:
The long-lived Loom process that owns one workspace runtime.
_Avoid_: Server, backend

**Connection**:
A schema-checked link from a client adapter to the daemon for one workspace.
_Avoid_: Channel, transport

**Session Tree**:
A root session and all agent sessions that descend from it.
_Avoid_: Conversation tree

**Agent**:
An autonomous participant in a session tree.
_Avoid_: Worker, actor

**Workflow Child Agent**:
An Agent that belongs to one Workflow Run and returns one durable Step result.
_Avoid_: Subagent handle, worker

**Code Kernel**:
A persistent code environment that belongs to one agent.
_Avoid_: REPL, sandbox, kernel

**Cell**:
One source submission to a code kernel.
_Avoid_: Snippet, eval

**Cell Ledger**:
The durable record that owns one Cell request and its outcome.
_Avoid_: Cell journal, evaluation log

**Process Identity**:
The PID, process group ID, and process start value that identify one operating-system process without trusting a reused PID.
_Avoid_: PID, process handle

**Actor State Hub**:
The in-memory projection that publishes the latest Agent, Job, and Workflow Run state for Plugins.
_Avoid_: Actor registry, recovery store

**Job**:
An execution that Loom tracks independently from the request that started it.
_Avoid_: Command, process, task

**Foreground Lease**:
The limited period in which a requester waits directly for a job.
_Avoid_: Timeout

**Job Request**:
The resolved command, ownership, wait, and output limits that cross the Loom protocol boundary.
_Avoid_: Client options, adapter defaults

**Detached Job**:
A job whose lifetime does not belong to its session.
_Avoid_: Background process, orphan

**Lost Job**:
A terminal Job whose stored Process Identity cannot prove a matching live process or terminal result.
_Avoid_: Missing job, orphan job

**Workflow**:
A reusable and versioned definition of durable work.
_Avoid_: Pipeline, recipe

**Workflow Key**:
A caller-selected key that identifies one Workflow Run within a Session, Workflow name, and Workflow version.
_Avoid_: Request ID, execution key

**Workflow Run**:
One durable execution of a workflow.
_Avoid_: Workflow instance, execution

**Workflow Incarnation**:
One accepted use of a Workflow identity tuple, identified by a daemon-minted idempotency key.
_Avoid_: Attempt, generation

**Workflow State Lease**:
The limited period in which Loom keeps a terminal Workflow Run available for inspection.
_Avoid_: Cleanup delay, history window

**Workflow Budget**:
The resolved limits that bound one Workflow Run and become part of its accepted request.
_Avoid_: Client defaults, runtime limits

**Retiring Workflow Run**:
A terminal Workflow Run that rejects new attachment while Loom stops owned work and removes recovery state.
_Avoid_: Deleting workflow, cleaning workflow

**Step**:
A stable, unique, and named external operation inside a Workflow Run.
_Avoid_: Stage, action

**Signal**:
A durable value that lets outside work continue a workflow run.
_Avoid_: Event, callback

**Artifact**:
A file that stores a large or durable result from a cell, job, or workflow run.
_Avoid_: Blob, attachment

**Artifact Reference**:
A typed Artifact ID that lets a caller read one published Artifact.
A missing Artifact is a storage failure.
_Avoid_: File path, result state

**Plugin**:
A versioned module that adds a declared Loom capability.
_Avoid_: Extension, hook bundle

**Plugin Component**:
One scoped part of a Plugin that runs in one Loom host.
_Avoid_: Entrypoint, runtime half

**Plugin Contribution**:
A typed tool, command, Workflow capability, or event consumer that a Plugin Component gives to its host.
_Avoid_: Hook, callback registration

**Plugin Grant**:
A limited Loom service that a host gives to one Plugin Component.
_Avoid_: Runtime context, dependency bag

**Plugin Host**:
A Loom process that validates, starts, supervises, and stops Plugin Components.
_Avoid_: Plugin manager, loader

**Plugin State**:
Schema-checked state that Loom stores for one Plugin ID and one Workspace or Session scope.
Each write uses a compare-and-set revision.
Session close removes Session Plugin State.
Workspace Plugin State stays until an operator removes it.

**Plugin State Revision**:
The positive integer that orders writes for one Plugin State key.
A write succeeds only when its expected revision matches the stored revision.

**Client Adapter**:
A module that connects a user interface or agent client to Loom.
_Avoid_: Frontend, transport
