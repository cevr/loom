# Loom

Loom coordinates coding agents, their execution environments, and their durable work.

## Language

**Session**:
A user-visible conversation that owns agents, jobs, and workflow runs.
_Avoid_: Thread, chat

**Workspace**:
A directory tree that owns one Loom daemon, one local connection endpoint, and one orchestration store.
_Avoid_: Project, repository

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

**Code Kernel**:
A persistent code environment that belongs to one agent.
_Avoid_: REPL, sandbox, kernel

**Cell**:
One source submission to a code kernel.
_Avoid_: Snippet, eval

**Job**:
An execution that Loom tracks independently from the request that started it.
_Avoid_: Command, process, task

**Foreground Lease**:
The limited period in which a requester waits directly for a job.
_Avoid_: Timeout

**Detached Job**:
A job whose lifetime does not belong to its session.
_Avoid_: Background process, orphan

**Workflow**:
A reusable and versioned definition of durable work.
_Avoid_: Pipeline, recipe

**Workflow Key**:
A caller-selected key that identifies one Workflow Run within a Session, Workflow name, and Workflow version.
_Avoid_: Request ID, execution key

**Workflow Run**:
One durable execution of a workflow.
_Avoid_: Workflow instance, execution

**Workflow State Lease**:
The limited period in which Loom keeps a terminal Workflow Run available for inspection.
_Avoid_: Cleanup delay, history window

**Step**:
A stable, unique, and named external operation inside a Workflow Run.
_Avoid_: Stage, action

**Signal**:
A durable value that lets outside work continue a workflow run.
_Avoid_: Event, callback

**Artifact**:
A file that stores a large or durable result from a cell, job, or workflow run.
_Avoid_: Blob, attachment

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

**Client Adapter**:
A module that connects a user interface or agent client to Loom.
_Avoid_: Frontend, transport
