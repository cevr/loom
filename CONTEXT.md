# Loom

Loom coordinates coding agents, their execution environments, and their durable work.

## Language

**Session**:
A user-visible conversation that owns agents, jobs, and workflow runs.
_Avoid_: Thread, chat

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
A reusable definition of durable work.
_Avoid_: Pipeline, recipe

**Workflow Run**:
One durable execution of a workflow.
_Avoid_: Workflow instance, execution

**Step**:
A stable and named unit of work inside a workflow run.
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

**Client Adapter**:
A module that connects a user interface or agent client to Loom.
_Avoid_: Frontend, transport
