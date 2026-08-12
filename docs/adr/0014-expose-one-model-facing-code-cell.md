# Expose one model-facing Code Cell

## Decision

Loom registers only `loom_cell` as a Pi model tool.
Loom makes it the only active model tool when a session starts.
The persistent Bun Code Kernel provides the global `loom` object.
This object owns file, search, Job, Workflow, and Goal operations.
Pi slash commands remain user controls.

## Rationale

Prime Agent gives the model one IPython tool.
The model composes computer work in that kernel.
Loom uses the same model shape with TypeScript and Bun.
One tool keeps intermediate values in the kernel.
It also reduces tool selection errors.

Loom does not run shell commands as direct child processes of the Cell.
`loom.run` starts a durable Loom Job.
The foreground lease defaults to five minutes.
Lease expiry returns control without stopping the Job.
This rule prevents a nonterminating pipeline from blocking the Agent.

## Consequences

New model capabilities must enter through the `loom` host object.
They must not add a second Pi model tool.
User-only controls can remain slash commands.
File changes must return structured change data for the Cell renderer.
The renderer can show a collapsed summary or an expanded diff.
