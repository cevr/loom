# Pi Client Adapter

Pi is Loom's first Client Adapter.
Loom loads as a Pi extension.
Loom does not fork Pi.

## Input ownership

Pi owns input key behavior and completion.
Pi `0.84.1` inserts a new line for Shift+Enter.
Pi also supports Ctrl+J as a newline alias.
Loom uses Pi `CustomEditor` with at least two columns of prompt padding.
This minimum is part of the Loom terminal interface.
Pi settings can increase the padding.
It does not add a second input parser.

The Pi Client Adapter maps Loom Client Contributions to Pi commands and tools.
Pi keeps input parsing, completion, and standard tool display ownership.
Loom can supply `/btw`, `/goal`, and similar features as scoped Client Components.
These Components use narrow Client Adapter grants.
They do not receive the full Pi extension API.

## Session lifecycle

Pi starts or connects to the Workspace daemon when its session starts.
The Pi session ID becomes the Loom Session ID.
The adapter uses `pi` as the Agent ID inside that Session.

Pi `/reload` keeps the Loom Session open.
Pi shutdown closes the attached Loom Session.
Detached Jobs do not belong to that shutdown.

## Interface ownership

Loom packages an official Rosé Pine theme for Pi.
Its tokens use the Rosé Pine main palette from [the official palette repository](https://github.com/rose-pine/palette).
The extension selects this theme on start and reload.
The extension replaces the Pi header with a compact Loom state view.
The extension replaces the Pi footer with an empty component.
The header shows active Loom state only.
Pi tool panels show each Loom tool call and its terminal state.
The actor line shows live Agent, Job, and Workflow Run state.
Pi `/session` keeps full model and usage information available.
Loom `/loom` keeps full daemon and socket information available.
Loom uses Pi theme tokens and Pi TUI width helpers.
It does not add a second TUI framework.

These choices follow the useful parts of the Prime Agent interface.
They keep the prompt padded, the footer quiet, and execution state visible.

Each operation performs the typed Workspace handshake.
The adapter starts the daemon when no daemon answers.
The client reconnects after a daemon restart within the operation timeout.

## Code Kernel tools

`loom_cell` evaluates TypeScript in the Agent's persistent Code Kernel.
The Pi tool call ID becomes the Cell ID.
The tool returns the Cell display and binding details.
Pi interruption interrupts the client request.
The daemon can finish an interrupted Cell and change its bindings.
The first beta does not expose server-side Cell cancellation.

`loom_cell_reset` replaces the Agent's Code Kernel.
It clears all live bindings.
It does not delete the Cell Ledger.

## Workflow tools

`loom_workflow_start` accepts an immutable Workflow definition and returns its Workflow Run ID.
It does not wait for terminal completion.
Its admission request has a ten-second timeout.

The adapter supplies these control tools:

- `loom_workflow_inspect`
- `loom_workflow_signal`
- `loom_workflow_interrupt`
- `loom_workflow_compensation`

A suspended Workflow continues after a declared Signal.
Loom does not expose a manual resume action.

## Errors

The Loom client preserves typed protocol failures.
Pi reports a rejected Loom tool call as a tool error.
The `/loom` command reports daemon connection failures as notifications.

## Job tools

`loom_job_start` creates the durable Job before process launch.
The Pi tool call ID becomes the Job ID.
Jobs stay attached by default.
The Foreground Lease defaults to five minutes.
A zero lease returns the first Job state without a wait.
Lease expiry and Pi interruption stop only the wait.
They do not stop the Job.

The adapter supplies these control tools:

- `loom_job_inspect`
- `loom_job_output`
- `loom_job_await`
- `loom_job_cancel`
- `loom_job_detach`

Output reads use a byte sequence and a byte limit.
Each result returns the next sequence.
It returns text for direct use.
It also returns Base64 for exact byte recovery.
Detach is one-way.
