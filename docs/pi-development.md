# Pi development

Run Pi with the Loom extension from the repository root.

```sh
bun run dev:pi
```

Pi starts the Workspace daemon when no daemon answers.

Loom selects its packaged Rosé Pine theme when Pi starts.
Loom also installs a compact header and an empty footer.
The header shows the daemon state and its Code Kernel lease.
It shows active Agent, Job, and Workflow state on a second line.
Wide terminals include short Job and Workflow Run IDs.
Narrow terminals omit these IDs and bound each line to the terminal width.

Run `/loom` in Pi to see full daemon and socket details.
Run `/session` in Pi to see full model and usage details.

Run `/reload` after you change Loom extension source files.

Use Shift+Enter to insert a new line.
Pi owns this key behavior.

The Loom extension registers persistent Cell and durable Workflow tools.
See [the Pi Client Adapter contract](./pi-client-adapter.md).

The development command loads the source file directly. It does not copy the extension into the global Pi directory.

Use Herdr to start Pi in a managed pane.

```sh
herdr pane split --current --direction right --cwd "$PWD" --no-focus
herdr pane run <pane-id> "bun run dev:pi"
herdr pane read <pane-id> --source recent-unwrapped --lines 100
```

Send `/reload` through the Pi editor after a source change.
