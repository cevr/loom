# Pi development

Run Pi with the Loom extension from the repository root.

```sh
bun run dev:pi
```

Pi starts the Workspace daemon when no daemon answers.

Run `/loom` in Pi to confirm the daemon connection.

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

Use `herdr pane run <pane-id> "/reload"` after a source change.
