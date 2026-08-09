# Loom

Loom is an OTP-inspired kernel for coding agents.

Loom uses Bun for process execution and persistent TypeScript code cells.
Loom uses Effect for actors, durable workflows, resource ownership, and typed protocols.

## Status

Loom is in its first architecture and scaffold phase.

Loom currently targets Bun `1.4.0-canary.1`.

## Development

```sh
bun install
bun run gate
bun run dev
```

Run the live Herdr Plugin probe from a Herdr pane:

```sh
bun run probe:herdr-live
```

Read [CONTEXT.md](./CONTEXT.md) for project language.
Read [docs/architecture.md](./docs/architecture.md) for the system design.
