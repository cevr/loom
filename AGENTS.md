# AGENTS.md

## Purpose

Loom is an OTP-inspired coding-agent kernel.

Read these files before an architectural change:

- `CONTEXT.md`
- `docs/architecture.md`
- `docs/adr/`
- `~/Developer/personal/dotfiles/PRINCIPLES.md`
- `~/Developer/personal/dotfiles/principles/never-block-on-the-human.md`
- `~/Developer/personal/dotfiles/principles/redesign-from-first-principles.md`

## Rules

- Use Bun 1.4 canary for runtime, package management, builds, and tests.
- Use current Effect v4 APIs.
- Pin every Effect beta package to the same exact version.
- Keep host APIs in named platform adapters.
- Keep domain packages free from platform dependencies.
- Use Effect Schema at storage and process boundaries.
- Use typed tools and protocol messages for control flow.
- Do not parse assistant prose to control runtime state.
- Do not add a package until a working boundary needs it.
- Inspect current upstream source with the `repo` skill before reporting an upstream gap.
- Do not create Pi issues.
- Prefer a fork when a significant upstream gap has no realistic workaround.
- Use Conventional Commits.
- Do not push unless the user asks.

## Verification

Run this gate before handoff:

```sh
bun run gate
```
