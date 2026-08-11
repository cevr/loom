# Keep Side Conversations off the Session transcript

## Decision

Loom implements `/btw` as a scoped Client Component.
It does not create a hidden Session or a Workflow Run.

The Pi Client Adapter maps `buildContextEntries` through Pi's exported `sessionEntryToContextMessages` function.
It deep-copies the resulting compaction-aware context for each side turn.
It calls `ModelRegistry.complete` with the selected model.
It runs one tool-free turn and omits the reasoning option.
It disables provider cache retention and uses a request Session ID that is separate from the main Session.
It reports token usage and typed context failures.

The Client Component owns command registration, prior completed side turns, typed actions, and rendering.
A follow-up receives a fresh main-context snapshot followed by the completed side turns.
No side question or answer enters the main Session transcript or later main model context.

The command Scope owns the provider request.
Scope interruption stops streaming through one Abort Signal.
The operation does not expose the raw Agent, mutable transcript, or a separate cancellation handle.

This boundary keeps Pi as the owner of Session model execution.
It also lets Loom add the feature through the existing Pi Extension API.
