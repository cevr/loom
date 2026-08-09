# Herdr Plugin

The Herdr Plugin publishes Loom actor activity to the Herdr pane that owns a Session.

## Ownership

Loom actors own lifecycle state.
The `ActorStateHub` stores only the latest projection for each live actor.
The Herdr Plugin consumes that projection.
Herdr does not control an Agent, Job, or Workflow Run.

## Mapping

The Plugin reduces all Agent, Job, and Workflow Run projections for one Session.

| Loom activity | Herdr state |
| ------------- | ----------- |
| `Blocked`     | `blocked`   |
| `Failed`      | `blocked`   |
| `Working`     | `working`   |
| `Idle`        | `idle`      |
| `Stopped`     | removed     |

`Blocked` and `Failed` have priority over `Working`.
`Working` has priority over `Idle`.

## Delivery

The Plugin uses the Herdr Unix socket.
It sends one schema-checked JSON request per state change.
It opens a new socket for every request.
This makes reconnect automatic.

The request sequence is monotonic.
The sequence starts from the current Unix time.
Herdr can reject stale reports after a Plugin reload.

The Plugin uses a sliding buffer with one item.
This keeps the newest projection when Herdr is slow.
It prevents stale state from building an unbounded queue.

The Plugin reports the Loom Session ID as the Herdr agent session ID.
The Plugin releases its Herdr agent when its Effect Scope closes.

## Failure isolation

A Herdr connection failure does not fail a Loom actor.
The Plugin logs the failure.
The next actor projection opens a new connection.

## Live verification

Run this command from a Herdr pane:

```sh
bun run probe:herdr-live
```

The probe publishes `working`, `blocked`, failed-as-`blocked`, and `idle`.
It then closes the Plugin Scope and releases the Herdr agent.
