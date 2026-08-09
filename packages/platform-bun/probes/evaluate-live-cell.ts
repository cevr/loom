import { BunRuntime, BunServices } from "@effect/platform-bun";
import { LoomClient } from "@cvr/loom-client";
import { AgentId, CellId, SessionId, WorkspaceRoot } from "@cvr/loom-domain";
import { CellEvaluation } from "@cvr/loom-protocol";
import { Config, Effect, Layer, Schema } from "effect";
import { layerBunLoomClient } from "../src/index.js";

const encodeEvaluation = Schema.encodeSync(Schema.fromJsonString(CellEvaluation));

const live = Layer.unwrap(
  Effect.gen(function* () {
    const socketPath = yield* Config.string("LOOM_PROBE_SOCKET");
    const workspaceRoot = WorkspaceRoot.make(yield* Config.string("LOOM_PROBE_WORKSPACE"));
    return layerBunLoomClient({ socketPath, workspaceRoot });
  }),
).pipe(Layer.provide(BunServices.layer));

const program = Effect.gen(function* () {
  const sessionId = SessionId.make(
    yield* Config.string("LOOM_PROBE_SESSION").pipe(Config.withDefault("pi-live-session")),
  );
  const agentId = AgentId.make(
    yield* Config.string("LOOM_PROBE_AGENT").pipe(Config.withDefault("pi-live-agent")),
  );
  const cellId = CellId.make(
    yield* Config.string("LOOM_PROBE_CELL").pipe(Config.withDefault("pi-live-cell")),
  );
  const source = yield* Config.string("LOOM_PROBE_SOURCE").pipe(Config.withDefault("40 + 2"));
  const client = yield* LoomClient;
  const evaluation = yield* client.evaluateCell({ sessionId, agentId, cellId, source });
  yield* Effect.log(encodeEvaluation(evaluation));
}).pipe(Effect.provide(live));

BunRuntime.runMain(program);
