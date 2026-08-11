import { BunServices } from "@effect/platform-bun";
import { LoomClient, MessageTooLargeError } from "@cvr/loom-client";
import { AgentId, CellId, SessionId, WorkspaceRoot } from "@cvr/loom-domain";
import { maximumCellSourceLength } from "@cvr/loom-protocol";
import { expect, it } from "effect-bun-test";
import { Effect } from "effect";
import { layerBunLoomClient } from "../src/index.js";

it.scoped.layer(BunServices.layer)("rejects an oversized Cell before socket I/O", () =>
  Effect.gen(function* () {
    const client = yield* LoomClient;
    const error = yield* client
      .evaluateCell({
        sessionId: SessionId.make("session-1"),
        agentId: AgentId.make("agent-1"),
        cellId: CellId.make("cell-large"),
        source: "x".repeat(maximumCellSourceLength + 1),
      })
      .pipe(Effect.flip);

    expect(error).toBeInstanceOf(MessageTooLargeError);
  }).pipe(
    Effect.provide(
      layerBunLoomClient({
        socketPath: "/tmp/loom-not-used.sock",
        workspaceRoot: WorkspaceRoot.make("/workspace"),
      }),
    ),
  ),
);
