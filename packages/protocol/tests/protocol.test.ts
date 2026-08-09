import { AgentId, CellId, SessionId, WorkspaceRoot } from "@cvr/loom-domain";
import { describe, expect, it } from "effect-bun-test";
import { Effect, Exit, Schema } from "effect";
import { EvaluateCell, EvaluateCellRequest, HandshakeRequest, LoomRpcs } from "../src/index.js";

describe("Loom RPC protocol", () => {
  it.effect("decodes an evaluation request", () =>
    Effect.gen(function* () {
      const request = yield* Schema.decodeUnknownEffect(EvaluateCellRequest)({
        sessionId: SessionId.make("session-1"),
        agentId: AgentId.make("agent-1"),
        cellId: CellId.make("cell-1"),
        source: "const answer: number = 42",
      });

      expect(request.source).toBe("const answer: number = 42");
    }),
  );

  it.effect("rejects an empty agent identifier", () =>
    Effect.gen(function* () {
      const exit = yield* Schema.decodeUnknownEffect(EvaluateCell.payloadSchema)({
        sessionId: "session-1",
        agentId: "",
        cellId: "cell-1",
        source: "1 + 1",
      }).pipe(Effect.exit);

      expect(Exit.isFailure(exit)).toBe(true);
    }),
  );

  it.effect("decodes the connection handshake", () =>
    Effect.gen(function* () {
      const request = yield* Schema.decodeUnknownEffect(HandshakeRequest)({
        workspaceRoot: "/workspace",
        minimumProtocolVersion: 1,
        maximumProtocolVersion: 1,
      });

      expect(request.workspaceRoot).toBe(WorkspaceRoot.make("/workspace"));
    }),
  );

  it.effect("registers the code kernel procedures", () =>
    Effect.sync(() => {
      expect(Array.from(LoomRpcs.requests.keys())).toEqual([
        "Connection.Handshake",
        "CodeKernel.EvaluateCell",
        "CodeKernel.Reset",
      ]);
    }),
  );
});
