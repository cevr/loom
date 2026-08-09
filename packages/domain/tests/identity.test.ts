import { describe, expect, it } from "effect-bun-test";
import { Effect, Exit, Schema } from "effect";
import { AgentOwner } from "../src/index.js";

describe("Loom identity", () => {
  it.effect("decodes an agent owner", () =>
    Effect.gen(function* () {
      const owner = yield* Schema.decodeUnknownEffect(AgentOwner)({
        sessionId: "session-1",
        agentId: "agent-1",
      });

      expect(String(owner.sessionId)).toBe("session-1");
      expect(String(owner.agentId)).toBe("agent-1");
    }),
  );

  it.effect("rejects an empty identifier", () =>
    Effect.gen(function* () {
      const exit = yield* Schema.decodeUnknownEffect(AgentOwner)({
        sessionId: "",
        agentId: "agent-1",
      }).pipe(Effect.exit);

      expect(Exit.isFailure(exit)).toBe(true);
    }),
  );
});
