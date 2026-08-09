import { expect, it } from "effect-bun-test";
import { Effect } from "effect";
import { startupMessage } from "../src/program.js";

it.effect("provides the daemon startup message", () =>
  Effect.gen(function* () {
    const message = yield* startupMessage;
    expect(message).toBe("Loom daemon is ready");
  }),
);
