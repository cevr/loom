/* oxlint-disable effect/noGlobals -- This named Bun adapter owns the Bun JSONL parser. */
import { Effect } from "effect";
import { BunJsonlError } from "./bun-jsonl-error.js";

export const parseBunJsonLine = Effect.fn("BunJsonl.parseLine")(function* (line: string) {
  return yield* Effect.try({
    try: () => Bun.JSONL.parse(line)[0],
    catch: (cause) => new BunJsonlError({ cause }),
  });
});
