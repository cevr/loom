import { Effect } from "effect";

export const startupMessage = Effect.succeed("Loom daemon is ready");

export const program = startupMessage.pipe(Effect.flatMap(Effect.logInfo));
