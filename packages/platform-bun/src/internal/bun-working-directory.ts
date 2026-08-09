/* oxlint-disable effect/noGlobals -- This named Bun adapter owns current-directory inspection. */
import { Effect } from "effect";

export const currentWorkingDirectory: Effect.Effect<string> = Effect.sync(() => process.cwd());
