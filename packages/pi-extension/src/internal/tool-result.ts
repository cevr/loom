import type { AgentToolResult } from "@earendil-works/pi-coding-agent";
import { Effect, Schema } from "effect";

const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

export const toolResult = <A>(result: A): AgentToolResult<{ readonly result: A }> => ({
  content: [{ type: "text", text: encodeJson(result) }],
  details: { result },
});

export const runTool = <A, E>(
  effect: Effect.Effect<A, E>,
  options: { readonly signal?: AbortSignal },
): Promise<A> =>
  Effect.runPromise(
    effect.pipe(
      Effect.matchEffect({
        onFailure: (error) => Effect.die(new Error(encodeJson(error))),
        onSuccess: Effect.succeed,
      }),
    ),
    options,
  );
