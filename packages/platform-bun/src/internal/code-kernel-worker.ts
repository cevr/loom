import { CodeKernelProcessRequest, CodeKernelProcessResponse } from "@cvr/loom-protocol";
import { SessionId } from "@cvr/loom-domain";
import { Config, Effect, Schema, Stdio, Stream } from "effect";
import { makeInProcessCodeKernelFor, type InProcessCodeKernelShape } from "./code-kernel.js";
import { parseBunJsonLine } from "./bun-jsonl.js";

const decodeRequest = Schema.decodeUnknownEffect(CodeKernelProcessRequest);
const encodeResponse = Schema.encodeEffect(Schema.fromJsonString(CodeKernelProcessResponse));

const respond = (
  request: CodeKernelProcessRequest,
  kernel: InProcessCodeKernelShape,
): Effect.Effect<CodeKernelProcessResponse> =>
  CodeKernelProcessRequest.match<Effect.Effect<CodeKernelProcessResponse>>(request, {
    Evaluate: ({ requestId, cellId, source }) =>
      kernel.evaluate({ cellId, source }).pipe(
        Effect.match({
          onFailure: (error) =>
            CodeKernelProcessResponse.cases.EvaluationFailed.make({ requestId, error }),
          onSuccess: (evaluation) =>
            CodeKernelProcessResponse.cases.EvaluationSucceeded.make({
              requestId,
              evaluation,
            }),
        }),
      ),
    Reset: ({ requestId }) =>
      kernel.reset.pipe(
        Effect.as(CodeKernelProcessResponse.cases.ResetSucceeded.make({ requestId })),
      ),
  });

export const runCodeKernelWorker = Effect.gen(function* () {
  const stdio = yield* Stdio.Stdio;
  const sessionId = yield* Config.string("LOOM_SESSION_ID").pipe(Effect.map(SessionId.make));
  const kernel = yield* makeInProcessCodeKernelFor(sessionId);
  const ready = yield* encodeResponse(CodeKernelProcessResponse.cases.Ready.make({}));
  yield* Stream.succeed(`${ready}\n`).pipe(
    Stream.encodeText,
    Stream.run(stdio.stdout({ endOnDone: false })),
  );
  const requests = stdio.stdin.pipe(
    Stream.decodeText(),
    Stream.splitLines,
    Stream.filter((line) => line.length > 0),
    Stream.mapEffect(parseBunJsonLine),
    Stream.mapEffect((value) => decodeRequest(value)),
  );

  yield* requests.pipe(
    Stream.mapEffect((request) => respond(request, kernel)),
    Stream.mapEffect((response) => encodeResponse(response)),
    Stream.map((encoded) => `${encoded}\n`),
    Stream.run(stdio.stdout({ endOnDone: false })),
  );
});
