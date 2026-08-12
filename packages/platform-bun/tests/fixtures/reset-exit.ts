/* oxlint-disable effect/noAsyncFunction, effect/noGlobals, eslint/no-await-in-loop -- This worker fixture owns a sequential protocol loop. */
import { CodeKernelProcessRequest, CodeKernelProcessResponse } from "@cvr/loom-protocol";
import { Schema } from "effect";

const decodeRequest = Schema.decodeUnknownSync(CodeKernelProcessRequest);
const encodeResponse = Schema.encodeSync(Schema.fromJsonString(CodeKernelProcessResponse));
const ready = encodeResponse(CodeKernelProcessResponse.cases.Ready.make({}));
const decoder = new TextDecoder();
let buffer = "";

await Bun.stdout.write(`${ready}\n`);
for await (const chunk of Bun.stdin.stream()) {
  buffer += decoder.decode(chunk, { stream: true });
  const parsed = Bun.JSONL.parseChunk(buffer);
  buffer = buffer.slice(parsed.read);
  for (const value of parsed.values) {
    const request = decodeRequest(value);
    if (CodeKernelProcessRequest.guards.Reset(request)) process.exit(17);
    const response = CodeKernelProcessResponse.cases.EvaluationSucceeded.make({
      requestId: request.requestId,
      evaluation: {
        cellId: request.cellId,
        display: "42",
        bindings: [],
        durationMillis: 0,
        fileChanges: [],
      },
    });
    await Bun.stdout.write(`${encodeResponse(response)}\n`);
  }
}
