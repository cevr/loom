import { JobId, SessionId } from "@cvr/loom-domain";
import { expect, it } from "effect-bun-test";
import { Effect, Exit, Schema } from "effect";
import {
  JobOutputChunk,
  ReadJobOutputRequest,
  StartJobRequest,
  WaitForJobRequest,
} from "../src/index.js";

it.effect("owns the encoded Job request defaults", () =>
  Effect.gen(function* () {
    const sessionId = SessionId.make("session-1");
    const jobId = JobId.make("job-1");
    const start = StartJobRequest.make({ sessionId, jobId, command: "true" });
    const wait = WaitForJobRequest.make({ sessionId, jobId });
    const output = ReadJobOutputRequest.make({ sessionId, jobId, stream: "stdout" });

    expect(yield* Schema.encodeEffect(Schema.fromJsonString(StartJobRequest))(start)).toBe(
      '{"sessionId":"session-1","jobId":"job-1","command":"true","attached":true,"foregroundLeaseMillis":300000}',
    );
    expect(yield* Schema.encodeEffect(Schema.fromJsonString(WaitForJobRequest))(wait)).toBe(
      '{"sessionId":"session-1","jobId":"job-1","foregroundLeaseMillis":300000}',
    );
    expect(yield* Schema.encodeEffect(Schema.fromJsonString(ReadJobOutputRequest))(output)).toBe(
      '{"sessionId":"session-1","jobId":"job-1","stream":"stdout","sequence":0,"maximumBytes":16384}',
    );
  }),
);

it.effect("resolves the same Job defaults during decoding", () =>
  Effect.gen(function* () {
    const start = yield* Schema.decodeUnknownEffect(StartJobRequest)({
      sessionId: "session-1",
      jobId: "job-1",
      command: "true",
    });
    const wait = yield* Schema.decodeUnknownEffect(WaitForJobRequest)({
      sessionId: "session-1",
      jobId: "job-1",
    });
    const output = yield* Schema.decodeUnknownEffect(ReadJobOutputRequest)({
      sessionId: "session-1",
      jobId: "job-1",
      stream: "stdout",
    });
    const sessionId = SessionId.make("session-1");
    const jobId = JobId.make("job-1");

    expect(start).toEqual(StartJobRequest.make({ sessionId, jobId, command: "true" }));
    expect(wait).toEqual(WaitForJobRequest.make({ sessionId, jobId }));
    expect(output).toEqual(ReadJobOutputRequest.make({ sessionId, jobId, stream: "stdout" }));
  }),
);

it.effect("round-trips a bounded output chunk", () =>
  Effect.gen(function* () {
    const request = yield* Schema.decodeUnknownEffect(ReadJobOutputRequest)({
      sessionId: "session-1",
      jobId: "job-1",
      stream: "stdout",
      sequence: 3,
      maximumBytes: 128,
    });
    const codec = Schema.fromJsonString(JobOutputChunk);
    const chunk = JobOutputChunk.make({
      stream: "stdout",
      sequence: request.sequence,
      nextSequence: 6,
      data: new Uint8Array([97, 98, 99]),
      complete: false,
    });
    expect(yield* Schema.decodeEffect(codec)(yield* Schema.encodeEffect(codec)(chunk))).toEqual(
      chunk,
    );
  }),
);

it.effect("rejects an output request outside the byte boundary", () =>
  Effect.gen(function* () {
    const decode = Schema.decodeUnknownEffect(ReadJobOutputRequest);
    const request = { sessionId: "session-1", jobId: "job-1", stream: "stdout" };

    expect(Exit.isFailure(yield* decode({ ...request, maximumBytes: 0 }).pipe(Effect.exit))).toBe(
      true,
    );
    expect(
      Exit.isFailure(yield* decode({ ...request, maximumBytes: 262_145 }).pipe(Effect.exit)),
    ).toBe(true);
  }),
);
