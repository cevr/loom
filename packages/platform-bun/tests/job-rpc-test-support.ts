import { JobId, type SessionId } from "@cvr/loom-domain";
import { JobState, type StartJobRequest } from "@cvr/loom-protocol";
import { Effect } from "effect";

export const makeStubJobHandlers = (sessionId: SessionId) => {
  const job = JobState.make({
    jobId: JobId.make("job-1"),
    sessionId,
    command: "exit 0",
    attached: true,
    status: "Succeeded",
    exitCode: 0,
  });
  return {
    "Job.Start": (_request: StartJobRequest) => Effect.succeed(job),
    "Job.Inspect": () => Effect.succeed(job),
    "Job.Output": (request: { readonly stream: "stdout" | "stderr"; readonly sequence: number }) =>
      Effect.succeed({
        stream: request.stream,
        sequence: request.sequence,
        nextSequence: request.sequence,
        data: new Uint8Array(),
        complete: true,
      }),
    "Job.Await": () => Effect.succeed(job),
    "Job.Cancel": () => Effect.succeed(job),
    "Job.Detach": () => Effect.succeed(job),
  };
};
