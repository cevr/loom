import { JobId, SessionId } from "@cvr/loom-domain";
import {
  defaultForegroundLeaseMillis,
  JobOutputStream,
  maximumJobOutputBytes,
} from "@cvr/loom-protocol";
import { StringEnum, Type } from "@earendil-works/pi-ai";
import { Effect, Encoding, Schema } from "effect";
import type { LoomExtensionApi } from "./extension-api.js";
import { runWithLoomClient } from "./loom-connection.js";
import { runTool, toolResult } from "./tool-result.js";

const addressParameters = Type.Object({ jobId: Type.String({ minLength: 1 }) });
const decodeOutputStream = Schema.decodeUnknownEffect(JobOutputStream);

const address = (sessionId: string, jobId: string) => ({
  sessionId: SessionId.make(sessionId),
  jobId: JobId.make(jobId),
});

const registerStartJob = (pi: LoomExtensionApi) =>
  pi.registerTool({
    name: "loom_job_start",
    label: "Start Loom Job",
    description: "Start a durable background-safe command and return its Job state.",
    promptSnippet: "Run shell commands as durable Loom Jobs",
    promptGuidelines: [
      "Use loom_job_start for commands that can block, run for a long time, or continue in the background.",
      "Use a zero foreground lease when only a Job handle is needed.",
    ],
    parameters: Type.Object({
      command: Type.String({ minLength: 1 }),
      foregroundLeaseMillis: Type.Optional(Type.Integer({ minimum: 0 })),
      attached: Type.Optional(Type.Boolean()),
    }),
    execute: (toolCallId, parameters, signal, _onUpdate, context) => {
      const foregroundLeaseMillis =
        parameters.foregroundLeaseMillis ?? defaultForegroundLeaseMillis;
      return runTool(
        runWithLoomClient(context.cwd, "5 seconds", (client) =>
          client
            .startJob({
              ...address(context.sessionManager.getSessionId(), toolCallId),
              command: parameters.command,
              attached: parameters.attached ?? true,
              foregroundLeaseMillis,
            })
            .pipe(Effect.map(toolResult)),
        ),
        { signal },
      );
    },
  });

const registerInspectJob = (pi: LoomExtensionApi) =>
  pi.registerTool({
    name: "loom_job_inspect",
    label: "Inspect Loom Job",
    description: "Read the current durable Job state.",
    parameters: addressParameters,
    execute: (_toolCallId, parameters, signal, _onUpdate, context) =>
      runTool(
        runWithLoomClient(context.cwd, "5 seconds", (client) =>
          client
            .inspectJob(address(context.sessionManager.getSessionId(), parameters.jobId))
            .pipe(Effect.map(toolResult)),
        ),
        { signal },
      ),
  });

const registerReadJobOutput = (pi: LoomExtensionApi) =>
  pi.registerTool({
    name: "loom_job_output",
    label: "Read Loom Job Output",
    description: "Read one bounded stdout or stderr chunk from a byte sequence.",
    parameters: Type.Object({
      jobId: Type.String({ minLength: 1 }),
      stream: StringEnum(["stdout", "stderr"]),
      sequence: Type.Optional(Type.Integer({ minimum: 0 })),
      maximumBytes: Type.Optional(Type.Integer({ minimum: 1, maximum: maximumJobOutputBytes })),
    }),
    execute: (_toolCallId, parameters, signal, _onUpdate, context) =>
      runTool(
        runWithLoomClient(context.cwd, "5 seconds", (client) =>
          Effect.gen(function* () {
            const stream = yield* decodeOutputStream(parameters.stream);
            return yield* client.readJobOutput({
              ...address(context.sessionManager.getSessionId(), parameters.jobId),
              stream,
              sequence: parameters.sequence ?? 0,
              maximumBytes: parameters.maximumBytes ?? 16 * 1_024,
            });
          }).pipe(
            Effect.map((chunk) =>
              toolResult({
                stream: chunk.stream,
                sequence: chunk.sequence,
                nextSequence: chunk.nextSequence,
                text: new TextDecoder().decode(chunk.data),
                base64: Encoding.encodeBase64(chunk.data),
                complete: chunk.complete,
              }),
            ),
          ),
        ),
        { signal },
      ),
  });

const registerAwaitJob = (pi: LoomExtensionApi) =>
  pi.registerTool({
    name: "loom_job_await",
    label: "Await Loom Job",
    description: "Wait for a Job until it ends or its foreground lease expires.",
    parameters: Type.Object({
      jobId: Type.String({ minLength: 1 }),
      foregroundLeaseMillis: Type.Optional(Type.Integer({ minimum: 0 })),
    }),
    execute: (_toolCallId, parameters, signal, _onUpdate, context) => {
      const foregroundLeaseMillis =
        parameters.foregroundLeaseMillis ?? defaultForegroundLeaseMillis;
      return runTool(
        runWithLoomClient(context.cwd, "5 seconds", (client) =>
          client
            .awaitJob({
              ...address(context.sessionManager.getSessionId(), parameters.jobId),
              foregroundLeaseMillis,
            })
            .pipe(Effect.map(toolResult)),
        ),
        { signal },
      );
    },
  });

const registerCancelJob = (pi: LoomExtensionApi) =>
  pi.registerTool({
    name: "loom_job_cancel",
    label: "Cancel Loom Job",
    description: "Stop a Job process group and record Cancelled.",
    parameters: addressParameters,
    execute: (_toolCallId, parameters, signal, _onUpdate, context) =>
      runTool(
        runWithLoomClient(context.cwd, "10 seconds", (client) =>
          client
            .cancelJob(address(context.sessionManager.getSessionId(), parameters.jobId))
            .pipe(Effect.map(toolResult)),
        ),
        { signal },
      ),
  });

const registerDetachJob = (pi: LoomExtensionApi) =>
  pi.registerTool({
    name: "loom_job_detach",
    label: "Detach Loom Job",
    description: "Detach a Job so Session close does not cancel it.",
    parameters: addressParameters,
    execute: (_toolCallId, parameters, signal, _onUpdate, context) =>
      runTool(
        runWithLoomClient(context.cwd, "5 seconds", (client) =>
          client
            .detachJob(address(context.sessionManager.getSessionId(), parameters.jobId))
            .pipe(Effect.map(toolResult)),
        ),
        { signal },
      ),
  });

export const registerJobTools = (pi: LoomExtensionApi): void => {
  registerStartJob(pi);
  registerInspectJob(pi);
  registerReadJobOutput(pi);
  registerAwaitJob(pi);
  registerCancelJob(pi);
  registerDetachJob(pi);
};
