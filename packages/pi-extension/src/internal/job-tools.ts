import { JobId, type SessionId } from "@cvr/loom-domain";
import {
  maximumJobOutputBytes,
  ReadJobOutputRequest,
  StartJobRequest,
  WaitForJobRequest,
} from "@cvr/loom-protocol";
import { Type } from "@earendil-works/pi-ai";
import { Effect, Encoding, Schema } from "effect";
import type { LoomExtensionApi } from "./extension-api.js";
import { loomTool } from "./loom-tool-ui.js";
import { runLoomTool } from "./loom-tool.js";
import { toolResult } from "./tool-result.js";

const addressParameters = Type.Object({ jobId: Type.String({ minLength: 1 }) });
const decodeStartJobRequest = Schema.decodeUnknownEffect(StartJobRequest);
const decodeReadJobOutputRequest = Schema.decodeUnknownEffect(ReadJobOutputRequest);
const decodeWaitForJobRequest = Schema.decodeUnknownEffect(WaitForJobRequest);

const address = (sessionId: SessionId, jobId: string) => ({
  sessionId,
  jobId: JobId.make(jobId),
});

export const startJobRequest = (
  sessionId: SessionId,
  jobId: string,
  parameters: {
    readonly command: string;
    readonly foregroundLeaseMillis?: number;
    readonly attached?: boolean;
  },
) => decodeStartJobRequest({ ...address(sessionId, jobId), ...parameters });

export const readJobOutputRequest = (
  sessionId: SessionId,
  parameters: {
    readonly jobId: string;
    readonly stream: "stdout" | "stderr";
    readonly sequence?: number;
    readonly maximumBytes?: number;
  },
) => decodeReadJobOutputRequest({ sessionId, ...parameters });

export const waitForJobRequest = (
  sessionId: SessionId,
  parameters: { readonly jobId: string; readonly foregroundLeaseMillis?: number },
) => decodeWaitForJobRequest({ sessionId, ...parameters });

const registerStartJob = (pi: LoomExtensionApi) =>
  pi.registerTool(
    loomTool({
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
      execute: (toolCallId, parameters, signal, _onUpdate, context) =>
        runLoomTool(context, { signal }, "5 seconds", (client, sessionId) =>
          startJobRequest(sessionId, toolCallId, parameters).pipe(
            Effect.flatMap(client.startJob),
            Effect.map(toolResult),
          ),
        ),
    }),
  );

const registerInspectJob = (pi: LoomExtensionApi) =>
  pi.registerTool(
    loomTool({
      name: "loom_job_inspect",
      label: "Inspect Loom Job",
      description: "Read the current durable Job state.",
      parameters: addressParameters,
      execute: (_toolCallId, parameters, signal, _onUpdate, context) =>
        runLoomTool(context, { signal }, "5 seconds", (client, sessionId) =>
          client.inspectJob(address(sessionId, parameters.jobId)).pipe(Effect.map(toolResult)),
        ),
    }),
  );

const registerReadJobOutput = (pi: LoomExtensionApi) =>
  pi.registerTool(
    loomTool({
      name: "loom_job_output",
      label: "Read Loom Job Output",
      description: "Read one bounded stdout or stderr chunk from a byte sequence.",
      parameters: Type.Object({
        jobId: Type.String({ minLength: 1 }),
        stream: Type.Union([Type.Literal("stdout"), Type.Literal("stderr")]),
        sequence: Type.Optional(Type.Integer({ minimum: 0 })),
        maximumBytes: Type.Optional(Type.Integer({ minimum: 1, maximum: maximumJobOutputBytes })),
      }),
      execute: (_toolCallId, parameters, signal, _onUpdate, context) =>
        runLoomTool(context, { signal }, "5 seconds", (client, sessionId) =>
          readJobOutputRequest(sessionId, parameters).pipe(
            Effect.flatMap(client.readJobOutput),
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
    }),
  );

const registerAwaitJob = (pi: LoomExtensionApi) =>
  pi.registerTool(
    loomTool({
      name: "loom_job_await",
      label: "Await Loom Job",
      description: "Wait for a Job until it ends or its foreground lease expires.",
      parameters: Type.Object({
        jobId: Type.String({ minLength: 1 }),
        foregroundLeaseMillis: Type.Optional(Type.Integer({ minimum: 0 })),
      }),
      execute: (_toolCallId, parameters, signal, _onUpdate, context) =>
        runLoomTool(context, { signal }, "5 seconds", (client, sessionId) =>
          waitForJobRequest(sessionId, parameters).pipe(
            Effect.flatMap(client.awaitJob),
            Effect.map(toolResult),
          ),
        ),
    }),
  );

const registerCancelJob = (pi: LoomExtensionApi) =>
  pi.registerTool(
    loomTool({
      name: "loom_job_cancel",
      label: "Cancel Loom Job",
      description: "Stop a Job process group and record Cancelled.",
      parameters: addressParameters,
      execute: (_toolCallId, parameters, signal, _onUpdate, context) =>
        runLoomTool(context, { signal }, "10 seconds", (client, sessionId) =>
          client.cancelJob(address(sessionId, parameters.jobId)).pipe(Effect.map(toolResult)),
        ),
    }),
  );

const registerDetachJob = (pi: LoomExtensionApi) =>
  pi.registerTool(
    loomTool({
      name: "loom_job_detach",
      label: "Detach Loom Job",
      description: "Detach a Job so Session close does not cancel it.",
      parameters: addressParameters,
      execute: (_toolCallId, parameters, signal, _onUpdate, context) =>
        runLoomTool(context, { signal }, "5 seconds", (client, sessionId) =>
          client.detachJob(address(sessionId, parameters.jobId)).pipe(Effect.map(toolResult)),
        ),
    }),
  );

export const registerJobTools = (pi: LoomExtensionApi): void => {
  registerStartJob(pi);
  registerInspectJob(pi);
  registerReadJobOutput(pi);
  registerAwaitJob(pi);
  registerCancelJob(pi);
  registerDetachJob(pi);
};
