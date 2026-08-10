import {
  AgentId,
  CellId,
  SessionId,
  WorkflowKey,
  WorkflowName,
  WorkflowRequestDigest,
  WorkflowRunId,
  WorkflowSignalAddress,
  WorkflowSignalName,
  WorkflowVersion,
  WorkspaceRoot,
} from "@cvr/loom-domain";
import { describe, expect, it } from "effect-bun-test";
import { Effect, Exit, Schema } from "effect";
import {
  CellInterruptedError,
  CodeKernelDiagnostic,
  CodeKernelProcessRequest,
  CodeKernelProcessResponse,
  EvaluateCell,
  EvaluateCellRequest,
  HandshakeRequest,
  LoomRpcs,
  SignalWorkflowRequest,
  WorkflowIdentityConflictError,
  WorkflowRunState,
  WorkflowSignalNotDeclaredError,
} from "../src/index.js";

describe("Loom RPC protocol", () => {
  it.effect("decodes an evaluation request", () =>
    Effect.gen(function* () {
      const request = yield* Schema.decodeUnknownEffect(EvaluateCellRequest)({
        sessionId: SessionId.make("session-1"),
        agentId: AgentId.make("agent-1"),
        cellId: CellId.make("cell-1"),
        source: "const answer: number = 42",
      });

      expect(request.source).toBe("const answer: number = 42");
    }),
  );

  it.effect("rejects an empty agent identifier", () =>
    Effect.gen(function* () {
      const exit = yield* Schema.decodeUnknownEffect(EvaluateCell.payloadSchema)({
        sessionId: "session-1",
        agentId: "",
        cellId: "cell-1",
        source: "1 + 1",
      }).pipe(Effect.exit);

      expect(Exit.isFailure(exit)).toBe(true);
    }),
  );

  it.effect("decodes the connection handshake", () =>
    Effect.gen(function* () {
      const request = yield* Schema.decodeUnknownEffect(HandshakeRequest)({
        workspaceRoot: "/workspace",
        minimumProtocolVersion: 1,
        maximumProtocolVersion: 1,
      });

      expect(request.workspaceRoot).toBe(WorkspaceRoot.make("/workspace"));
    }),
  );
});

describe("Loom RPC registry", () => {
  it.effect("registers the code kernel procedures", () =>
    Effect.sync(() => {
      expect(Array.from(LoomRpcs.requests.keys())).toEqual([
        "Connection.Handshake",
        "Session.Close",
        "CodeKernel.EvaluateCell",
        "CodeKernel.Reset",
        "Workflow.Start",
        "Workflow.Execute",
        "Workflow.Signal",
        "Workflow.Inspect",
        "Workflow.Interrupt",
        "Workflow.Resume",
      ]);
    }),
  );
});

describe("Code Kernel process protocol", () => {
  it.effect("decodes the Code Kernel process frames", () =>
    Effect.gen(function* () {
      const request = yield* Schema.decodeUnknownEffect(CodeKernelProcessRequest)({
        _tag: "Evaluate",
        requestId: 1,
        cellId: "cell-1",
        source: "21 * 2",
      });
      const response = yield* Schema.decodeUnknownEffect(CodeKernelProcessResponse)({
        _tag: "Ready",
      });

      expect(request).toHaveProperty("_tag", "Evaluate");
      expect(response).toHaveProperty("_tag", "Ready");
    }),
  );
});

describe("Code Kernel diagnostics", () => {
  it.effect("round-trips an interruption without diagnostic data", () =>
    Effect.gen(function* () {
      const codec = Schema.fromJsonString(CellInterruptedError);
      const encoded = yield* Schema.encodeEffect(codec)(
        new CellInterruptedError({
          cellId: CellId.make("cell-1"),
          reason: "ProcessExited",
          message: "Code Kernel process did not start.",
        }),
      );
      const decoded = yield* Schema.decodeEffect(codec)(encoded);

      expect(decoded.diagnostic).toBeUndefined();
    }),
  );

  it.effect("round-trips Code Kernel diagnostic data", () =>
    Effect.gen(function* () {
      const codec = Schema.fromJsonString(CellInterruptedError);
      const diagnostic = CodeKernelDiagnostic.make({
        requestId: 1,
        exitCode: 23,
        stderrTail: "kernel failed\n",
        stderrPath: "/tmp/kernel.stderr.log",
      });
      const encoded = yield* Schema.encodeEffect(codec)(
        new CellInterruptedError({
          cellId: CellId.make("cell-1"),
          reason: "ProcessExited",
          message: "Code Kernel process exited.",
          diagnostic,
        }),
      );
      const decoded = yield* Schema.decodeEffect(codec)(encoded);

      expect(decoded.diagnostic).toEqual(diagnostic);
    }),
  );
});

describe("Workflow Run state", () => {
  it.effect("round-trips public Workflow Run state", () =>
    Effect.gen(function* () {
      const state = WorkflowRunState.cases.Success.make({ value: { answer: 42 } });
      const codec = Schema.fromJsonString(WorkflowRunState);

      expect(yield* Schema.decodeEffect(codec)(yield* Schema.encodeEffect(codec)(state))).toEqual(
        state,
      );
    }),
  );
});

describe("Workflow protocol", () => {
  it.effect("round-trips a public Workflow signal address", () =>
    Effect.gen(function* () {
      const address = WorkflowSignalAddress.make({
        workflowRunId: WorkflowRunId.make("workflow-run-1"),
        name: WorkflowSignalName.make("approval"),
      });
      const request = yield* Schema.decodeUnknownEffect(SignalWorkflowRequest)({
        address,
        value: { approved: true },
      });
      const codec = Schema.fromJsonString(WorkflowSignalNotDeclaredError);
      const encoded = yield* Schema.encodeEffect(codec)(
        new WorkflowSignalNotDeclaredError({ address }),
      );
      const decoded = yield* Schema.decodeEffect(codec)(encoded);

      expect(request.address).toEqual(address);
      expect(decoded.address).toEqual(address);
    }),
  );

  it.effect("round-trips a Workflow identity conflict", () =>
    Effect.gen(function* () {
      const acceptedDigest = WorkflowRequestDigest.make(`sha256:${"a".repeat(64)}`);
      const receivedDigest = WorkflowRequestDigest.make(`sha256:${"b".repeat(64)}`);
      const codec = Schema.fromJsonString(WorkflowIdentityConflictError);
      const encoded = yield* Schema.encodeEffect(codec)(
        new WorkflowIdentityConflictError({
          identity: {
            sessionId: SessionId.make("session-1"),
            name: WorkflowName.make("ReviewRepository"),
            version: WorkflowVersion.make("1"),
            key: WorkflowKey.make("daily-review"),
          },
          acceptedDigest,
          receivedDigest,
        }),
      );
      const decoded = yield* Schema.decodeEffect(codec)(encoded);

      expect(decoded.acceptedDigest).toBe(acceptedDigest);
      expect(decoded.receivedDigest).toBe(receivedDigest);
    }),
  );
});
