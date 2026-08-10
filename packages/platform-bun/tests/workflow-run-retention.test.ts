import { WorkflowDefinition, WorkflowKey, WorkflowSignalName } from "@cvr/loom-domain";
import { WorkflowRuntime } from "@cvr/loom-runtime";
import { expect } from "effect-bun-test";
import { Effect, FileSystem, Ref, Schedule } from "effect";
import { layerLoomWorkflowRuntimeWith } from "../src/index.js";
import { request } from "./workflow-runtime-fixtures.js";
import { runtimeLayer, scopedLive, storageCounts } from "./workflow-runtime-test-support.js";

scopedLive("retires terminal Workflow storage after its state lease", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-workflow-retention-" });
    const filename = `${directory}/loom.sqlite`;
    const executions = yield* Ref.make(0);
    const acceptedRequest = {
      ...request,
      key: WorkflowKey.make("retention"),
      definition: WorkflowDefinition.make({
        ...request.definition,
        signals: [WorkflowSignalName.make("unused")],
      }),
    };

    const counts = yield* Effect.scoped(
      Effect.gen(function* () {
        const runtime = yield* WorkflowRuntime;
        const workflowRunId = yield* runtime.send(acceptedRequest);
        yield* runtime.wait(acceptedRequest);
        expect((yield* storageCounts).acceptance).toBe(1);
        const retired = yield* storageCounts.pipe(
          Effect.repeat({
            while: ({ acceptance }) => acceptance > 0,
            schedule: Schedule.spaced("10 millis"),
          }),
        );
        const unavailable = yield* runtime
          .inspect({ sessionId: acceptedRequest.sessionId, workflowRunId })
          .pipe(Effect.flip);
        expect(unavailable).toHaveProperty("_tag", "WorkflowRunNotFoundError");
        return retired;
      }).pipe(
        Effect.provide(
          runtimeLayer(
            filename,
            executions,
            layerLoomWorkflowRuntimeWith({ stateLease: "500 millis" }),
          ),
        ),
      ),
    );

    expect(counts).toEqual({ acceptance: 0, signals: 0, messages: 0, replies: 0 });
  }),
);

scopedLive("prunes old terminal Workflow storage during startup", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-workflow-startup-" });
    const filename = `${directory}/loom.sqlite`;
    const executions = yield* Ref.make(0);

    const before = yield* Effect.scoped(
      Effect.gen(function* () {
        const runtime = yield* WorkflowRuntime;
        yield* runtime.execute(request);
        return yield* storageCounts;
      }).pipe(Effect.provide(runtimeLayer(filename, executions))),
    );
    expect(before.acceptance).toBe(1);

    const after = yield* Effect.scoped(
      Effect.gen(function* () {
        yield* WorkflowRuntime;
        return yield* storageCounts;
      }).pipe(Effect.provide(runtimeLayer(filename, executions))),
    );

    expect(after).toEqual({
      acceptance: 0,
      signals: 0,
      messages: 0,
      replies: 0,
    });
  }),
);
