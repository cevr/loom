import {
  WorkflowActivityKey,
  WorkflowDefinition,
  WorkflowKey,
  WorkflowSignalName,
} from "@cvr/loom-domain";
import { WorkflowRunRecovery, WorkflowRuntime } from "@cvr/loom-runtime";
import { expect } from "effect-bun-test";
import { DateTime, Effect, FileSystem, Option, Ref, Schedule } from "effect";
import { request } from "./workflow-runtime-fixtures.js";
import {
  claimWorkflowChildAgent,
  emptyWorkflowStorage,
  expireRetirement,
  leasedRuntime,
  retirementDeadline,
  scopedLive,
  storageCounts,
} from "./workflow-runtime-test-support.js";

const completeWithDeadline = (filename: string, executions: Ref.Ref<number>) =>
  Effect.scoped(
    Effect.gen(function* () {
      const runtime = yield* WorkflowRuntime;
      const workflowRunId = yield* runtime.send(request);
      yield* runtime.wait({ sessionId: request.sessionId, workflowRunId });
      const retirement = yield* retirementDeadline(workflowRunId).pipe(
        Effect.repeat({
          while: ({ retireAfter }) => Option.isNone(retireAfter),
          schedule: Schedule.spaced("10 millis"),
        }),
      );
      const retireAfter = yield* Option.match(retirement.retireAfter, {
        onNone: () => Effect.die("No Workflow retirement deadline was stored."),
        onSome: Effect.succeed,
      });
      return { workflowRunId, retireAfter };
    }).pipe(Effect.provide(leasedRuntime(filename, executions, "500 millis"))),
  );

const recoverBeforeExpiry = (
  filename: string,
  executions: Ref.Ref<number>,
  workflowRunId: Parameters<typeof retirementDeadline>[0],
) =>
  Effect.scoped(
    Effect.gen(function* () {
      yield* WorkflowRuntime;
      const recovery = yield* WorkflowRunRecovery;
      yield* recovery.retire;
      const retirement = yield* retirementDeadline(workflowRunId);
      return { counts: yield* storageCounts, retireAfter: retirement.retireAfter };
    }).pipe(Effect.provide(leasedRuntime(filename, executions, "1 milli"))),
  );

const recoverAfterExpiry = (filename: string, executions: Ref.Ref<number>) =>
  Effect.scoped(
    Effect.gen(function* () {
      yield* WorkflowRuntime;
      const recovery = yield* WorkflowRunRecovery;
      yield* recovery.retire;
      yield* recovery.retire;
      return yield* storageCounts;
    }).pipe(Effect.provide(leasedRuntime(filename, executions, "1 hour"))),
  );

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
        yield* claimWorkflowChildAgent(
          { sessionId: acceptedRequest.sessionId, workflowRunId },
          WorkflowActivityKey.make("retention/child"),
          "Review the release.",
        );
        yield* runtime.wait({ sessionId: acceptedRequest.sessionId, workflowRunId });
        expect(yield* storageCounts).toHaveProperty("activeAgents", 1);
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
      }).pipe(Effect.provide(leasedRuntime(filename, executions, "500 millis"))),
    );

    expect(counts).toEqual(emptyWorkflowStorage);
  }),
);

scopedLive("mints a new Workflow Run ID after terminal retirement", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-workflow-incarnation-" });
    const filename = `${directory}/loom.sqlite`;
    const executions = yield* Ref.make(0);

    const [first, second] = yield* Effect.scoped(
      Effect.gen(function* () {
        const runtime = yield* WorkflowRuntime;
        const firstId = yield* runtime.send(request);
        yield* runtime.wait({ sessionId: request.sessionId, workflowRunId: firstId });
        yield* storageCounts.pipe(
          Effect.repeat({
            while: ({ acceptance }) => acceptance > 0,
            schedule: Schedule.spaced("10 millis"),
          }),
        );
        const secondId = yield* runtime.send(request);
        yield* runtime.wait({ sessionId: request.sessionId, workflowRunId: secondId });
        return [firstId, secondId];
      }).pipe(Effect.provide(leasedRuntime(filename, executions, "100 millis"))),
    );

    expect(second).not.toBe(first);
    expect(yield* Ref.get(executions)).toBe(2);
  }),
);

scopedLive("keeps one terminal retirement deadline across restarts", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-workflow-startup-" });
    const filename = `${directory}/loom.sqlite`;
    const executions = yield* Ref.make(0);

    const firstDeadline = yield* completeWithDeadline(filename, executions);

    const beforeExpiry = yield* recoverBeforeExpiry(
      filename,
      executions,
      firstDeadline.workflowRunId,
    );
    expect(beforeExpiry.counts.acceptance).toBe(1);
    expect(Option.map(beforeExpiry.retireAfter, DateTime.toEpochMillis)).toEqual(
      Option.some(DateTime.toEpochMillis(firstDeadline.retireAfter)),
    );

    yield* expireRetirement(firstDeadline.workflowRunId).pipe(
      Effect.provide(leasedRuntime(filename, executions, "1 hour")),
      Effect.scoped,
    );

    const afterExpiry = yield* recoverAfterExpiry(filename, executions);

    expect(afterExpiry).toEqual(emptyWorkflowStorage);
  }),
);
