import { BunServices } from "@effect/platform-bun";
import { AgentParent, SessionId, WorkflowActivityKey, WorkflowRunId } from "@cvr/loom-domain";
import {
  WorkflowActivityContext,
  WorkflowChildAgentStore,
  WorkflowJobStore,
} from "@cvr/loom-runtime";
import { expect, it } from "effect-bun-test";
import { Effect, FileSystem, Layer } from "effect";
import {
  layerLoomSqlite,
  layerSqliteWorkflowChildAgentStore,
  layerSqliteWorkflowJobStore,
} from "../src/index.js";

const context = WorkflowActivityContext.make({
  activityKey: WorkflowActivityKey.make("workflow/step"),
  sessionId: SessionId.make("session-1"),
  workflowRunId: WorkflowRunId.make("workflow-1"),
});

const layerStores = (filename: string) => {
  const database = layerLoomSqlite({ filename });
  return Layer.mergeAll(
    database,
    layerSqliteWorkflowChildAgentStore.pipe(Layer.provide(database)),
    layerSqliteWorkflowJobStore.pipe(Layer.provide(database)),
  );
};

it.scopedLive.layer(BunServices.layer)("deduplicates Agent and Job claims by Activity key", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-capability-store-" });

    yield* Effect.gen(function* () {
      const agents = yield* WorkflowChildAgentStore;
      const jobs = yield* WorkflowJobStore;
      const claimedAgents = yield* Effect.all(
        [agents.claim(context, "Check the build."), agents.claim(context, "Check the build.")],
        { concurrency: "unbounded" },
      );
      const claimedJobs = yield* Effect.all([jobs.claim(context), jobs.claim(context)], {
        concurrency: "unbounded",
      });
      const launchRights = yield* Effect.all(
        [jobs.begin(context.activityKey), jobs.begin(context.activityKey)],
        { concurrency: "unbounded" },
      );

      expect(claimedAgents[0]).toEqual(claimedAgents[1]);
      expect(claimedAgents[0].parent).toEqual(
        AgentParent.cases.WorkflowRun.make({
          sessionId: context.sessionId,
          workflowRunId: context.workflowRunId,
        }),
      );
      expect(claimedJobs[0]).toEqual(claimedJobs[1]);
      expect(launchRights.filter(Boolean)).toHaveLength(1);

      yield* jobs.markFailed(context.activityKey);
      expect(yield* jobs.begin(context.activityKey)).toBe(true);

      yield* agents.stop(context.activityKey);
      yield* agents.stop(context.activityKey);
      expect(yield* agents.listActiveBySession(context.sessionId)).toEqual([]);
    }).pipe(Effect.provide(layerStores(`${directory}/loom.sqlite`)));
  }),
);
