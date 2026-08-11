import { BunServices } from "@effect/platform-bun";
import {
  SessionId,
  WorkflowActivityKey,
  WorkflowRunAddress,
  WorkflowRunId,
} from "@cvr/loom-domain";
import { WorkflowActivityContext, WorkflowChildAgentStore } from "@cvr/loom-runtime";
import { expect, it } from "effect-bun-test";
import { Effect, FileSystem, Layer } from "effect";
import { layerLoomSqlite, layerSqliteWorkflowChildAgentStore } from "../src/index.js";

const context = WorkflowActivityContext.make({
  activityKey: WorkflowActivityKey.make("workflow/step"),
  sessionId: SessionId.make("session-1"),
  workflowRunId: WorkflowRunId.make("workflow-1"),
});

const layerStores = (filename: string) => {
  const database = layerLoomSqlite({ filename });
  return Layer.mergeAll(database, layerSqliteWorkflowChildAgentStore.pipe(Layer.provide(database)));
};

it.scopedLive.layer(BunServices.layer)("deduplicates Agent claims by Activity key", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-capability-store-" });

    yield* Effect.gen(function* () {
      const agents = yield* WorkflowChildAgentStore;
      const claimedAgents = yield* Effect.all(
        [agents.claim(context, "Check the build."), agents.claim(context, "Check the build.")],
        { concurrency: "unbounded" },
      );

      expect(claimedAgents[0]).toEqual(claimedAgents[1]);
      expect(claimedAgents[0].parent).toEqual(
        WorkflowRunAddress.make({
          sessionId: context.sessionId,
          workflowRunId: context.workflowRunId,
        }),
      );
      yield* agents.stop(context.activityKey);
      yield* agents.stop(context.activityKey);
      expect(yield* agents.listActiveBySession(context.sessionId)).toEqual([]);
    }).pipe(Effect.provide(layerStores(`${directory}/loom.sqlite`)));
  }),
);
