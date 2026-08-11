import { BunServices } from "@effect/platform-bun";
import {
  SessionId,
  WorkflowBudget,
  WorkflowCapability,
  WorkflowDefinition,
  WorkflowKey,
  WorkflowName,
  WorkflowRunRequest,
  WorkflowSignalName,
  WorkflowVersion,
} from "@cvr/loom-domain";
import { WorkflowIdentityConflictError } from "@cvr/loom-protocol";
import { WorkflowRunAcceptance, layerWorkflowRunAcceptance } from "@cvr/loom-runtime";
import { expect, it } from "effect-bun-test";
import { Effect, FileSystem, Layer, Option, Schema } from "effect";
import { SqlClient, SqlSchema } from "effect/unstable/sql";
import { layerLoomSqlite, layerSqliteWorkflowRunAcceptanceStore } from "../src/index.js";

const definition = WorkflowDefinition.make({
  name: WorkflowName.make("release"),
  version: WorkflowVersion.make("1"),
  interpreterVersion: 1,
  source: "return await step.run('publish', input)",
  capabilities: [WorkflowCapability.make("job"), WorkflowCapability.make("artifact")],
  signals: [WorkflowSignalName.make("approval")],
});

const budget = WorkflowBudget.make({
  maxSteps: 10,
  maxAgentRuns: 2,
  maxParallelism: 3,
  maxInlineStepResultBytes: 4096,
  maxTokens: Option.some(10_000),
  maxDurationMillis: Option.some(60_000),
});

const request = WorkflowRunRequest.make({
  sessionId: SessionId.make("session-1"),
  key: WorkflowKey.make("deploy"),
  definition,
  input: { target: "production", metadata: { region: "north", replicas: 2 } },
  budget,
});
const scopedLive = it.scopedLive.layer(BunServices.layer);

const conflictingRequests = [
  WorkflowRunRequest.make({
    ...request,
    definition: WorkflowDefinition.make({ ...definition, source: "return 'changed'" }),
  }),
  WorkflowRunRequest.make({ ...request, input: { target: "staging" } }),
  WorkflowRunRequest.make({
    ...request,
    definition: WorkflowDefinition.make({
      ...definition,
      capabilities: [...definition.capabilities, WorkflowCapability.make("agent")],
    }),
  }),
  WorkflowRunRequest.make({
    ...request,
    definition: WorkflowDefinition.make({
      ...definition,
      signals: [...definition.signals, WorkflowSignalName.make("cancel")],
    }),
  }),
  WorkflowRunRequest.make({
    ...request,
    budget: WorkflowBudget.make({ ...budget, maxSteps: 11 }),
  }),
  WorkflowRunRequest.make({
    ...request,
    definition: WorkflowDefinition.make({ ...definition, interpreterVersion: 2 }),
  }),
];

const acceptanceLayer = (filename: string) =>
  layerWorkflowRunAcceptance.pipe(
    Layer.provide(
      layerSqliteWorkflowRunAcceptanceStore.pipe(Layer.provide(layerLoomSqlite({ filename }))),
    ),
  );

const withAcceptance = <A, E>(
  filename: string,
  effect: Effect.Effect<A, E, WorkflowRunAcceptance>,
) => Effect.scoped(effect.pipe(Effect.provide(acceptanceLayer(filename))));

const countAcceptanceRows = (filename: string) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const count = SqlSchema.findOne({
      Request: Schema.Void,
      Result: Schema.Struct({ count: Schema.Natural }),
      execute: () => sql`SELECT COUNT(*) AS count FROM workflow_run_acceptance`,
    });
    return (yield* count()).count;
  }).pipe(Effect.provide(layerLoomSqlite({ filename })), Effect.scoped);

scopedLive("creates one acceptance row for concurrent matching requests", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-workflow-acceptance-" });
    const filename = `${directory}/loom.sqlite`;

    const accepted = yield* withAcceptance(
      filename,
      Effect.gen(function* () {
        const acceptance = yield* WorkflowRunAcceptance;
        return yield* Effect.all(
          Array.from({ length: 16 }, () => acceptance.accept(request)),
          { concurrency: "unbounded" },
        );
      }),
    );

    expect(new Set(accepted.map(({ digest }) => digest)).size).toBe(1);
    expect(new Set(accepted.map(({ workflowRunId }) => workflowRunId)).size).toBe(1);
    expect(yield* countAcceptanceRows(filename)).toBe(1);
  }),
);

scopedLive("normalizes names and JSON object keys before it attaches", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-workflow-normalize-" });
    const filename = `${directory}/loom.sqlite`;
    const reordered = WorkflowRunRequest.make({
      ...request,
      definition: WorkflowDefinition.make({
        ...definition,
        capabilities: [
          WorkflowCapability.make("artifact"),
          WorkflowCapability.make("job"),
          WorkflowCapability.make("artifact"),
        ],
        signals: [WorkflowSignalName.make("approval"), WorkflowSignalName.make("approval")],
      }),
      input: { metadata: { replicas: 2, region: "north" }, target: "production" },
    });

    const { first, attached } = yield* withAcceptance(
      filename,
      Effect.gen(function* () {
        const acceptance = yield* WorkflowRunAcceptance;
        const initial = yield* acceptance.accept(request);
        const retry = yield* acceptance.accept(reordered);
        return { first: initial, attached: retry };
      }),
    );

    expect(attached).toEqual(first);
    expect(first.request.definition.capabilities).toEqual([
      WorkflowCapability.make("artifact"),
      WorkflowCapability.make("job"),
    ]);
    expect(first.request.definition.signals).toEqual([WorkflowSignalName.make("approval")]);
  }),
);

scopedLive("keeps the accepted request immutable across daemon storage restarts", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-workflow-restart-" });
    const filename = `${directory}/loom.sqlite`;

    const accepted = yield* withAcceptance(
      filename,
      Effect.gen(function* () {
        const acceptance = yield* WorkflowRunAcceptance;
        return yield* acceptance.accept(request);
      }),
    );

    const attached = yield* withAcceptance(
      filename,
      Effect.gen(function* () {
        const acceptance = yield* WorkflowRunAcceptance;
        return yield* acceptance.accept(request);
      }),
    );
    expect(attached).toEqual(accepted);

    yield* withAcceptance(
      filename,
      Effect.gen(function* () {
        const acceptance = yield* WorkflowRunAcceptance;
        yield* Effect.forEach(conflictingRequests, (changed) =>
          Effect.gen(function* () {
            const conflict = yield* acceptance.accept(changed).pipe(Effect.flip);
            expect(conflict).toBeInstanceOf(WorkflowIdentityConflictError);
            if (conflict instanceof WorkflowIdentityConflictError) {
              expect(conflict.acceptedDigest).toBe(accepted.digest);
              expect(conflict.receivedDigest).not.toBe(accepted.digest);
            }
          }),
        );
        expect(yield* acceptance.accept(request)).toEqual(accepted);
      }),
    );
  }),
);

scopedLive("keeps the default Workflow Budget digest stable across storage restarts", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-workflow-budget-" });
    const filename = `${directory}/loom.sqlite`;
    const defaultedRequest = WorkflowRunRequest.make({
      ...request,
      key: WorkflowKey.make("default-budget"),
      budget: WorkflowBudget.make({}),
    });

    const accepted = yield* withAcceptance(
      filename,
      WorkflowRunAcceptance.pipe(
        Effect.flatMap((acceptance) => acceptance.accept(defaultedRequest)),
      ),
    );
    const attached = yield* withAcceptance(
      filename,
      WorkflowRunAcceptance.pipe(
        Effect.flatMap((acceptance) => acceptance.accept(defaultedRequest)),
      ),
    );

    expect(attached.digest).toBe(accepted.digest);
    expect(attached.workflowRunId).toBe(accepted.workflowRunId);
  }),
);

scopedLive("resolves only the accepted Session after storage restarts", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-workflow-address-" });
    const filename = `${directory}/loom.sqlite`;
    const accepted = yield* withAcceptance(
      filename,
      WorkflowRunAcceptance.pipe(Effect.flatMap((acceptance) => acceptance.accept(request))),
    );
    const address = { sessionId: request.sessionId, workflowRunId: accepted.workflowRunId };

    yield* withAcceptance(
      filename,
      Effect.gen(function* () {
        const acceptance = yield* WorkflowRunAcceptance;
        yield* acceptance.authorize(address);
        expect(yield* acceptance.list).toEqual([address]);
        const denied = yield* acceptance
          .authorize({ ...address, sessionId: SessionId.make("session-2") })
          .pipe(Effect.flip);
        expect(denied).toHaveProperty("_tag", "WorkflowRunNotFoundError");
      }),
    );
  }),
);
