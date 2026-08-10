import { BunCrypto, BunServices } from "@effect/platform-bun";
import {
  ArtifactId,
  SessionId,
  WorkflowBudget,
  WorkflowCapability,
  WorkflowDefinition,
  WorkflowKey,
  WorkflowName,
  WorkflowRunRequest,
  WorkflowVersion,
} from "@cvr/loom-domain";
import {
  LoomDynamicWorkflow,
  WorkflowArtifactReference,
  WorkflowArtifactStore,
  WorkflowCapabilityExecutor,
  WorkflowRuntime,
  WorkflowStepExecution,
} from "@cvr/loom-runtime";
import { expect, it } from "effect-bun-test";
import { Effect, FileSystem, Layer, Option, Ref, Stream } from "effect";
import { ClusterWorkflowEngine, SingleRunner } from "effect/unstable/cluster";
import {
  layerLoomDynamicWorkflow,
  layerLoomSqlite,
  layerLoomWorkflowRuntime,
} from "../src/index.js";

const request = WorkflowRunRequest.make({
  sessionId: SessionId.make("session-1"),
  key: WorkflowKey.make("shared"),
  definition: WorkflowDefinition.make({
    name: WorkflowName.make("echo"),
    version: WorkflowVersion.make("1"),
    interpreterVersion: 1,
    source: `
      return await step.run({
        stepId: "echo",
        capability: "echo",
        input,
      })
    `,
    capabilities: [WorkflowCapability.make("echo")],
    signals: [],
  }),
  input: { value: 42 },
  budget: WorkflowBudget.make({
    maxSteps: 2,
    maxAgentRuns: 1,
    maxParallelism: 1,
    maxInlineStepResultBytes: 1_024,
    maxTokens: Option.some(1_000),
    maxDurationMillis: Option.none(),
  }),
});

const durationRequest = WorkflowRunRequest.make({
  ...request,
  key: WorkflowKey.make("duration"),
  definition: WorkflowDefinition.make({
    ...request.definition,
    source: "return await new Promise(() => {})",
  }),
  budget: WorkflowBudget.make({
    ...request.budget,
    maxDurationMillis: Option.some(25),
  }),
});

const workflowSupport = (filename: string, executions: Ref.Ref<number>) => {
  const foundation = Layer.merge(layerLoomSqlite({ filename }), BunCrypto.layer);
  const capabilities = Layer.succeed(
    WorkflowCapabilityExecutor,
    WorkflowCapabilityExecutor.of({
      supports: () => true,
      execute: (call) =>
        Ref.update(executions, (count) => count + 1).pipe(
          Effect.as(
            WorkflowStepExecution.make({
              value: call.input,
              tokenCount: 0,
              agentRuns: 0,
            }),
          ),
        ),
    }),
  );
  const artifacts = Layer.succeed(
    WorkflowArtifactStore,
    WorkflowArtifactStore.of({
      store: ({ stepId }) =>
        Effect.succeed(
          WorkflowArtifactReference.make({
            artifactId: ArtifactId.make(`artifact-${stepId}`),
          }),
        ),
    }),
  );
  return Layer.mergeAll(
    foundation,
    SingleRunner.layer({ runnerStorage: "memory" }).pipe(Layer.provide(foundation)),
    capabilities,
    artifacts,
  );
};

const workflowLayer = (filename: string, executions: Ref.Ref<number>) => {
  const support = workflowSupport(filename, executions);
  const engine = ClusterWorkflowEngine.layer.pipe(Layer.provide(support));
  const workflow = layerLoomDynamicWorkflow.pipe(Layer.provide([engine, support]));
  return Layer.merge(workflow, engine);
};

const runtimeLayer = (filename: string, executions: Ref.Ref<number>) =>
  layerLoomWorkflowRuntime.pipe(Layer.provide(workflowSupport(filename, executions)));

const execute = (filename: string, executions: Ref.Ref<number>, acceptedRequest = request) =>
  Effect.scoped(
    LoomDynamicWorkflow.execute(acceptedRequest).pipe(
      Effect.provide(workflowLayer(filename, executions)),
    ),
  );

const scopedLive = it.scopedLive.layer(BunServices.layer);

scopedLive("shares one Workflow Run between matching callers", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-workflow-shared-" });
    const executions = yield* Ref.make(0);

    const results = yield* Effect.scoped(
      Effect.all([LoomDynamicWorkflow.execute(request), LoomDynamicWorkflow.execute(request)], {
        concurrency: "unbounded",
      }).pipe(Effect.provide(workflowLayer(`${directory}/loom.sqlite`, executions))),
    );

    expect(results).toEqual([request.input, request.input]);
    expect(yield* Ref.get(executions)).toBe(1);
  }),
);

scopedLive("reuses a completed Step after the cluster restarts", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-workflow-restart-" });
    const filename = `${directory}/loom.sqlite`;
    const executions = yield* Ref.make(0);

    expect(yield* execute(filename, executions)).toEqual(request.input);
    expect(yield* execute(filename, executions)).toEqual(request.input);
    expect(yield* Ref.get(executions)).toBe(1);
  }),
);

scopedLive("persists the durable duration limit across cluster restarts", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-workflow-duration-" });
    const filename = `${directory}/loom.sqlite`;
    const executions = yield* Ref.make(0);

    const first = yield* execute(filename, executions, durationRequest).pipe(Effect.flip);
    const replay = yield* execute(filename, executions, durationRequest).pipe(Effect.flip);

    expect(first).toHaveProperty("budget", "Duration");
    expect(replay).toEqual(first);
  }),
);

scopedLive("exposes the Workflow Run lifecycle through one runtime service", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-workflow-runtime-" });
    const executions = yield* Ref.make(0);
    const layer = runtimeLayer(`${directory}/loom.sqlite`, executions);

    yield* Effect.gen(function* () {
      const runtime = yield* WorkflowRuntime;
      const executionId = yield* runtime.send(request);
      const states = yield* runtime.watch(request).pipe(Stream.runCollect);
      const terminal = yield* runtime.wait(request);

      expect(states.at(-1)).toEqual(terminal);
      expect(terminal).toHaveProperty("_tag", "Success");
      yield* runtime.interrupt(executionId);
      yield* runtime.resume(executionId);
      expect(yield* runtime.execute(request)).toEqual(request.input);
    }).pipe(Effect.provide(layer), Effect.scoped);

    expect(yield* Ref.get(executions)).toBe(1);
  }),
);
