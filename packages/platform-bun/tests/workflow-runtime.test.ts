import { WorkflowRunId, WorkflowSignalAddress, WorkflowSignalName } from "@cvr/loom-domain";
import {
  LoomDynamicWorkflow,
  ActorStateHub,
  type ActorStateHubShape,
  WorkflowRuntime,
} from "@cvr/loom-runtime";
import { WorkflowRunState } from "@cvr/loom-protocol";
import { expect } from "effect-bun-test";
import { Effect, FileSystem, Layer, Ref, Schedule, Stream } from "effect";
import { ClusterWorkflowEngine } from "effect/unstable/cluster";
import { layerLoomDynamicWorkflow, layerLoomWorkflowRuntimeWith } from "../src/index.js";
import {
  durationRequest,
  execution,
  failureRequest,
  request,
  signalRequest,
} from "./workflow-runtime-fixtures.js";
import {
  runtimeLayer,
  scopedLive,
  storageCounts,
  workflowSupport,
} from "./workflow-runtime-test-support.js";

const workflowLayer = (filename: string, executions: Ref.Ref<number>) => {
  const support = workflowSupport(filename, executions);
  const engine = ClusterWorkflowEngine.layer.pipe(Layer.provide(support));
  const workflow = layerLoomDynamicWorkflow.pipe(Layer.provide([engine, support]));
  return Layer.merge(workflow, engine);
};

const execute = (filename: string, executions: Ref.Ref<number>, acceptedRequest = request) =>
  Effect.scoped(
    LoomDynamicWorkflow.execute(execution(acceptedRequest)).pipe(
      Effect.provide(workflowLayer(filename, executions)),
    ),
  );

const waitForActorCount = (actors: ActorStateHubShape, count: number) =>
  actors.snapshot.pipe(
    Effect.repeat({
      while: (snapshot) => snapshot.size !== count,
      schedule: Schedule.spaced("10 millis"),
    }),
  );

scopedLive("shares one Workflow Run between matching callers", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-workflow-shared-" });
    const executions = yield* Ref.make(0);

    const results = yield* Effect.scoped(
      Effect.all(
        [
          LoomDynamicWorkflow.execute(execution(request)),
          LoomDynamicWorkflow.execute(execution(request)),
        ],
        { concurrency: "unbounded" },
      ).pipe(Effect.provide(workflowLayer(`${directory}/loom.sqlite`, executions))),
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

scopedLive("persists a Workflow signal across cluster restarts", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-workflow-signal-" });
    const filename = `${directory}/loom.sqlite`;
    const executions = yield* Ref.make(0);

    const executionId = yield* Effect.scoped(
      WorkflowRuntime.pipe(
        Effect.flatMap((runtime) => runtime.send(signalRequest)),
        Effect.provide(runtimeLayer(filename, executions)),
      ),
    );
    const address = WorkflowSignalAddress.make({
      sessionId: signalRequest.sessionId,
      workflowRunId: executionId,
      name: WorkflowSignalName.make("approval"),
    });

    yield* Effect.scoped(
      Effect.gen(function* () {
        const runtime = yield* WorkflowRuntime;
        const actors = yield* ActorStateHub;
        expect((yield* waitForActorCount(actors, 1)).size).toBe(1);
        yield* runtime.signal({ address, value: { approved: true } });
      }).pipe(Effect.provide(runtimeLayer(filename, executions))),
    );
    const result = yield* Effect.scoped(
      WorkflowRuntime.pipe(
        Effect.flatMap((runtime) => runtime.execute(signalRequest)),
        Effect.provide(runtimeLayer(filename, executions)),
      ),
    );

    expect(result).toEqual({ approved: true });
  }),
);

scopedLive("rejects undeclared public Workflow signals", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-workflow-signal-denied-" });
    const executions = yield* Ref.make(0);
    const layer = runtimeLayer(`${directory}/loom.sqlite`, executions);

    yield* Effect.gen(function* () {
      const runtime = yield* WorkflowRuntime;
      const workflowRunId = yield* runtime.send(signalRequest);
      const undeclared = yield* runtime
        .signal({
          address: WorkflowSignalAddress.make({
            sessionId: signalRequest.sessionId,
            workflowRunId,
            name: WorkflowSignalName.make("cancel"),
          }),
          value: true,
        })
        .pipe(Effect.flip);
      const unknownRun = yield* runtime
        .signal({
          address: WorkflowSignalAddress.make({
            sessionId: signalRequest.sessionId,
            workflowRunId: WorkflowRunId.make("unknown-run"),
            name: WorkflowSignalName.make("approval"),
          }),
          value: true,
        })
        .pipe(Effect.flip);

      expect(undeclared).toHaveProperty("_tag", "WorkflowSignalNotDeclaredError");
      expect(undeclared).toHaveProperty("address.workflowRunId", workflowRunId);
      expect(undeclared).toHaveProperty("address.name", WorkflowSignalName.make("cancel"));
      expect(unknownRun).toHaveProperty("_tag", "WorkflowRunNotFoundError");
      expect(unknownRun).toHaveProperty("address.workflowRunId", WorkflowRunId.make("unknown-run"));
      expect(unknownRun).toHaveProperty("address.sessionId", signalRequest.sessionId);
    }).pipe(Effect.provide(layer), Effect.scoped);
  }),
);

scopedLive("does not accept a Workflow Run through a read", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-workflow-read-" });
    const executions = yield* Ref.make(0);

    yield* Effect.gen(function* () {
      const runtime = yield* WorkflowRuntime;
      const address = {
        sessionId: request.sessionId,
        workflowRunId: WorkflowRunId.make("unknown-run"),
      };
      const waited = yield* runtime.wait(address).pipe(Effect.flip);
      const watched = yield* runtime.watch(address).pipe(Stream.runCollect, Effect.flip);

      expect(waited).toHaveProperty("_tag", "WorkflowRunNotFoundError");
      expect(watched).toHaveProperty("_tag", "WorkflowRunNotFoundError");
      expect((yield* storageCounts).acceptance).toBe(0);
    }).pipe(Effect.provide(runtimeLayer(`${directory}/loom.sqlite`, executions)), Effect.scoped);
  }),
);

scopedLive("clears a failed Workflow Run after its state lease", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-workflow-failure-" });
    const executions = yield* Ref.make(0);
    const runtime = layerLoomWorkflowRuntimeWith({ stateLease: "25 millis" });

    yield* Effect.gen(function* () {
      const workflows = yield* WorkflowRuntime;
      const actors = yield* ActorStateHub;
      const workflowRunId = yield* workflows.send(failureRequest);
      const address = { sessionId: failureRequest.sessionId, workflowRunId };
      const state = yield* workflows.inspect(address).pipe(
        Effect.repeat({
          while: (current) => !WorkflowRunState.guards.Failure(current),
          schedule: Schedule.spaced("10 millis"),
        }),
      );

      expect(WorkflowRunState.guards.Failure(state)).toBe(true);
      expect((yield* waitForActorCount(actors, 0)).size).toBe(0);
    }).pipe(
      Effect.provide(runtimeLayer(`${directory}/loom.sqlite`, executions, runtime)),
      Effect.scoped,
    );
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
      const actors = yield* ActorStateHub;
      const executionId = yield* runtime.send(request);
      const address = { sessionId: request.sessionId, workflowRunId: executionId };
      const states = yield* runtime.watch(address).pipe(Stream.runCollect);
      const terminal = yield* runtime.wait(address);

      expect(states.at(-1)).toEqual(terminal);
      expect(terminal).toHaveProperty("_tag", "Success");
      expect(yield* runtime.inspect(address)).toHaveProperty("_tag", "Success");
      yield* runtime.interrupt(address);
      expect(yield* runtime.execute(request)).toEqual(request.input);
      const inflightId = yield* runtime.send(signalRequest);
      const inflight = { sessionId: signalRequest.sessionId, workflowRunId: inflightId };
      const working = yield* waitForActorCount(actors, 1);
      expect(working.size).toBe(1);
      yield* runtime.interrupt(inflight);
      const interrupted = yield* runtime.inspect(inflight).pipe(
        Effect.repeat({
          while: (state) => !WorkflowRunState.guards.Interrupted(state),
          schedule: Schedule.spaced("10 millis"),
        }),
      );
      expect(WorkflowRunState.guards.Interrupted(interrupted)).toBe(true);
      const stopped = yield* waitForActorCount(actors, 0);
      expect(stopped.size).toBe(0);
    }).pipe(Effect.provide(layer), Effect.scoped);

    expect(yield* Ref.get(executions)).toBe(1);
  }),
);
