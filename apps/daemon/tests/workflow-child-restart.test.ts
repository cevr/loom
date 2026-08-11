import {
  SessionId,
  WorkflowBudget,
  WorkflowCapability,
  WorkflowDefinition,
  WorkflowKey,
  WorkflowName,
  type WorkflowRunAddress,
  WorkflowRunRequest,
  WorkflowSignalName,
  WorkflowVersion,
  WorkspaceRoot,
} from "@cvr/loom-domain";
import {
  layerLoomSqlite,
  layerSqliteJobStore,
  layerSqliteWorkflowChildAgentStore,
  layerWorkflowCapabilities,
  makeBunProcessInspector,
} from "@cvr/loom-platform-bun";
import {
  JobStore,
  ProcessObservation,
  WorkflowAgentHandle,
  WorkflowChildAgentStore,
  WorkflowJobHandle,
} from "@cvr/loom-runtime";
import { expect } from "effect-bun-test";
import { Array as Arr, Effect, Fiber, FileSystem, Layer, Option, Schedule, Schema } from "effect";
import type { DaemonConfig } from "../src/daemon-config.js";
import { runLoomDaemon } from "../src/program.js";
import { scopedLive, waitForSuspension, withClient } from "./workflow-test-support.js";

const resultSchema = Schema.Struct({
  agent: WorkflowAgentHandle,
  job: WorkflowJobHandle,
  signal: Schema.String,
});

const ownershipRequest = (command: string) =>
  WorkflowRunRequest.make({
    sessionId: SessionId.make("workflow-ownership-restart"),
    key: WorkflowKey.make("workflow-ownership-restart"),
    definition: WorkflowDefinition.make({
      name: WorkflowName.make("workflow-ownership-restart"),
      version: WorkflowVersion.make("1"),
      interpreterVersion: 1,
      source: `
        const agent = await step.run({
          stepId: "agent", capability: "agent", input: { prompt: "Inspect the build." },
        })
        const job = await step.run({
          stepId: "job", capability: "job", input: { command: input.command },
        })
        const received = await signal.wait("continue")
        return { agent, job, signal: received }
      `,
      capabilities: [WorkflowCapability.make("agent"), WorkflowCapability.make("job")],
      signals: [WorkflowSignalName.make("continue")],
    }),
    input: { command },
    budget: WorkflowBudget.make({
      maxSteps: 2,
      maxAgentRuns: 1,
      maxParallelism: 1,
      maxInlineStepResultBytes: 1_024,
      maxTokens: Option.none(),
      maxDurationMillis: Option.none(),
    }),
  });

const ownershipCapabilities = (workspaceRoot: WorkspaceRoot) =>
  layerWorkflowCapabilities({ workspaceRoot }).pipe(
    Layer.provide(layerSqliteWorkflowChildAgentStore),
  );

const ownershipStorage = (filename: string) => {
  const database = layerLoomSqlite({ filename });
  return Layer.mergeAll(
    database,
    layerSqliteWorkflowChildAgentStore.pipe(Layer.provide(database)),
    layerSqliteJobStore.pipe(Layer.provide(database)),
  );
};

const requireHead = <A>(values: ReadonlyArray<A>, label: string) =>
  Option.match(Arr.head(values), {
    onNone: () => Effect.die(`Missing ${label}.`),
    onSome: Effect.succeed,
  });

const readOwnership = (filename: string, sessionId: SessionId) =>
  Effect.gen(function* () {
    const agents = yield* WorkflowChildAgentStore;
    const jobs = yield* JobStore;
    const running = yield* jobs.listRecoverable.pipe(
      Effect.repeat({
        while: (records) => records.length === 0,
        schedule: Schedule.spaced("10 millis"),
      }),
      Effect.timeout("5 seconds"),
    );
    const job = yield* requireHead(running, "Job process");
    const agent = yield* requireHead(yield* agents.listActiveBySession(sessionId), "child Agent");
    return { agent, job };
  }).pipe(Effect.provide(ownershipStorage(filename)));

const readActiveAgents = (filename: string, sessionId: SessionId) =>
  WorkflowChildAgentStore.pipe(
    Effect.flatMap((agents) => agents.listActiveBySession(sessionId)),
    Effect.provide(ownershipStorage(filename)),
  );

const readActiveJobs = (filename: string) =>
  JobStore.pipe(
    Effect.flatMap((jobs) => Effect.all([jobs.listUncommitted, jobs.listRecoverable])),
    Effect.map(([uncommitted, recoverable]) => [...uncommitted, ...recoverable]),
    Effect.provide(ownershipStorage(filename)),
  );

const waitForNoActiveJobs = (filename: string) =>
  readActiveJobs(filename).pipe(
    Effect.repeat({ while: (jobs) => jobs.length > 0, schedule: Schedule.spaced("10 millis") }),
    Effect.timeout("5 seconds"),
  );

const startDaemon = (config: DaemonConfig) =>
  runLoomDaemon(config, ownershipCapabilities(config.workspaceRoot)).pipe(Effect.forkScoped);

const completeWorkflow = (
  config: DaemonConfig,
  request: WorkflowRunRequest,
  address: WorkflowRunAddress,
) =>
  withClient(config.workspaceRoot, config.socketPath, (client) =>
    client
      .signalWorkflow({
        address: { ...address, name: WorkflowSignalName.make("continue") },
        value: "continued",
      })
      .pipe(Effect.andThen(client.executeWorkflow(request))),
  ).pipe(Effect.flatMap(Schema.decodeUnknownEffect(resultSchema)));

const waitForProcessExit = (pid: number) =>
  makeBunProcessInspector.pipe(
    Effect.flatMap((inspector) =>
      inspector.inspect(pid).pipe(
        Effect.repeat({
          while: ProcessObservation.$is("Found"),
          schedule: Schedule.spaced("10 millis"),
        }),
      ),
    ),
    Effect.timeout("5 seconds"),
  );

scopedLive("reconciles Workflow child ownership through a full daemon restart", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-child-restart-" });
    const releasePath = `${directory}/release-job`;
    const config = {
      workspaceRoot: WorkspaceRoot.make(directory),
      socketPath: `${directory}/daemon.sock`,
      databasePath: `${directory}/loom.sqlite`,
    };
    const request = ownershipRequest(`while [ ! -f '${releasePath}' ]; do sleep 0.05; done`);
    yield* Effect.addFinalizer(() => fs.writeFileString(releasePath, "release").pipe(Effect.orDie));

    const firstDaemon = yield* startDaemon(config);
    const handle = yield* withClient(config.workspaceRoot, config.socketPath, (client) =>
      client.startWorkflow(request),
    );
    const address = { sessionId: request.sessionId, ...handle };
    const before = yield* readOwnership(config.databasePath, request.sessionId);
    yield* Fiber.interrupt(firstDaemon);

    const secondDaemon = yield* startDaemon(config);
    yield* withClient(config.workspaceRoot, config.socketPath, (client) => client.handshake);
    const after = yield* readOwnership(config.databasePath, request.sessionId);
    expect(after.agent).toEqual(before.agent);
    if (after.job.status !== "Running") return yield* Effect.die("Job is not running.");
    if (before.job.status !== "Running") return yield* Effect.die("Job was not running.");
    expect(after.job.identity).toEqual(before.job.identity);
    expect(after.job.status).toBe("Running");

    yield* fs.writeFileString(releasePath, "release");
    expect(
      yield* waitForSuspension(config.workspaceRoot, config.socketPath, address),
    ).toHaveProperty("_tag", "Suspended");
    const result = yield* completeWorkflow(config, request, address);
    expect(result.agent.agentId).toBe(after.agent.agentId);
    expect(result.job.jobId).toBe(after.job.jobId);
    yield* withClient(config.workspaceRoot, config.socketPath, (client) =>
      client.closeSession(request.sessionId),
    );
    expect(yield* readActiveAgents(config.databasePath, request.sessionId)).toEqual([]);
    const identity = after.job.identity;
    expect(yield* waitForProcessExit(identity.pid)).toHaveProperty("_tag", "Missing");
    expect(yield* waitForNoActiveJobs(config.databasePath)).toEqual([]);
    yield* Fiber.interrupt(secondDaemon);
  }),
);
