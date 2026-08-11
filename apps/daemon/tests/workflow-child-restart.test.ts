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
import { workflowInterpreterVersion } from "@cvr/loom-protocol";
import {
  JobStore,
  ProcessObservation,
  WorkflowAgentResult,
  WorkflowChildAgentStore,
  WorkflowJobHandle,
} from "@cvr/loom-runtime";
import { expect } from "effect-bun-test";
import { Array as Arr, Effect, Fiber, FileSystem, Layer, Option, Schedule, Schema } from "effect";
import type { DaemonConfig } from "../src/daemon-config.js";
import { runLoomDaemon } from "../src/program.js";
import { scopedLive, waitForSuspension, withClient } from "./workflow-test-support.js";

const resultSchema = Schema.Struct({
  agent: WorkflowAgentResult,
  job: WorkflowJobHandle,
  signal: Schema.String,
});

const workflowAgentFixture = new URL(
  "../../../packages/platform-bun/tests/fixtures/workflow-agent.ts",
  import.meta.url,
).pathname;

const ownershipRequest = (command: string, agentReleasePath: string) =>
  WorkflowRunRequest.make({
    sessionId: SessionId.make("workflow-ownership-restart"),
    key: WorkflowKey.make("workflow-ownership-restart"),
    definition: WorkflowDefinition.make({
      name: WorkflowName.make("workflow-ownership-restart"),
      version: WorkflowVersion.make("1"),
      interpreterVersion: workflowInterpreterVersion,
      source: `
        const agent = await step.run({
          stepId: "agent", capability: "agent", input: { prompt: input.agentPrompt },
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
    input: { command, agentPrompt: `wait-for:${agentReleasePath}` },
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
  layerWorkflowCapabilities({
    workspaceRoot,
    executable: "bun",
    arguments: ["run", workflowAgentFixture],
    maximumOutputBytes: 64 * 1_024,
  }).pipe(Layer.provide(layerSqliteWorkflowChildAgentStore));

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
    const agent = yield* requireHead(yield* agents.listActiveBySession(sessionId), "child Agent");
    const agentJob = yield* requireHead(
      running.filter((job) => job.jobId === agent.jobId),
      "Agent Job process",
    );
    return { agent, agentJob };
  }).pipe(Effect.provide(ownershipStorage(filename)));

const readJob = (filename: string, agentJobId: string) =>
  readActiveJobs(filename).pipe(
    Effect.map((jobs) =>
      jobs.filter((job) => job.jobId !== agentJobId && job.status === "Running"),
    ),
    Effect.repeat({ while: (jobs) => jobs.length === 0, schedule: Schedule.spaced("10 millis") }),
    Effect.flatMap((jobs) => requireHead(jobs, "Job process")),
    Effect.timeout("5 seconds"),
  );

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

const makeRestartScenario = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-child-restart-" });
  const agentReleasePath = `${directory}/release-agent`;
  const jobReleasePath = `${directory}/release-job`;
  const config = {
    workspaceRoot: WorkspaceRoot.make(directory),
    socketPath: `${directory}/daemon.sock`,
    databasePath: `${directory}/loom.sqlite`,
  };
  const request = ownershipRequest(
    `while [ ! -f '${jobReleasePath}' ]; do sleep 0.05; done`,
    agentReleasePath,
  );
  const releaseAgent = fs.writeFileString(agentReleasePath, "release");
  const releaseJob = fs.writeFileString(jobReleasePath, "release");
  yield* Effect.addFinalizer(() =>
    Effect.all([releaseAgent, releaseJob], { discard: true }).pipe(Effect.orDie),
  );
  return { config, releaseAgent, releaseJob, request };
});

const reconcileRestart = Effect.fn("WorkflowChildRestart.reconcile")(function* (
  config: DaemonConfig,
  request: WorkflowRunRequest,
  releaseAgent: Effect.Effect<void, unknown>,
  releaseJob: Effect.Effect<void, unknown>,
) {
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
  if (after.agentJob.status !== "Running") return yield* Effect.die("Agent Job is not running.");
  if (before.agentJob.status !== "Running") {
    return yield* Effect.die("Agent Job was not running.");
  }
  expect(after.agentJob.identity).toEqual(before.agentJob.identity);

  yield* releaseAgent;
  const job = yield* readJob(config.databasePath, after.agentJob.jobId);
  if (job.status !== "Running") return yield* Effect.die("Job is not running.");
  yield* releaseJob;
  expect(yield* waitForSuspension(config.workspaceRoot, config.socketPath, address)).toHaveProperty(
    "_tag",
    "Suspended",
  );
  const result = yield* completeWorkflow(config, request, address);
  expect(result.agent.agentId).toBe(after.agent.agentId);
  expect(result.agent.outcome).toEqual({ _tag: "Succeeded", exitCode: 0 });
  expect(result.agent.stdout).toContain("agent-complete:wait-for:");
  expect(result.job.jobId).toBe(job.jobId);
  const processExits = yield* Effect.all([
    waitForProcessExit(after.agentJob.identity.pid),
    waitForProcessExit(job.identity.pid),
  ]);
  expect(processExits[0]).toHaveProperty("_tag", "Missing");
  expect(processExits[1]).toHaveProperty("_tag", "Missing");
  yield* withClient(config.workspaceRoot, config.socketPath, (client) =>
    client.closeSession(request.sessionId),
  );
  expect(yield* readActiveAgents(config.databasePath, request.sessionId)).toEqual([]);
  expect(yield* waitForNoActiveJobs(config.databasePath)).toEqual([]);
  yield* Fiber.interrupt(secondDaemon);
});

scopedLive(
  "reconciles Workflow child ownership through a full daemon restart",
  () =>
    makeRestartScenario.pipe(
      Effect.flatMap(({ config, releaseAgent, releaseJob, request }) =>
        reconcileRestart(config, request, releaseAgent, releaseJob),
      ),
    ),
  15_000,
);
