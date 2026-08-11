import {
  JobAddress,
  JobId,
  JobRequest,
  JobRecord,
  JobSubmission,
  ProcessIdentity,
  SessionId,
  type WorkspaceRoot,
} from "@cvr/loom-domain";
import { JobRuntime, JobStore, layerActorStateHub } from "@cvr/loom-runtime";
import { Effect, Layer, Option, Schedule } from "effect";
import {
  layerBunJobRuntime,
  layerBunProcessController,
  layerBunProcessInspector,
  layerLoomSqlite,
  layerSqliteJobStore,
} from "../src/index.js";

export const sessionId = SessionId.make("recovery-session");
export const request = (jobId: string, command: string) =>
  JobRequest.make({ jobId: JobId.make(jobId), sessionId, command, attached: true });

export const storeLayer = (filename: string) =>
  layerSqliteJobStore.pipe(Layer.provide(layerLoomSqlite({ filename })));

export const runtimeLayer = (filename: string, workspaceRoot: WorkspaceRoot) =>
  layerBunJobRuntime({ workspaceRoot, terminationGrace: "50 millis" }).pipe(
    Layer.provide([
      layerActorStateHub,
      layerBunProcessController,
      layerBunProcessInspector,
      storeLayer(filename),
    ]),
  );

export const submission = (workspaceRoot: WorkspaceRoot, jobRequest: JobRequest) => {
  const directory = `${workspaceRoot}/.loom/jobs/${encodeURIComponent(jobRequest.jobId)}`;
  return JobSubmission.make({
    ...jobRequest,
    stdoutPath: `${directory}/stdout.log`,
    stderrPath: `${directory}/stderr.log`,
    resultPath: `${directory}/result`,
  });
};

export const inspectAfterReconcile = (
  filename: string,
  workspaceRoot: WorkspaceRoot,
  address: JobAddress,
) =>
  Effect.gen(function* () {
    const runtime = yield* JobRuntime;
    yield* runtime.reconcile;
    return yield* runtime.inspect(address);
  }).pipe(Effect.provide(runtimeLayer(filename, workspaceRoot)), Effect.scoped);

export const waitForTerminal = (runtime: JobRuntime["Service"], address: JobAddress) =>
  runtime.inspect(address).pipe(
    Effect.flatMap(
      Option.match({ onNone: () => Effect.die("Missing Job."), onSome: Effect.succeed }),
    ),
    Effect.repeat({
      until: JobRecord.isAnyOf(["Succeeded", "Failed", "Cancelled", "Lost"]),
      schedule: Schedule.spaced("10 millis"),
    }),
    Effect.timeout("5 seconds"),
  );

export const seedRunning = (filename: string, job: JobSubmission) =>
  Effect.gen(function* () {
    const jobs = yield* JobStore;
    yield* jobs.create(job);
    yield* jobs.begin(job.jobId);
    yield* jobs.activate(
      job.jobId,
      ProcessIdentity.make({
        pid: 2_147_483_647,
        processGroupId: 2_147_483_647,
        processStartId: "missing-process",
      }),
    );
  }).pipe(Effect.provide(storeLayer(filename)));

export const status = (job: Option.Option<JobRecord>) => Option.map(job, (record) => record.status);
