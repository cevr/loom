import { BunServices } from "@effect/platform-bun";
import { JobAddress, JobId, JobRequest, SessionId, WorkspaceRoot } from "@cvr/loom-domain";
import { JobRuntime, ProcessObservation, layerActorStateHub } from "@cvr/loom-runtime";
import { expect, it } from "effect-bun-test";
import { Effect, FileSystem, Layer, Option, Schedule } from "effect";
import {
  layerBunJobRuntime,
  layerBunProcessController,
  layerBunProcessInspector,
  layerLoomSqlite,
  layerSqliteJobStore,
  makeBunProcessController,
  makeBunProcessInspector,
} from "../src/index.js";

const sessionId = SessionId.make("session-1");
const request = (jobId: string, command: string, attached = true) =>
  JobRequest.make({ jobId: JobId.make(jobId), sessionId, command, attached });

const runtimeLayer = (filename: string, workspaceRoot: WorkspaceRoot) => {
  const database = layerLoomSqlite({ filename });
  const store = layerSqliteJobStore.pipe(Layer.provide(database));
  return layerBunJobRuntime({ workspaceRoot, terminationGrace: "50 millis" }).pipe(
    Layer.provide([layerActorStateHub, layerBunProcessController, layerBunProcessInspector, store]),
  );
};

const waitForJob = (
  runtime: JobRuntime["Service"],
  address: JobAddress,
  predicate: (status: string) => boolean,
) =>
  runtime.inspect(address).pipe(
    Effect.flatMap(
      Option.match({
        onNone: () => Effect.die("The Job record is missing."),
        onSome: Effect.succeed,
      }),
    ),
    Effect.repeat({
      until: (job) => predicate(job.status),
      schedule: Schedule.spaced("10 millis"),
    }),
    Effect.timeout("5 seconds"),
  );

const terminal = (status: string) =>
  status === "Succeeded" || status === "Failed" || status === "Cancelled" || status === "Lost";

it.scopedLive.layer(BunServices.layer)("runs a Job and persists its result", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-job-runtime-" });
    const workspaceRoot = WorkspaceRoot.make(directory);
    const jobRequest = request("job-success", "printf 'complete\\n'");
    const address = JobAddress.make(jobRequest);

    yield* Effect.gen(function* () {
      const runtime = yield* JobRuntime;
      const accepted = yield* runtime.start(jobRequest);
      expect(accepted.status).toBe("Accepted");
      const completed = yield* waitForJob(runtime, address, terminal);
      expect(completed.status).toBe("Succeeded");
      expect(completed.exitCode).toEqual(Option.some(0));
      expect(yield* fs.readFileString(completed.stdoutPath)).toBe("complete\n");
      expect(yield* fs.readFileString(completed.resultPath)).toBe("0\n");
    }).pipe(Effect.provide(runtimeLayer(`${directory}/loom.sqlite`, workspaceRoot)));
  }),
);

it.scopedLive.layer(BunServices.layer)("returns control for a nonterminating pipeline", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-job-pipeline-" });
    const workspaceRoot = WorkspaceRoot.make(directory);
    const jobRequest = request(
      "job-pipeline",
      "while :; do printf 'tick\\n'; sleep 1; done | tail -n 1",
    );
    const address = JobAddress.make(jobRequest);

    yield* Effect.gen(function* () {
      const runtime = yield* JobRuntime;
      expect((yield* runtime.start(jobRequest).pipe(Effect.timeout("1 second"))).jobId).toBe(
        jobRequest.jobId,
      );
      expect((yield* waitForJob(runtime, address, (status) => status === "Running")).status).toBe(
        "Running",
      );
      const cancelled = yield* runtime.cancel(address);
      expect(Option.map(cancelled, (job) => job.status)).toEqual(Option.some("Cancelled"));
    }).pipe(Effect.provide(runtimeLayer(`${directory}/loom.sqlite`, workspaceRoot)));
  }),
);

it.scopedLive.layer(BunServices.layer)("keeps detached Jobs when a Session closes", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-job-session-" });
    const workspaceRoot = WorkspaceRoot.make(directory);
    const attached = request("job-attached", "sleep 30");
    const detached = request("job-detached", "sleep 30");

    yield* Effect.gen(function* () {
      const runtime = yield* JobRuntime;
      yield* runtime.start(attached);
      yield* runtime.start(detached);
      yield* waitForJob(runtime, JobAddress.make(attached), (status) => status === "Running");
      yield* waitForJob(runtime, JobAddress.make(detached), (status) => status === "Running");
      yield* runtime.detach(JobAddress.make(detached));
      yield* runtime.closeSession(sessionId);
      expect((yield* waitForJob(runtime, JobAddress.make(attached), terminal)).status).toBe(
        "Cancelled",
      );
      expect(
        (yield* waitForJob(runtime, JobAddress.make(detached), (status) => status === "Running"))
          .status,
      ).toBe("Running");
      yield* runtime.cancel(JobAddress.make(detached));
    }).pipe(Effect.provide(runtimeLayer(`${directory}/loom.sqlite`, workspaceRoot)));
  }),
);

it.scopedLive.layer(BunServices.layer)("recovers an exact Job identity after restart", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-job-restart-" });
    const workspaceRoot = WorkspaceRoot.make(directory);
    const filename = `${directory}/loom.sqlite`;
    const jobRequest = request("job-restart", "printf 'before-restart\\n'; sleep 30", false);
    const address = JobAddress.make(jobRequest);

    const before = yield* Effect.gen(function* () {
      const runtime = yield* JobRuntime;
      yield* runtime.start(jobRequest);
      return yield* waitForJob(runtime, address, (status) => status === "Running");
    }).pipe(Effect.provide(runtimeLayer(filename, workspaceRoot)), Effect.scoped);

    const after = yield* Effect.gen(function* () {
      const runtime = yield* JobRuntime;
      yield* runtime.reconcile;
      const recovered = yield* waitForJob(runtime, address, (status) => status === "Running");
      expect(recovered.identity).toEqual(before.identity);
      expect(yield* fs.readFileString(recovered.stdoutPath)).toBe("before-restart\n");
      yield* runtime.cancel(address);
      return yield* waitForJob(runtime, address, terminal);
    }).pipe(Effect.provide(runtimeLayer(filename, workspaceRoot)), Effect.scoped);

    expect(after.status).toBe("Cancelled");
  }),
);

it.scopedLive.layer(BunServices.layer)("escalates cancellation for a TERM-resistant Job", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-job-kill-" });
    const workspaceRoot = WorkspaceRoot.make(directory);
    const jobRequest = request("job-kill", "trap '' TERM; while :; do sleep 1; done");
    const address = JobAddress.make(jobRequest);

    yield* Effect.gen(function* () {
      const runtime = yield* JobRuntime;
      yield* runtime.start(jobRequest);
      const running = yield* waitForJob(runtime, address, (status) => status === "Running");
      const identity = Option.getOrThrow(running.identity);
      expect(Option.map(yield* runtime.cancel(address), (job) => job.status)).toEqual(
        Option.some("Cancelled"),
      );
      const inspector = yield* makeBunProcessInspector;
      const observation = yield* inspector.inspect(identity.pid).pipe(
        Effect.repeat({
          while: ProcessObservation.$is("Found"),
          schedule: Schedule.spaced("10 millis"),
        }),
        Effect.timeout("5 seconds"),
      );
      expect(ProcessObservation.$is("Missing")(observation)).toBe(true);
    }).pipe(Effect.provide(runtimeLayer(`${directory}/loom.sqlite`, workspaceRoot)));
  }),
);

it.scopedLive.layer(BunServices.layer)(
  "kills a TERM-resistant process after its leader exits",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-job-group-kill-" });
      const workspaceRoot = WorkspaceRoot.make(directory);
      const jobRequest = request(
        "job-group-kill",
        "sh -c 'trap \"\" TERM; while :; do sleep 1; done' & child=$!; trap 'exit 0' TERM; wait \"$child\"",
      );
      const address = JobAddress.make(jobRequest);

      yield* Effect.gen(function* () {
        const runtime = yield* JobRuntime;
        yield* runtime.start(jobRequest);
        const running = yield* waitForJob(runtime, address, (status) => status === "Running");
        const identity = Option.getOrThrow(running.identity);
        expect(Option.map(yield* runtime.cancel(address), (job) => job.status)).toEqual(
          Option.some("Cancelled"),
        );
        expect(yield* makeBunProcessController.isGroupAlive(identity)).toBe(false);
      }).pipe(Effect.provide(runtimeLayer(`${directory}/loom.sqlite`, workspaceRoot)));
    }),
);
