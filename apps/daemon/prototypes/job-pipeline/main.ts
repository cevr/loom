/* oxlint-disable effect/noGlobals, effect/noNodeBuiltinImport, eslint/max-lines-per-function, eslint/no-underscore-dangle -- PROTOTYPE: this Bun platform probe must inspect process-group liveness and print its full state. */
import { BunRuntime, BunServices } from "@effect/platform-bun";
import {
  Cause,
  Deferred,
  Duration,
  Effect,
  Exit,
  Fiber,
  FileSystem,
  Inspectable,
  Ref,
  Stream,
} from "effect";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

type Channel = "stdout" | "stderr";

interface OutputEvent {
  readonly sequence: number;
  readonly channel: Channel;
  readonly bytes: number;
}

type JobState =
  | { readonly _tag: "Accepted" }
  | { readonly _tag: "Starting" }
  | { readonly _tag: "Running"; readonly pid: number }
  | { readonly _tag: "Stopping"; readonly pid: number }
  | { readonly _tag: "Succeeded"; readonly exitCode: number }
  | { readonly _tag: "Failed"; readonly cause: string }
  | { readonly _tag: "Cancelled"; readonly cause: string };

interface JobController {
  readonly state: Ref.Ref<JobState>;
  readonly handle: Deferred.Deferred<ChildProcessSpawner.ChildProcessHandle>;
  readonly terminal: Deferred.Deferred<JobState>;
  readonly output: Ref.Ref<ReadonlyArray<OutputEvent>>;
  readonly stdoutPath: string;
  readonly stderrPath: string;
  readonly cancel: Effect.Effect<void, unknown>;
}

const leaseExpired = "LeaseExpired" satisfies "LeaseExpired";

const pipeline = String.raw`
trap '' TERM
printf 'job-started\n'
i=0
while :; do
  i=$((i + 1))
  printf 'stdout-%s\n' "$i"
  printf 'stderr-%s\n' "$i" >&2
  if [ $((i % 100)) -eq 0 ]; then
    sleep 0.01
  fi
done | {
  trap '' TERM
  tail -n +1
}
`;

const printState = Effect.fn("JobPipelinePrototype.printState")(function* (
  label: string,
  value: unknown,
) {
  yield* Effect.log(`\n${label}\n${Inspectable.toStringUnknown(value)}`);
});

const requireCondition = Effect.fn("JobPipelinePrototype.requireCondition")(function* (
  condition: boolean,
  message: string,
) {
  if (!condition) {
    return yield* Effect.die(new Error(message));
  }
});

const processGroupIsAlive = (pid: number): Effect.Effect<boolean> =>
  Effect.try({
    try: () => {
      process.kill(-pid, 0);
      return true;
    },
    catch: () => false,
  }).pipe(Effect.catch(() => Effect.succeed(false)));

const captureOutput = Effect.fn("JobPipelinePrototype.captureOutput")(function* (
  channel: Channel,
  stream: Stream.Stream<Uint8Array, unknown>,
  path: string,
  events: Ref.Ref<ReadonlyArray<OutputEvent>>,
) {
  const fs = yield* FileSystem.FileSystem;
  yield* stream.pipe(
    Stream.tap((chunk) =>
      Ref.update(events, (current) => [
        ...current,
        {
          sequence: current.length + 1,
          channel,
          bytes: chunk.length,
        },
      ]),
    ),
    Stream.run(fs.sink(path)),
  );
});

const startJob = Effect.fn("JobPipelinePrototype.startJob")(function* () {
  const fs = yield* FileSystem.FileSystem;
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const outputDirectory = yield* fs.makeTempDirectory({ prefix: "loom-job-prototype-" });
  const stdoutPath = `${outputDirectory}/stdout.log`;
  const stderrPath = `${outputDirectory}/stderr.log`;
  const state = yield* Ref.make<JobState>({ _tag: "Accepted" });
  const handle = yield* Deferred.make<ChildProcessSpawner.ChildProcessHandle>();
  const terminal = yield* Deferred.make<JobState>();
  const output = yield* Ref.make<ReadonlyArray<OutputEvent>>([]);
  const cancellationRequested = yield* Ref.make(false);

  const command = ChildProcess.make("/bin/sh", ["-lc", pipeline], {
    detached: true,
    killSignal: "SIGTERM",
    forceKillAfter: "500 millis",
    stdin: "ignore",
    stdout: "pipe",
    stderr: "pipe",
  });

  const run = Effect.scoped(
    Effect.gen(function* () {
      yield* Ref.set(state, { _tag: "Starting" });
      const child = yield* spawner.spawn(command);
      yield* Ref.set(state, { _tag: "Running", pid: child.pid });
      yield* Deferred.succeed(handle, child);

      const stdoutCapture = yield* captureOutput("stdout", child.stdout, stdoutPath, output).pipe(
        Effect.forkChild,
      );
      const stderrCapture = yield* captureOutput("stderr", child.stderr, stderrPath, output).pipe(
        Effect.forkChild,
      );
      const processExit = yield* Effect.exit(child.exitCode);
      yield* Fiber.await(stdoutCapture);
      yield* Fiber.await(stderrCapture);

      const wasCancelled = yield* Ref.get(cancellationRequested);
      let finalState: JobState;
      if (wasCancelled) {
        let cause: string;
        if (Exit.isFailure(processExit)) {
          cause = Cause.pretty(processExit.cause);
        } else {
          cause = `exit code ${processExit.value}`;
        }
        finalState = { _tag: "Cancelled", cause };
      } else if (Exit.isSuccess(processExit)) {
        finalState = { _tag: "Succeeded", exitCode: processExit.value };
      } else {
        finalState = { _tag: "Failed", cause: Cause.pretty(processExit.cause) };
      }
      yield* Ref.set(state, finalState);
      yield* Deferred.succeed(terminal, finalState);
    }),
  );

  yield* Effect.forkScoped(run);

  const cancel = Effect.gen(function* () {
    const child = yield* Deferred.await(handle);
    yield* Ref.set(cancellationRequested, true);
    yield* Ref.set(state, { _tag: "Stopping", pid: child.pid });
    yield* child.kill({ killSignal: "SIGTERM", forceKillAfter: "500 millis" });
  });

  return {
    state,
    handle,
    terminal,
    output,
    stdoutPath,
    stderrPath,
    cancel,
  } satisfies JobController;
});

const observeLease = (
  job: JobController,
  duration: Duration.Input,
): Effect.Effect<"LeaseExpired" | JobState> =>
  Effect.raceFirst(
    Deferred.await(job.terminal),
    Effect.succeed<"LeaseExpired">(leaseExpired).pipe(Effect.delay(duration)),
  );

const program = Effect.scoped(
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const job = yield* startJob();
    const child = yield* Deferred.await(job.handle);

    yield* printState("1. Job accepted and process group started", yield* Ref.get(job.state));

    const leaseResult = yield* observeLease(job, "150 millis");
    yield* printState("2. Foreground Lease result", leaseResult);
    yield* requireCondition(leaseResult === "LeaseExpired", "The Foreground Lease did not expire.");
    yield* requireCondition(
      yield* processGroupIsAlive(child.pid),
      "The process group stopped when the Foreground Lease expired.",
    );

    const caller = yield* Deferred.await(job.terminal).pipe(Effect.forkChild);
    yield* Fiber.interrupt(caller);
    yield* Effect.sleep("150 millis");
    yield* requireCondition(
      yield* processGroupIsAlive(child.pid),
      "The process group stopped when the caller disconnected.",
    );

    const outputBeforeCancel = yield* Ref.get(job.output);
    const channels = new Set(outputBeforeCancel.map((event) => event.channel));
    yield* printState("3. Output after caller disconnect", {
      events: outputBeforeCancel.length,
      channels: [...channels],
      stdoutPath: job.stdoutPath,
      stderrPath: job.stderrPath,
    });
    yield* requireCondition(channels.has("stdout"), "The prototype captured no stdout.");
    yield* requireCondition(channels.has("stderr"), "The prototype captured no stderr.");

    const cancelStartedAt = yield* Effect.clockWith((clock) => clock.currentTimeMillis);
    yield* job.cancel;
    const finalState = yield* Deferred.await(job.terminal);
    const cancelEndedAt = yield* Effect.clockWith((clock) => clock.currentTimeMillis);
    const graceElapsed = cancelEndedAt - cancelStartedAt;

    yield* printState("4. Final Job state", { finalState, graceElapsed });
    yield* requireCondition(
      finalState["_tag"] === "Cancelled",
      "Cancellation did not produce Cancelled.",
    );
    yield* requireCondition(
      graceElapsed >= 450,
      "The process group did not require kill escalation.",
    );
    yield* requireCondition(
      !(yield* processGroupIsAlive(child.pid)),
      "A process in the Job process group survived cancellation.",
    );

    const stdout = yield* fs.readFileString(job.stdoutPath);
    const stderr = yield* fs.readFileString(job.stderrPath);
    yield* printState("5. Complete output Artifacts", {
      stdoutBytes: stdout.length,
      stderrBytes: stderr.length,
      stdoutLastLine: stdout.trim().split("\n").at(-1),
      stderrLastLine: stderr.trim().split("\n").at(-1),
    });
    yield* requireCondition(stdout.length > 0, "The stdout Artifact is empty.");
    yield* requireCondition(stderr.length > 0, "The stderr Artifact is empty.");

    yield* printState("Verdict", {
      result: "PASS",
      effectOwnsDetachedProcessGroups: true,
      leaseDoesNotOwnJobLifetime: true,
      callerDoesNotOwnJobLifetime: true,
      cancellationStopsProcessGroup: true,
      outputSurvivesCaller: true,
      signalNeedsLoomAdapter: true,
    });
  }),
).pipe(Effect.provide(BunServices.layer));

BunRuntime.runMain(program);
