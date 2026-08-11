import { JobFailure, JobOutcome, type JobRecord, type WorkspaceRoot } from "@cvr/loom-domain";
import { Effect, FileSystem, Option, type PlatformError, Schema, Stream } from "effect";
import { ChildProcess, type ChildProcessSpawner } from "effect/unstable/process";

const ExitCodeFile = Schema.FiniteFromString.check(Schema.isInt());
const decodeExitCode = Schema.decodeUnknownEffect(ExitCodeFile);

export const makeJobCommand = (job: JobRecord, workspaceRoot: WorkspaceRoot) =>
  ChildProcess.make(
    "/bin/sh",
    [
      "-c",
      'IFS= read -r launch || exit 0; [ "$launch" = start ] || exit 0; exec </dev/null >"$1" 2>"$2"; eval "$4"; code=$?; temporary="$3.$$"; printf "%s\\n" "$code" >"$temporary"; mv "$temporary" "$3"; exit "$code"',
      "loom-job",
      job.stdoutPath,
      job.stderrPath,
      job.resultPath,
      job.command,
    ],
    {
      cwd: workspaceRoot,
      detached: true,
      stdin: { stream: "pipe" },
      stdout: "ignore",
      stderr: "ignore",
    },
  );

export const releaseJob = (child: ChildProcessSpawner.ChildProcessHandle) =>
  Stream.succeed("start\n").pipe(Stream.encodeText, Stream.run(child.stdin));

export const outcomeForExitCode = (exitCode: number): JobOutcome => {
  if (exitCode === 0) return JobOutcome.cases.Succeeded.make({ exitCode });
  return JobOutcome.cases.Failed.make({
    failure: JobFailure.cases.Exit.make({
      exitCode,
      detail: Option.none(),
    }),
  });
};

export const readJobOutcome = (
  fs: FileSystem.FileSystem,
  job: JobRecord,
): Effect.Effect<Option.Option<JobOutcome>, PlatformError.PlatformError | Schema.SchemaError> =>
  fs.readFileString(job.resultPath).pipe(
    Effect.flatMap((contents) => decodeExitCode(contents.trim())),
    Effect.map(outcomeForExitCode),
    Effect.map(Option.some),
    Effect.catchReason("PlatformError", "NotFound", () => Effect.succeed(Option.none())),
  );
