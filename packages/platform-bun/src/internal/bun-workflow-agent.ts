/* oxlint-disable effect/noGlobals -- This Bun adapter builds a process command and decodes its text output. */
import {
  JobAddress,
  JobOutcome,
  JobRequest,
  type JobTerminalRecord,
  type WorkflowChildAgent,
  type WorkspaceRoot,
} from "@cvr/loom-domain";
import {
  JobRuntimeError,
  type JobOutputStream,
  type JobRuntimeShape,
  WorkflowAgentResult,
} from "@cvr/loom-runtime";
import { Effect, FileSystem, Option } from "effect";

export interface BunWorkflowAgentConfig {
  readonly workspaceRoot: WorkspaceRoot;
  readonly executable: string;
  readonly arguments: ReadonlyArray<string>;
  readonly maximumOutputBytes: number;
}

export type BunWorkflowAgentPolicy = Omit<BunWorkflowAgentConfig, "workspaceRoot">;

export const defaultBunWorkflowAgentPolicy = {
  executable: "pi",
  arguments: ["--print", "--no-session", "--no-extensions", "--model", "openai-codex/gpt-5.6-luna"],
  maximumOutputBytes: 64 * 1_024,
} satisfies BunWorkflowAgentPolicy;

const quoteShellArgument = (value: string) => `'${value.replaceAll("'", `'"'"'`)}'`;
const textDecoder = new TextDecoder();

export const prepareWorkflowAgent = Effect.fn("BunWorkflowAgent.prepare")(function* (
  config: BunWorkflowAgentConfig,
  agent: WorkflowChildAgent,
) {
  const fs = yield* FileSystem.FileSystem;
  const directory = `${config.workspaceRoot}/.loom/agents/${encodeURIComponent(agent.agentId)}`;
  const promptPath = `${directory}/prompt.txt`;
  yield* fs.makeDirectory(directory, { recursive: true });
  yield* fs.writeFileString(promptPath, agent.prompt);
  const serializedArguments = config.arguments.map(quoteShellArgument).join(" ");
  return JobRequest.make({
    jobId: agent.jobId,
    sessionId: agent.parent.sessionId,
    command: `${quoteShellArgument(config.executable)} ${serializedArguments} < ${quoteShellArgument(promptPath)}`,
    attached: true,
  });
});

const outcomeFor = (job: JobTerminalRecord) => {
  switch (job.status) {
    case "Succeeded":
      return JobOutcome.cases.Succeeded.make({ exitCode: job.exitCode });
    case "Failed":
      return JobOutcome.cases.Failed.make({ failure: job.failure });
    case "Cancelled":
      return JobOutcome.cases.Cancelled.make({});
    case "Lost":
      return JobOutcome.cases.Lost.make({ detail: job.detail });
  }
};

const readOutput = (
  config: BunWorkflowAgentConfig,
  jobs: JobRuntimeShape,
  agent: WorkflowChildAgent,
  stream: JobOutputStream,
) =>
  jobs
    .readOutput({
      jobId: agent.jobId,
      sessionId: agent.parent.sessionId,
      stream,
      sequence: 0,
      maximumBytes: config.maximumOutputBytes,
    })
    .pipe(Effect.map((chunk) => textDecoder.decode(chunk.data)));

export const awaitWorkflowAgent = Effect.fn("BunWorkflowAgent.await")(function* (
  config: BunWorkflowAgentConfig,
  jobs: JobRuntimeShape,
  agent: WorkflowChildAgent,
) {
  const terminal = yield* jobs.awaitTerminal(
    JobAddress.make({ jobId: agent.jobId, sessionId: agent.parent.sessionId }),
  );
  if (Option.isNone(terminal)) {
    return yield* new JobRuntimeError({
      operation: "awaitAgent",
      cause: "The Agent Job record is missing.",
    });
  }
  const [stdout, stderr] = yield* Effect.all(
    [readOutput(config, jobs, agent, "stdout"), readOutput(config, jobs, agent, "stderr")],
    { concurrency: "unbounded" },
  );
  return WorkflowAgentResult.make({
    agentId: agent.agentId,
    outcome: outcomeFor(terminal.value),
    stdout,
    stderr,
  });
});
