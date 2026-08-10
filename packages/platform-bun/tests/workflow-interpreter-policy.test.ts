import { ArtifactId, WorkflowBudget, WorkflowCapability, WorkflowStepId } from "@cvr/loom-domain";
import {
  WorkflowArtifactReference,
  WorkflowBudgetExceededError,
  WorkflowInterpreterVersionMismatchError,
  WorkflowSourceError,
  WorkflowStepExecution,
  WorkflowStepError,
  type WorkflowStepCall,
} from "@cvr/loom-runtime";
import { expect, it } from "effect-bun-test";
import { Deferred, Effect, Fiber, Latch, Option, Ref } from "effect";
import { interpretWorkflow } from "../src/index.js";
import { budget, execution, host, request } from "./workflow-interpreter-fixtures.js";

it.effect("enforces the Step budget before the excess operation", () =>
  Effect.gen(function* () {
    const calls: Array<WorkflowStepCall> = [];
    const error = yield* interpretWorkflow(
      request(
        `
          await step.run({ stepId: "one", capability: "echo", input: 1 })
          return await step.run({ stepId: "two", capability: "echo", input: 2 })
        `,
        ["echo"],
        WorkflowBudget.make({ ...budget, maxSteps: 1 }),
      ),
      host((call) =>
        Effect.sync(() => {
          calls.push(call);
          return execution(call.input);
        }),
      ),
    ).pipe(Effect.flip);

    expect(error).toBeInstanceOf(WorkflowBudgetExceededError);
    expect(error).toHaveProperty("budget", "Steps");
    expect(calls).toHaveLength(1);
  }),
);

it.effect("enforces Agent and token spend from Step results", () =>
  Effect.gen(function* () {
    const workflow = `return await step.run({ stepId: "agent", capability: "agent", input: null })`;
    const agentError = yield* interpretWorkflow(
      request(workflow, ["agent"], WorkflowBudget.make({ ...budget, maxAgentRuns: 1 })),
      host(() =>
        Effect.succeed(WorkflowStepExecution.make({ value: 0, tokenCount: 0, agentRuns: 2 })),
      ),
    ).pipe(Effect.flip);
    const tokenError = yield* interpretWorkflow(
      request(workflow, ["agent"], WorkflowBudget.make({ ...budget, maxTokens: Option.some(4) })),
      host(() =>
        Effect.succeed(WorkflowStepExecution.make({ value: 0, tokenCount: 5, agentRuns: 1 })),
      ),
    ).pipe(Effect.flip);

    expect(agentError).toHaveProperty("budget", "Agents");
    expect(tokenError).toHaveProperty("budget", "Tokens");
  }),
);

it.effect("bounds parallel Step work with an Effect Semaphore", () =>
  Effect.gen(function* () {
    const active = yield* Ref.make(0);
    const maximum = yield* Ref.make(0);
    const twoStarted = yield* Deferred.make<boolean>();
    const release = yield* Latch.make();
    const execute = () =>
      Effect.acquireUseRelease(
        Ref.updateAndGet(active, (count) => count + 1).pipe(
          Effect.tap((count) => Ref.update(maximum, (current) => Math.max(current, count))),
          Effect.tap((count) => {
            if (count === 2) return Deferred.succeed(twoStarted, true);
            return Effect.void;
          }),
        ),
        () => release.await.pipe(Effect.as(execution(0))),
        () => Ref.update(active, (count) => count - 1),
      );
    const workflow = request(`
      return await Promise.all([
        step.run({ stepId: "one", capability: "echo", input: null }),
        step.run({ stepId: "two", capability: "echo", input: null }),
        step.run({ stepId: "three", capability: "echo", input: null }),
      ])
    `);

    const fiber = yield* interpretWorkflow(workflow, host(execute)).pipe(Effect.forkChild);
    yield* Deferred.await(twoStarted);
    yield* release.open;
    yield* Fiber.join(fiber);

    expect(yield* Ref.get(maximum)).toBe(2);
  }),
);

it.effect("spills a large inline result only through the Artifact capability", () =>
  Effect.gen(function* () {
    const writes: Array<string> = [];
    const smallBudget = WorkflowBudget.make({ ...budget, maxInlineStepResultBytes: 4 });
    const interpreterHost = host(() => Effect.succeed(execution("too large")));
    const result = yield* interpretWorkflow(
      request(
        `return await step.run({ stepId: "large", capability: "echo", input: null })`,
        ["echo", "artifact"],
        smallBudget,
      ),
      {
        ...interpreterHost,
        storeArtifact: ({ stepId }) =>
          Effect.sync(() => {
            writes.push(String(stepId));
            return WorkflowArtifactReference.make({
              artifactId: ArtifactId.make(`artifact-${stepId}`),
            });
          }),
      },
    );

    expect(result).toEqual({ _tag: "Artifact", artifactId: "artifact-large" });
    expect(writes).toEqual(["large"]);
  }),
);

it.effect("fails a large inline result when Artifact is not declared", () =>
  Effect.gen(function* () {
    const error = yield* interpretWorkflow(
      request(
        `return await step.run({ stepId: "large", capability: "echo", input: null })`,
        ["echo"],
        WorkflowBudget.make({ ...budget, maxInlineStepResultBytes: 4 }),
      ),
      host(() => Effect.succeed(execution("too large"))),
    ).pipe(Effect.flip);

    expect(error).toHaveProperty("budget", "InlineResultBytes");
  }),
);

it.effect("uses the host duration limit without exposing a clock to source", () =>
  Effect.gen(function* () {
    const limited = WorkflowBudget.make({ ...budget, maxDurationMillis: Option.some(5) });
    const interpreterHost = host(() => Effect.never);
    const error = yield* interpretWorkflow(
      request(
        `return await step.run({ stepId: "slow", capability: "echo", input: null })`,
        ["echo"],
        limited,
      ),
      {
        ...interpreterHost,
        withDurationLimit: (milliseconds) =>
          new WorkflowBudgetExceededError({
            budget: "Duration",
            limit: milliseconds,
            actual: milliseconds,
          }),
      },
    ).pipe(Effect.flip);

    expect(error).toHaveProperty("budget", "Duration");
  }),
);

it.effect("stops synchronous source at the duration limit", () =>
  Effect.gen(function* () {
    const limited = WorkflowBudget.make({ ...budget, maxDurationMillis: Option.some(10) });
    const error = yield* interpretWorkflow(
      request("while (true) {}", ["echo"], limited),
      host((call) => Effect.succeed(execution(call.input))),
    ).pipe(Effect.flip);

    expect(error).toHaveProperty("budget", "Duration");
  }),
);

it.effect("preserves typed Step failures across the VM bridge", () =>
  Effect.gen(function* () {
    const failure = new WorkflowStepError({
      stepId: WorkflowStepId.make("failed"),
      capability: WorkflowCapability.make("echo"),
      message: "failed",
    });
    const error = yield* interpretWorkflow(
      request(`return await step.run({ stepId: "failed", capability: "echo", input: null })`),
      host(() => failure),
    ).pipe(Effect.flip);

    expect(error).toBeInstanceOf(WorkflowStepError);
  }),
);

it.effect("reports source and interpreter version failures", () =>
  Effect.gen(function* () {
    const interpreterHost = host((call) => Effect.succeed(execution(call.input)));
    const sourceFailure = yield* interpretWorkflow(request("return ("), interpreterHost).pipe(
      Effect.flip,
    );
    const asyncSourceFailure = yield* interpretWorkflow(
      request(`throw new Error("source failed")`),
      interpreterHost,
    ).pipe(Effect.flip);
    const versionFailure = yield* interpretWorkflow(
      request("return null", ["echo"], budget, 2),
      interpreterHost,
    ).pipe(Effect.flip);

    expect(sourceFailure).toBeInstanceOf(WorkflowSourceError);
    expect(asyncSourceFailure).toHaveProperty("message", "source failed");
    expect(versionFailure).toBeInstanceOf(WorkflowInterpreterVersionMismatchError);
  }),
);
