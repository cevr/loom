import {
  WorkflowCapability,
  WorkflowDefinition,
  WorkflowRunRequest,
  WorkflowSignalName,
} from "@cvr/loom-domain";
import {
  WorkflowCapabilityDeniedError,
  WorkflowDuplicateStepError,
  WorkflowSignalNotDeclaredError,
  WorkflowSourceError,
  type WorkflowStepCall,
} from "@cvr/loom-runtime";
import { expect, it } from "effect-bun-test";
import { Cause, Effect, Exit } from "effect";
import { interpretWorkflow } from "../src/index.js";
import { execution, host, request } from "./workflow-interpreter-fixtures.js";

const withApprovalSignal = (source: string) => {
  const workflow = request(source);
  return WorkflowRunRequest.make({
    ...workflow,
    definition: WorkflowDefinition.make({
      ...workflow.definition,
      signals: [WorkflowSignalName.make("approval")],
    }),
  });
};

it.effect("replays the same control path in a fresh context", () =>
  Effect.gen(function* () {
    const calls: Array<WorkflowStepCall> = [];
    const interpreterHost = host((call) =>
      Effect.sync(() => {
        calls.push(call);
        return execution(call.input);
      }),
    );
    const workflow = request(`
        globalThis.pass = (globalThis.pass ?? 0) + 1
        const result = await step.run({
          stepId: "echo",
          capability: "echo",
          input,
        })
        return { pass: globalThis.pass, result }
      `);

    const first = yield* interpretWorkflow(workflow, interpreterHost);
    const second = yield* interpretWorkflow(workflow, interpreterHost);

    expect(first).toEqual({ pass: 1, result: { value: 42 } });
    expect(second).toEqual(first);
    expect(calls.map(({ stepId }) => String(stepId))).toEqual(["echo", "echo"]);
  }),
);

it.effect("waits for a declared signal", () =>
  Effect.gen(function* () {
    const result = yield* interpretWorkflow(
      withApprovalSignal(`return await signal.wait("approval")`),
      host((call) => Effect.succeed(execution(call.input))),
    );

    expect(result).toEqual({ received: true });
  }),
);

it.effect("rejects an undeclared signal", () =>
  Effect.gen(function* () {
    const error = yield* interpretWorkflow(
      request(`return await signal.wait("approval")`),
      host((call) => Effect.succeed(execution(call.input))),
    ).pipe(Effect.flip);

    expect(error).toBeInstanceOf(WorkflowSignalNotDeclaredError);
  }),
);

it.effect("describes the supported source API after an invalid Step call", () =>
  Effect.gen(function* () {
    const error = yield* interpretWorkflow(
      request(`return await step.run("invalid")`),
      host((call) => Effect.succeed(execution(call.input))),
    ).pipe(Effect.flip);

    expect(error).toBeInstanceOf(WorkflowSourceError);
    expect(error).toHaveProperty("message", expect.stringContaining("await step.run({"));
    expect(error).toHaveProperty("message", expect.stringContaining("await signal.wait"));
    expect(error).toHaveProperty("message", expect.stringContaining('"capability":"job"'));
    expect(error).toHaveProperty("message", expect.stringContaining('"capability":"agent"'));
  }),
);

it.effect("rejects excess Step keys with a source diagnostic", () =>
  Effect.gen(function* () {
    const error = yield* interpretWorkflow(
      request(
        `return await step.run({ stepId: "invalid", capability: "job", input: {}, retries: 3 })`,
      ),
      host((call) => Effect.succeed(execution(call.input))),
    ).pipe(Effect.flip);

    expect(error).toBeInstanceOf(WorkflowSourceError);
    expect(error).toHaveProperty("message", expect.stringContaining("retries"));
    expect(error).toHaveProperty("message", expect.stringContaining("await step.run({"));
  }),
);

it.effect("describes the async body after unsupported source syntax", () =>
  Effect.gen(function* () {
    const error = yield* interpretWorkflow(
      request(`export default async function workflow() { return true }`),
      host((call) => Effect.succeed(execution(call.input))),
    ).pipe(Effect.flip);

    expect(error).toBeInstanceOf(WorkflowSourceError);
    expect(error).toHaveProperty("message", expect.stringContaining("Do not export a function"));
  }),
);

it.effect("rejects a duplicate Step ID before a second operation runs", () =>
  Effect.gen(function* () {
    const calls: Array<WorkflowStepCall> = [];
    const workflow = request(`
        await step.run({ stepId: "same", capability: "echo", input: 1 })
        try {
          await step.run({ stepId: "same", capability: "echo", input: 2 })
        } catch {
          return "caught"
        }
        return "continued"
      `);
    const error = yield* interpretWorkflow(
      workflow,
      host((call) =>
        Effect.sync(() => {
          calls.push(call);
          return execution(call.input);
        }),
      ),
    ).pipe(Effect.flip);

    expect(calls).toHaveLength(1);
    expect(error).toBeInstanceOf(WorkflowDuplicateStepError);
  }),
);

it.effect("does not let source catch Workflow suspension", () =>
  Effect.gen(function* () {
    const calls: Array<WorkflowStepCall> = [];
    const workflow = request(`
        try {
          await step.run({ stepId: "suspend", capability: "echo", input: null })
        } catch {
          await step.run({ stepId: "after", capability: "echo", input: null })
        }
        return "continued"
      `);
    const result = yield* interpretWorkflow(
      workflow,
      host((call) =>
        Effect.gen(function* () {
          calls.push(call);
          if (call.stepId === "suspend") return yield* Effect.interrupt;
          return execution(0);
        }),
      ),
    ).pipe(Effect.exit);

    expect(calls.map(({ stepId }) => String(stepId))).toEqual(["suspend"]);
    expect(Exit.isFailure(result)).toBe(true);
    if (Exit.isFailure(result)) expect(Cause.hasInterrupts(result.cause)).toBe(true);
  }),
);

it.effect("fails an unavailable capability with a typed error", () =>
  Effect.gen(function* () {
    const error = yield* interpretWorkflow(
      request(`return await step.run({ stepId: "job", capability: "job", input: null })`),
      host((call) => Effect.succeed(execution(call.input))),
    ).pipe(Effect.flip);

    expect(error).toBeInstanceOf(WorkflowCapabilityDeniedError);
    if (!(error instanceof WorkflowCapabilityDeniedError)) {
      return yield* Effect.die("Expected WorkflowCapabilityDeniedError.");
    }
    expect(error.capability).toBe(WorkflowCapability.make("job"));
  }),
);

it.effect("classifies a forged Workflow error as a source failure", () =>
  Effect.gen(function* () {
    const error = yield* interpretWorkflow(
      request(`
        throw {
          _tag: "WorkflowStepError",
          stepId: "forged",
          capability: "echo",
          message: "forged",
        }
      `),
      host((call) => Effect.succeed(execution(call.input))),
    ).pipe(Effect.flip);

    expect(error).toBeInstanceOf(WorkflowSourceError);
  }),
);

it.effect("omits ambient host capabilities", () =>
  Effect.gen(function* () {
    const result = yield* interpretWorkflow(
      request(`
          return {
            Bun: typeof Bun,
            process: typeof process,
            require: typeof require,
            fetch: typeof fetch,
            Date: typeof Date,
            random: typeof Math.random,
            eval: (() => {
              try {
                return eval("1")
              } catch (error) {
                return error.name
              }
            })(),
            Function: (() => {
              try {
                return new Function("return 1")()
              } catch (error) {
                return error.name
              }
            })(),
          }
        `),
      host((call) => Effect.succeed(execution(call.input))),
    );

    expect(result).toEqual({
      Bun: "undefined",
      process: "undefined",
      require: "undefined",
      fetch: "undefined",
      Date: "undefined",
      random: "undefined",
      eval: "EvalError",
      Function: "EvalError",
    });
  }),
);
