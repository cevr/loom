import {
  SessionId,
  WorkflowCapability,
  WorkflowRunId,
  WorkflowSignalAddress,
  WorkflowSignalName,
  WorkflowStepId,
} from "@cvr/loom-domain";
import { SessionClosingError, WorkflowSignalNotDeclaredError } from "@cvr/loom-protocol";
import { expect, it } from "effect-bun-test";
import { Effect, Schema } from "effect";
import {
  WorkflowBudgetExceededError,
  WorkflowCapabilityDeniedError,
  WorkflowDuplicateStepError,
  WorkflowInterpreterVersionMismatchError,
  WorkflowRunError,
  WorkflowSourceError,
  WorkflowStepError,
} from "../src/index.js";

it.effect("round-trips every durable Workflow failure", () =>
  Effect.gen(function* () {
    const errors: ReadonlyArray<WorkflowRunError> = [
      new WorkflowSourceError({ message: "invalid source" }),
      new WorkflowStepError({
        stepId: WorkflowStepId.make("review"),
        capability: WorkflowCapability.make("agent"),
        message: "agent failed",
      }),
      new WorkflowBudgetExceededError({ budget: "Tokens", limit: 10, actual: 11 }),
      new WorkflowCapabilityDeniedError({ capability: WorkflowCapability.make("job") }),
      new WorkflowDuplicateStepError({ stepId: WorkflowStepId.make("duplicate") }),
      new WorkflowSignalNotDeclaredError({
        address: WorkflowSignalAddress.make({
          sessionId: SessionId.make("session"),
          workflowRunId: WorkflowRunId.make("run"),
          name: WorkflowSignalName.make("approval"),
        }),
      }),
      new WorkflowInterpreterVersionMismatchError({ supported: 1, received: 2 }),
    ];
    const codec = Schema.fromJsonString(WorkflowRunError);

    const decoded = yield* Effect.forEach(errors, (error) =>
      Schema.encodeEffect(codec)(error).pipe(Effect.flatMap(Schema.decodeEffect(codec))),
    );

    expect(decoded.map(({ _tag }) => _tag)).toEqual(errors.map(({ _tag }) => _tag));
  }),
);

it.effect("rejects Session closure as a durable Workflow failure", () =>
  Effect.gen(function* () {
    const error = yield* Schema.decodeUnknownEffect(WorkflowRunError)(
      new SessionClosingError({ sessionId: SessionId.make("closing") }),
    ).pipe(Effect.flip);

    expect(error).toBeInstanceOf(Schema.SchemaError);
  }),
);
