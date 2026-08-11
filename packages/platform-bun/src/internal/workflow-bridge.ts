/* oxlint-disable effect/noNewPromise -- This adapter owns the VM Promise bridge. */
import {
  describeWorkflowSourceError,
  WorkflowDuplicateStepError,
  WorkflowRunError,
  WorkflowSourceError,
} from "@cvr/loom-runtime";
import { Cause, Effect, Exit, Inspectable, Option, Predicate, Queue, Schema } from "effect";

const decodeSourceError = Schema.decodeUnknownOption(Schema.Struct({ message: Schema.String }));

export const workflowSourceError = (cause: unknown): WorkflowSourceError =>
  new WorkflowSourceError({
    message: describeWorkflowSourceError(
      Option.match(decodeSourceError(cause), {
        onNone: () => Inspectable.toStringUnknown(cause),
        onSome: ({ message }) => message,
      }),
    ),
  });

export interface WorkflowBridge {
  readonly next: Effect.Effect<WorkflowBridgeRequest>;
  readonly promiseAll: (values: unknown) => Promise<unknown>;
  readonly run: (received: unknown) => Promise<unknown>;
  readonly runStep: (call: unknown) => object;
  readonly settle: (
    request: WorkflowBridgeRequest,
    exit: Exit.Exit<Schema.Json, WorkflowRunError>,
  ) => Effect.Effect<void, WorkflowRunError>;
  readonly sourceError: (cause: unknown) => WorkflowRunError;
  readonly deactivate: Effect.Effect<void>;
}

export interface WorkflowBridgeRequest {
  readonly received: unknown;
  readonly resolve: (value: Schema.Json) => void;
  readonly reject: (reason: object) => void;
}

const rejectFailure = (
  failures: WeakMap<object, WorkflowRunError>,
  reject: (reason: object) => void,
  error: WorkflowRunError,
) => {
  const failure = Object.freeze({});
  failures.set(failure, error);
  reject(failure);
};

const recoverSourceError = (
  failures: WeakMap<object, WorkflowRunError>,
  cause: unknown,
): WorkflowRunError => {
  if (Predicate.isObject(cause)) {
    const error = Option.fromNullishOr(failures.get(cause));
    if (Option.isSome(error)) return error.value;
  }
  return workflowSourceError(cause);
};

const makeRun =
  (
    requests: Queue.Queue<WorkflowBridgeRequest>,
    failures: WeakMap<object, WorkflowRunError>,
    isActive: () => boolean,
  ) =>
  (received: unknown): Promise<unknown> =>
    new Promise((resolve, reject) => {
      if (isActive()) {
        Queue.offerUnsafe(requests, { received, resolve, reject });
      } else {
        rejectFailure(
          failures,
          reject,
          new WorkflowSourceError({ message: "The Workflow bridge is inactive." }),
        );
      }
    });

const makeRejected = (failures: WeakMap<object, WorkflowRunError>) => (error: WorkflowRunError) =>
  new Promise<never>((_resolve, reject) => {
    rejectFailure(failures, reject, error);
  });

const makeRunStep =
  (run: WorkflowBridge["run"], stepCalls: WeakMap<object, unknown>) =>
  (call: unknown): object => {
    const thenable = Object.freeze({
      then: (...callbacks: Parameters<Promise<unknown>["then"]>) =>
        run({ _tag: "Step", call }).then(...callbacks),
    });
    stepCalls.set(thenable, call);
    return thenable;
  };

const makePromiseAll =
  (
    run: WorkflowBridge["run"],
    rejected: ReturnType<typeof makeRejected>,
    stepCalls: WeakMap<object, unknown>,
  ) =>
  (values: unknown): Promise<unknown> => {
    if (!Array.isArray(values)) {
      return rejected(new WorkflowSourceError({ message: "Promise.all expects an array." }));
    }
    if (values.length === 0) return Promise.resolve([]);
    const calls: Array<unknown> = [];
    for (const value of values) {
      let call: Option.Option<unknown> = Option.none();
      if (Predicate.isObject(value)) call = Option.fromNullishOr(stepCalls.get(value));
      if (Option.isNone(call)) {
        return rejected(
          new WorkflowSourceError({
            message: "Promise.all accepts only values returned by step.run.",
          }),
        );
      }
      calls.push(call.value);
    }
    return run({ _tag: "Parallel", calls });
  };

const makeSettle =
  (failures: WeakMap<object, WorkflowRunError>) =>
  (
    request: WorkflowBridgeRequest,
    exit: Exit.Exit<Schema.Json, WorkflowRunError>,
  ): Effect.Effect<void, WorkflowRunError> => {
    if (Exit.isSuccess(exit)) {
      request.resolve(exit.value);
      return Effect.void;
    }
    const error = Cause.findErrorOption(exit.cause);
    if (Option.isSome(error) && !Cause.hasDies(exit.cause) && !Cause.hasInterrupts(exit.cause)) {
      rejectFailure(failures, request.reject, error.value);
      if (error.value instanceof WorkflowDuplicateStepError) return error.value;
      return Effect.void;
    }
    return Effect.failCause(exit.cause);
  };

export const makeWorkflowBridge: Effect.Effect<WorkflowBridge> = Effect.gen(function* () {
  const requests = yield* Queue.unbounded<WorkflowBridgeRequest>();
  const failures = new WeakMap<object, WorkflowRunError>();
  const stepCalls = new WeakMap<object, unknown>();
  let active = true;
  const run = makeRun(requests, failures, () => active);
  const rejected = makeRejected(failures);
  return {
    next: Queue.take(requests),
    promiseAll: makePromiseAll(run, rejected, stepCalls),
    run,
    runStep: makeRunStep(run, stepCalls),
    settle: makeSettle(failures),
    sourceError: (cause) => recoverSourceError(failures, cause),
    deactivate: Effect.sync(() => {
      active = false;
    }),
  };
});
