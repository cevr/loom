/* oxlint-disable effect/noNewPromise -- This adapter owns the VM Promise bridge. */
import {
  describeWorkflowSourceError,
  WorkflowDuplicateStepError,
  WorkflowRunError,
  WorkflowSourceError,
} from "@cvr/loom-runtime";
import {
  Cause,
  Deferred,
  Effect,
  Exit,
  FiberSet,
  Inspectable,
  Option,
  Predicate,
  Schema,
  Scope,
} from "effect";

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
  readonly fatal: Effect.Effect<never, WorkflowRunError>;
  readonly run: (received: unknown) => Promise<unknown>;
  readonly sourceError: (cause: unknown) => WorkflowRunError;
  readonly deactivate: Effect.Effect<void>;
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

export const makeWorkflowBridge = <R>(
  runHostCall: (received: unknown) => Effect.Effect<Schema.Json, WorkflowRunError, R>,
): Effect.Effect<WorkflowBridge, never, R | Scope.Scope> =>
  Effect.gen(function* () {
    const fibers = yield* FiberSet.make<Schema.Json, WorkflowRunError>();
    const fatal = yield* Deferred.make<never, WorkflowRunError>();
    const runFork = yield* FiberSet.runtime(fibers)<R>();
    const failures = new WeakMap<object, WorkflowRunError>();
    let active = true;
    const run = (received: unknown): Promise<unknown> =>
      new Promise((resolve, reject) => {
        if (!active) {
          rejectFailure(
            failures,
            reject,
            new WorkflowSourceError({ message: "The Workflow bridge is inactive." }),
          );
          return;
        }
        runFork(runHostCall(received)).addObserver((exit) => {
          if (Exit.isSuccess(exit)) {
            resolve(exit.value);
            return;
          }
          const error = Cause.findErrorOption(exit.cause);
          if (
            Option.isSome(error) &&
            !Cause.hasDies(exit.cause) &&
            !Cause.hasInterrupts(exit.cause)
          ) {
            if (error.value instanceof WorkflowDuplicateStepError) {
              Deferred.doneUnsafe(fatal, error.value);
              return;
            }
            rejectFailure(failures, reject, error.value);
            return;
          }
          Deferred.doneUnsafe(fatal, Effect.failCause(exit.cause));
        });
      });
    return {
      fatal: Deferred.await(fatal),
      run,
      sourceError: (cause) => recoverSourceError(failures, cause),
      deactivate: Effect.sync(() => {
        active = false;
      }).pipe(Effect.andThen(FiberSet.clear(fibers))),
    };
  });
