import { CodeKernelProcessRecord, type AgentOwner, type ProcessIdentity } from "@cvr/loom-domain";
import {
  type CodeKernelProcessStoreShape,
  ProcessObservation,
  type ProcessInspectorShape,
} from "@cvr/loom-runtime";
import { Deferred, Effect, Option } from "effect";
import { CodeKernelProcessError } from "./code-kernel-process-error.js";

export interface KernelProcessLifecycle {
  readonly register: (pid: number) => Effect.Effect<ProcessIdentity, CodeKernelProcessError>;
  readonly release: (identity: ProcessIdentity) => Effect.Effect<void>;
}

export interface KernelProcessRegistration {
  readonly identity: Deferred.Deferred<Option.Option<ProcessIdentity>>;
  readonly lifecycle: Option.Option<KernelProcessLifecycle>;
}

export const makeKernelProcessRegistration = (lifecycle: Option.Option<KernelProcessLifecycle>) =>
  Deferred.make<Option.Option<ProcessIdentity>>().pipe(
    Effect.map((identity) => ({ identity, lifecycle })),
  );

export const registerKernelProcess = Effect.fn("CodeKernelProcess.registerChild")(function* (
  registration: KernelProcessRegistration,
  pid: number,
) {
  let identity = Option.none<ProcessIdentity>();
  if (Option.isSome(registration.lifecycle)) {
    identity = Option.some(
      yield* registration.lifecycle.value
        .register(pid)
        .pipe(Effect.onError(() => Deferred.succeed(registration.identity, Option.none()))),
    );
  }
  yield* Deferred.succeed(registration.identity, identity);
});

export const releaseKernelProcess = (registration: KernelProcessRegistration) =>
  Option.match(registration.lifecycle, {
    onNone: () => Effect.void,
    onSome: (lifecycle) =>
      Deferred.await(registration.identity).pipe(
        Effect.flatMap(
          Option.match({
            onNone: () => Effect.void,
            onSome: lifecycle.release,
          }),
        ),
      ),
  });

const inspectProcessIdentity = Effect.fn("CodeKernelProcess.inspectIdentity")(function* (
  inspector: ProcessInspectorShape,
  pid: number,
) {
  const observation = yield* inspector.inspect(pid).pipe(
    Effect.mapError(
      (cause) =>
        new CodeKernelProcessError({
          reason: "ProcessIdentityFailure",
          message: "Code Kernel process identity inspection failed.",
          cause,
        }),
    ),
  );
  return yield* ProcessObservation.$match(observation, {
    Missing: () =>
      Effect.fail(
        new CodeKernelProcessError({
          reason: "ProcessIdentityFailure",
          message: "Code Kernel process disappeared before identity registration.",
          cause: observation,
        }),
      ),
    Found: ({ identity }) => Effect.succeed(identity),
  });
});

export const makeKernelProcessLifecycle = (
  owner: AgentOwner,
  store: CodeKernelProcessStoreShape,
  inspector: ProcessInspectorShape,
): KernelProcessLifecycle => {
  const recordFor = (identity: ProcessIdentity) =>
    CodeKernelProcessRecord.make({ ...owner, ...identity });
  return {
    register: Effect.fn("CodeKernelProcess.register")(function* (pid) {
      const processIdentity = yield* inspectProcessIdentity(inspector, pid);
      const registered = yield* store.register(recordFor(processIdentity)).pipe(
        Effect.mapError(
          (cause) =>
            new CodeKernelProcessError({
              reason: "ProcessIdentityFailure",
              message: "Code Kernel process identity registration failed.",
              cause,
            }),
        ),
      );
      if (registered) return processIdentity;
      return yield* new CodeKernelProcessError({
        reason: "ProcessIdentityFailure",
        message: "This Agent already owns a Code Kernel process.",
        cause: owner,
      });
    }),
    release: (identity) =>
      store.remove(recordFor(identity)).pipe(
        Effect.tapError((error) =>
          Effect.logWarning("Code Kernel process identity removal failed.", error),
        ),
        Effect.ignore,
      ),
  };
};
