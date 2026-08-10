import { Cause, Clock, Duration, Effect, Option } from "effect";
import { CodeKernelProcessError } from "./code-kernel-process-error.js";

export interface CodeKernelSupervisorPolicyConfig {
  readonly crashLoopLimit?: number;
  readonly crashLoopWindow?: Duration.Input;
  readonly crashLoopCooldown?: Duration.Input;
}

export interface CodeKernelSupervisorPolicyState {
  failureTimes: Array<number>;
  blockedUntil: Option.Option<number>;
}

export const assertStartAllowed = Effect.fn("CodeKernelProcess.assertStartAllowed")(function* (
  state: CodeKernelSupervisorPolicyState,
) {
  if (Option.isNone(state.blockedUntil)) return;
  const blockedUntil = state.blockedUntil.value;
  const now = yield* Clock.currentTimeMillis;
  if (now >= blockedUntil) {
    yield* clearProcessFailures(state);
    return;
  }
  return yield* new CodeKernelProcessError({
    reason: "CrashLoop",
    message: `Code Kernel restart is blocked for ${blockedUntil - now} milliseconds.`,
    cause: Cause.empty,
  });
});

export const recordProcessFailure = Effect.fn("CodeKernelProcess.recordProcessFailure")(function* (
  config: CodeKernelSupervisorPolicyConfig,
  state: CodeKernelSupervisorPolicyState,
) {
  const now = yield* Clock.currentTimeMillis;
  const window = Duration.toMillis(config.crashLoopWindow ?? "30 seconds");
  state.failureTimes = [...state.failureTimes.filter((time) => now - time <= window), now];
  if (state.failureTimes.length >= (config.crashLoopLimit ?? 3)) {
    state.blockedUntil = Option.some(
      now + Duration.toMillis(config.crashLoopCooldown ?? "30 seconds"),
    );
  }
});

export const clearProcessFailures = (state: CodeKernelSupervisorPolicyState) =>
  Effect.sync(() => {
    state.failureTimes = [];
    state.blockedUntil = Option.none();
  });
