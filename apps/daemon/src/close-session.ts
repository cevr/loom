import { type SessionId } from "@cvr/loom-domain";
import { CloseSessionError } from "@cvr/loom-protocol";
import {
  JobRuntime,
  PluginStateStore,
  SessionLifecycle,
  WorkflowChildAgentStore,
  WorkflowRuntime,
} from "@cvr/loom-runtime";
import { Effect, Inspectable } from "effect";

export const makeCloseSession = Effect.gen(function* () {
  const workflows = yield* WorkflowRuntime;
  const childAgents = yield* WorkflowChildAgentStore;
  const jobs = yield* JobRuntime;
  const pluginState = yield* PluginStateStore;
  const sessions = yield* SessionLifecycle;
  return (sessionId: SessionId) =>
    sessions
      .close(
        sessionId,
        Effect.result(workflows.closeSession(sessionId)).pipe(
          Effect.flatMap((workflowResult) =>
            Effect.all(
              [
                Effect.result(
                  jobs
                    .closeSession(sessionId)
                    .pipe(Effect.andThen(childAgents.stopSession(sessionId))),
                ),
                Effect.result(pluginState.deleteSession(sessionId)),
              ],
              { concurrency: "unbounded" },
            ).pipe(
              Effect.flatMap(([ownedWorkResult, pluginStateResult]) =>
                Effect.fromResult(workflowResult).pipe(
                  Effect.andThen(Effect.fromResult(ownedWorkResult)),
                  Effect.andThen(Effect.fromResult(pluginStateResult)),
                ),
              ),
            ),
          ),
        ),
      )
      .pipe(
        Effect.mapError(
          (error) =>
            new CloseSessionError({
              sessionId,
              message: Inspectable.toStringUnknown(error),
            }),
        ),
      );
});
