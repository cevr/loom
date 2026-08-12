import { SessionId } from "@cvr/loom-domain";
import { Type } from "@earendil-works/pi-ai";
import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import { Effect, Semaphore } from "effect";
import type { LoomExtensionApi } from "./extension-api.js";
import {
  GoalCommand,
  goalContinuationMessage,
  makeGoalComponent,
  makeGoalStateGrant,
  parseGoalCommand,
} from "./goal-component.js";
import { runTool, toolResult } from "./tool-result.js";

const componentFor = (pi: LoomExtensionApi, context: ExtensionContext) =>
  makeGoalComponent({
    state: makeGoalStateGrant(context.cwd, SessionId.make(context.sessionManager.getSessionId())),
    turns: {
      ready: Effect.sync(() => context.isIdle() && !context.hasPendingMessages()),
      request: (state) =>
        Effect.sync(() =>
          pi.sendMessage(
            {
              customType: "loom_goal",
              content: goalContinuationMessage(state),
              display: true,
              details: state,
            },
            { triggerTurn: true, deliverAs: "followUp" },
          ),
        ),
      stop: Effect.sync(() => context.abort()),
    },
    actions: {
      showStatus: (message) => Effect.sync(() => context.ui.notify(message, "info")),
    },
  });

const notifyFailure = (context: ExtensionContext, cause: unknown) =>
  Effect.sync(() => context.ui.notify(`Loom Goal failed: ${String(cause)}`, "error"));

type RunExclusive = <A, E>(effect: Effect.Effect<A, E>) => Effect.Effect<A, E>;

const runEvent = <A, E>(
  context: ExtensionContext,
  runExclusive: RunExclusive,
  effect: Effect.Effect<A, E>,
) =>
  runExclusive(effect).pipe(
    Effect.asVoid,
    Effect.catchCause((cause) => notifyFailure(context, cause)),
    Effect.runPromise,
  );

const executeCommand = (
  pi: LoomExtensionApi,
  runExclusive: RunExclusive,
  argumentsText: string,
  context: ExtensionContext,
) =>
  runExclusive(
    Effect.gen(function* () {
      const command = yield* parseGoalCommand(argumentsText);
      const component = componentFor(pi, context);
      return yield* GoalCommand.match(command, {
        Status: () => component.status,
        Pause: () => Effect.asVoid(component.pause),
        Resume: () => Effect.asVoid(component.resume),
        Clear: () => Effect.asVoid(component.clear),
        Start: ({ objective, tokenBudget }) =>
          Effect.asVoid(component.start(objective, tokenBudget)),
      });
    }),
  ).pipe(
    Effect.asVoid,
    Effect.catchCause((cause) => notifyFailure(context, cause)),
    Effect.runPromise,
  );

const registerGoalTools = (pi: LoomExtensionApi, runExclusive: RunExclusive) => {
  pi.registerTool({
    name: "loom_goal_complete",
    label: "Complete Loom Goal",
    description: "Mark the active Loom Goal complete after all requirements are verified.",
    parameters: Type.Object({}),
    execute: (_toolCallId, _parameters, signal, _onUpdate, context) =>
      runTool(
        runExclusive(
          componentFor(pi, context).complete.pipe(
            Effect.as(toolResult("The Loom Goal is complete")),
          ),
        ),
        { signal },
      ),
  });

  pi.registerTool({
    name: "loom_goal_blocked",
    label: "Block Loom Goal",
    description:
      "Mark the active Loom Goal blocked after the same blocker stops progress three times.",
    parameters: Type.Object({ reason: Type.String({ minLength: 1 }) }),
    execute: (_toolCallId, parameters, signal, _onUpdate, context) =>
      runTool(
        runExclusive(
          componentFor(pi, context)
            .block(parameters.reason)
            .pipe(Effect.as(toolResult("The Loom Goal is blocked"))),
        ),
        { signal },
      ),
  });
};

export const registerGoal = (pi: LoomExtensionApi): void => {
  const semaphore = Semaphore.makeUnsafe(1);
  const accountedAssistantMessages = new WeakSet<object>();
  const runExclusive: RunExclusive = (effect) => semaphore.withPermits(1)(effect);
  pi.registerCommand("goal", {
    description: "Start or control a durable Loom Goal",
    handler: (argumentsText, context) => executeCommand(pi, runExclusive, argumentsText, context),
  });

  registerGoalTools(pi, runExclusive);

  pi.on("session_start", (_event, context) =>
    runEvent(context, runExclusive, componentFor(pi, context).continueIfActive),
  );
  pi.on("message_end", (event, context) => {
    if (event.message.role !== "assistant") return;
    if (
      event.message.stopReason !== "stop" &&
      event.message.stopReason !== "length" &&
      event.message.stopReason !== "toolUse"
    ) {
      return;
    }
    if (accountedAssistantMessages.has(event.message)) return;
    accountedAssistantMessages.add(event.message);
    return runEvent(
      context,
      runExclusive,
      componentFor(pi, context).accountUsage(
        event.message.usage.input + event.message.usage.output,
      ),
    );
  });
  pi.on("agent_settled", (_event, context) =>
    runEvent(context, runExclusive, componentFor(pi, context).continueIfActive),
  );
};
