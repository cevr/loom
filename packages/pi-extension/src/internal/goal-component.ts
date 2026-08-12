import { makePluginState, type PluginStateValue } from "@cvr/loom-client";
import { goalPluginId, goalStateKey, PluginStateScope, type SessionId } from "@cvr/loom-domain";
import { PluginStateRevision, PluginStateRevisionConflictError } from "@cvr/loom-protocol";
import { Effect, Option, Schema } from "effect";
import {
  accountGoalUsage,
  blockGoal,
  clearGoal,
  completeGoal,
  GoalState,
  GoalTransitionError,
  goalStatus,
  MAX_GOAL_OBJECTIVE_LENGTH,
  pauseGoal,
  resumeGoal,
  startGoal,
} from "./goal-state.js";
import { runWithLoomClient } from "./loom-connection.js";

export interface GoalStateGrant<E> {
  readonly read: Effect.Effect<Option.Option<PluginStateValue<GoalState>>, E>;
  readonly write: (
    state: GoalState,
    expectedRevision: Option.Option<PluginStateRevision>,
  ) => Effect.Effect<PluginStateRevision, E | PluginStateRevisionConflictError>;
}

export interface AgentTurnControl {
  readonly ready: Effect.Effect<boolean>;
  readonly request: (state: GoalState) => Effect.Effect<void>;
  readonly stop: Effect.Effect<void>;
}

export interface GoalClientActions {
  readonly showStatus: (message: string) => Effect.Effect<void>;
  readonly publish: (state: Option.Option<GoalState>) => Effect.Effect<void>;
}

export interface GoalComponentGrants<E> {
  readonly state: GoalStateGrant<E>;
  readonly turns: AgentTurnControl;
  readonly actions: GoalClientActions;
}

export class GoalCommandError extends Schema.TaggedError<GoalCommandError>()("GoalCommandError", {
  message: Schema.String,
}) {}

export const GoalCommand = Schema.TaggedUnion({
  Status: {},
  Pause: {},
  Resume: {},
  Clear: {},
  Start: {
    objective: Schema.NonEmptyString,
    tokenBudget: Schema.OptionFromNullOr(Schema.Int.check(Schema.isGreaterThan(0))),
  },
});
export type GoalCommand = typeof GoalCommand.Type;

export const makeGoalStateGrant = (cwd: string, sessionId: SessionId) => {
  const scope = PluginStateScope.cases.Session.make({ sessionId });
  return {
    read: runWithLoomClient(cwd, "5 seconds", (client) =>
      makePluginState(client, goalPluginId, scope, GoalState).read(goalStateKey),
    ),
    write: (state: GoalState, expectedRevision: Option.Option<PluginStateRevision>) =>
      runWithLoomClient(cwd, "5 seconds", (client) =>
        makePluginState(client, goalPluginId, scope, GoalState).write(
          goalStateKey,
          state,
          expectedRevision,
        ),
      ),
  };
};

const nextRevision = (current: Option.Option<PluginStateValue<GoalState>>) =>
  PluginStateRevision.make(
    Option.match(current, {
      onNone: () => 1,
      onSome: ({ revision }) => revision + 1,
    }),
  );

const currentState = (current: Option.Option<PluginStateValue<GoalState>>, message: string) =>
  Effect.gen(function* () {
    if (Option.isNone(current)) {
      return yield* new GoalTransitionError({ reason: "GoalMissing", message });
    }
    return current.value.value;
  });

const storedState = Option.map((entry: PluginStateValue<GoalState>) => entry.value);
const storedRevision = Option.map((entry: PluginStateValue<GoalState>) => entry.revision);
const retryRevisionConflict = Effect.retry({
  times: 3,
  while: (error) => error instanceof PluginStateRevisionConflictError,
});

const makeUpdate = <StateError>(state: GoalStateGrant<StateError>) =>
  Effect.fn("GoalComponent.update")(
    <E>(
      transition: (
        current: Option.Option<PluginStateValue<GoalState>>,
        revision: PluginStateRevision,
      ) => Effect.Effect<GoalState, E>,
    ) =>
      Effect.gen(function* () {
        const current = yield* state.read;
        const next = yield* transition(current, nextRevision(current));
        yield* state.write(next, storedRevision(current));
        return next;
      }).pipe(retryRevisionConflict),
  );

const makeRequestIfActive = <StateError>(
  state: GoalStateGrant<StateError>,
  turns: AgentTurnControl,
  actions: GoalClientActions,
) =>
  Effect.gen(function* () {
    const current = yield* state.read;
    yield* actions.publish(storedState(current));
    if (Option.isNone(current) || !GoalState.guards.Active(current.value.value)) return;
    if (!(yield* turns.ready)) return;
    yield* turns.request(current.value.value);
  });

const makeAccountUsage = <StateError>(
  state: GoalStateGrant<StateError>,
  turns: AgentTurnControl,
  actions: GoalClientActions,
) =>
  Effect.fn("GoalComponent.accountUsage")((tokens: number) =>
    Effect.gen(function* () {
      const current = yield* state.read;
      if (Option.isNone(current) || !GoalState.guards.Active(current.value.value)) return;
      const entry = current.value;
      const goal = accountGoalUsage(
        entry.value,
        PluginStateRevision.make(entry.revision + 1),
        tokens,
      );
      yield* state.write(goal, Option.some(entry.revision));
      yield* actions.publish(Option.some(goal));
      if (GoalState.guards.BudgetExhausted(goal)) {
        yield* actions.showStatus(goalStatus(Option.some(goal)));
        yield* turns.stop;
      }
    }).pipe(retryRevisionConflict),
  );

const makeShow = (actions: GoalClientActions) => (goal: Option.Option<GoalState>) =>
  Effect.all([actions.showStatus(goalStatus(goal)), actions.publish(goal)], {
    discard: true,
  });

export const makeGoalComponent = <StateError>(grants: GoalComponentGrants<StateError>) => {
  const { state, turns, actions } = grants;
  const update = makeUpdate(state);
  const show = makeShow(actions);
  const requestIfActive = makeRequestIfActive(state, turns, actions);

  return {
    start: (objective: string, tokenBudget: Option.Option<number>) =>
      update((current, revision) =>
        startGoal(storedState(current), revision, objective, tokenBudget),
      ).pipe(
        Effect.tap((goal) => show(Option.some(goal))),
        Effect.tap(() => requestIfActive),
      ),
    status: state.read.pipe(Effect.flatMap((current) => show(storedState(current)))),
    pause: update((current, revision) =>
      currentState(current, "No Goal is available to pause.").pipe(
        Effect.flatMap((goal) => pauseGoal(goal, revision)),
      ),
    ).pipe(
      Effect.tap((goal) => show(Option.some(goal))),
      Effect.tap(() => turns.stop),
    ),
    resume: update((current, revision) =>
      currentState(current, "No Goal is available to resume.").pipe(
        Effect.flatMap((goal) => resumeGoal(goal, revision)),
      ),
    ).pipe(
      Effect.tap((goal) => show(Option.some(goal))),
      Effect.tap(() => requestIfActive),
    ),
    clear: update((_current, revision) => clearGoal(revision)).pipe(
      Effect.tap((goal) => show(Option.some(goal))),
      Effect.tap(() => turns.stop),
    ),
    complete: update((current, revision) =>
      currentState(current, "No Goal is available to complete.").pipe(
        Effect.flatMap((goal) => completeGoal(goal, revision)),
      ),
    ).pipe(Effect.tap((goal) => show(Option.some(goal)))),
    block: (reason: string) =>
      update((current, revision) =>
        currentState(current, "No Goal is available to block.").pipe(
          Effect.flatMap((goal) => blockGoal(goal, revision, reason)),
        ),
      ).pipe(Effect.tap((goal) => show(Option.some(goal)))),
    accountUsage: makeAccountUsage(state, turns, actions),
    continueIfActive: requestIfActive,
  };
};

export const goalContinuationMessage = (state: GoalState): string => {
  if (!GoalState.guards.Active(state)) return "The Loom Goal is not active.";
  const budget = Option.match(state.tokenBudget, {
    onNone: () => "No token budget is set.",
    onSome: (value) => `${state.consumedTokens} of ${value} input and output tokens are used.`,
  });
  return `<loom_goal>\nContinue work on this active Goal:\n${state.objective}\n${budget}\nUse await loom.goal.complete() only after all requirements are complete. Use await loom.goal.block(reason) only after the same blocking condition stops progress three times.\n</loom_goal>`;
};

const parseGoalStart = (input: string): Effect.Effect<GoalCommand, GoalCommandError> => {
  const makeStart = (objective: string, tokenBudget: Option.Option<number>) => {
    if (objective.length > MAX_GOAL_OBJECTIVE_LENGTH) {
      return Effect.fail(
        new GoalCommandError({
          message: `The Goal objective must have at most ${MAX_GOAL_OBJECTIVE_LENGTH} characters.`,
        }),
      );
    }
    return Effect.succeed(GoalCommand.cases.Start.make({ objective, tokenBudget }));
  };
  const budgetPrefix = "--budget";
  if (
    input !== budgetPrefix &&
    !input.startsWith(`${budgetPrefix} `) &&
    !input.startsWith(`${budgetPrefix}=`)
  ) {
    return makeStart(input, Option.none());
  }

  const remainder = input.slice(budgetPrefix.length);
  let valueStart = remainder.trimStart();
  if (remainder.startsWith("=")) valueStart = remainder.slice(1);
  const separator = valueStart.search(/\s/u);
  if (separator < 1) {
    return Effect.fail(
      new GoalCommandError({ message: "Usage: /goal [--budget <tokens>] <objective>" }),
    );
  }
  const value = Number(valueStart.slice(0, separator));
  const objective = valueStart.slice(separator).trim();
  if (!Number.isSafeInteger(value) || value <= 0 || objective.length === 0) {
    return Effect.fail(
      new GoalCommandError({
        message: "The Goal budget must be a positive integer and the objective must not be empty.",
      }),
    );
  }
  return makeStart(objective, Option.some(value));
};

export const parseGoalCommand = (
  argumentsText: string,
): Effect.Effect<GoalCommand, GoalCommandError> => {
  const input = argumentsText.trim();
  switch (input.toLowerCase()) {
    case "":
    case "status":
      return Effect.succeed(GoalCommand.cases.Status.make({}));
    case "pause":
      return Effect.succeed(GoalCommand.cases.Pause.make({}));
    case "resume":
      return Effect.succeed(GoalCommand.cases.Resume.make({}));
    case "clear":
      return Effect.succeed(GoalCommand.cases.Clear.make({}));
    default:
      return parseGoalStart(input);
  }
};
