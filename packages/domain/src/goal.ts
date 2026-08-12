import { Effect, Option, Schema } from "effect";
import { PluginId, PluginStateKey } from "./identifiers.js";

const positiveInteger = Schema.Int.check(Schema.isGreaterThan(0));
export const MAX_GOAL_OBJECTIVE_LENGTH = 4_000;
const objective = Schema.NonEmptyString.check(Schema.isMaxLength(MAX_GOAL_OBJECTIVE_LENGTH));
const budget = Schema.OptionFromNullOr(positiveInteger);
export const goalPluginId = PluginId.make("loom.goal");
export const goalStateKey = PluginStateKey.make("goal");

const goalFields = {
  objective,
  tokenBudget: budget,
  consumedTokens: Schema.Natural,
  revision: positiveInteger,
};

export const GoalState = Schema.TaggedUnion({
  Active: goalFields,
  Paused: goalFields,
  Completed: goalFields,
  Blocked: { ...goalFields, reason: Schema.NonEmptyString },
  BudgetExhausted: goalFields,
  Cleared: { revision: positiveInteger },
});
export type GoalState = typeof GoalState.Type;

export const GoalTransitionReason = Schema.Literals([
  "GoalAlreadyOpen",
  "GoalMissing",
  "GoalNotActive",
  "GoalNotPaused",
  "GoalNotCompletable",
]);
export type GoalTransitionReason = typeof GoalTransitionReason.Type;

export class GoalTransitionError extends Schema.TaggedError<GoalTransitionError>()(
  "GoalTransitionError",
  { reason: GoalTransitionReason, message: Schema.String },
) {}

const fail = (reason: GoalTransitionReason, message: string) =>
  Effect.fail(new GoalTransitionError({ reason, message }));

type OpenGoal = Exclude<GoalState, { readonly _tag: "Cleared" }>;

const goalData = (state: OpenGoal) => ({
  objective: state.objective,
  tokenBudget: state.tokenBudget,
  consumedTokens: state.consumedTokens,
});

export const startGoal = (
  current: Option.Option<GoalState>,
  nextRevision: number,
  goalObjective: string,
  tokenBudget: Option.Option<number>,
) => {
  if (
    Option.exists(
      current,
      (state) => GoalState.guards.Active(state) || GoalState.guards.Paused(state),
    )
  ) {
    return fail("GoalAlreadyOpen", "Clear or finish the current Goal before starting another.");
  }
  return Effect.succeed(
    GoalState.cases.Active.make({
      objective: goalObjective,
      tokenBudget,
      consumedTokens: 0,
      revision: nextRevision,
    }),
  );
};

export const pauseGoal = (current: GoalState, nextRevision: number) => {
  if (!GoalState.guards.Active(current)) {
    return fail("GoalNotActive", "Only an active Goal can be paused.");
  }
  return Effect.succeed(
    GoalState.cases.Paused.make({ ...goalData(current), revision: nextRevision }),
  );
};

export const resumeGoal = (current: GoalState, nextRevision: number) => {
  if (!GoalState.guards.Paused(current)) {
    return fail("GoalNotPaused", "Only a paused Goal can be resumed.");
  }
  return Effect.succeed(
    GoalState.cases.Active.make({ ...goalData(current), revision: nextRevision }),
  );
};

export const clearGoal = (nextRevision: number) =>
  Effect.succeed(GoalState.cases.Cleared.make({ revision: nextRevision }));

export const completeGoal = (current: GoalState, nextRevision: number) => {
  if (!GoalState.guards.Active(current) && !GoalState.guards.BudgetExhausted(current)) {
    return fail("GoalNotCompletable", "Only a running Goal can be completed.");
  }
  return Effect.succeed(
    GoalState.cases.Completed.make({ ...goalData(current), revision: nextRevision }),
  );
};

export const blockGoal = (current: GoalState, nextRevision: number, reason: string) => {
  if (!GoalState.guards.Active(current) && !GoalState.guards.BudgetExhausted(current)) {
    return fail("GoalNotCompletable", "Only a running Goal can be blocked.");
  }
  return Effect.succeed(
    GoalState.cases.Blocked.make({ ...goalData(current), reason, revision: nextRevision }),
  );
};

export const accountGoalUsage = (
  current: GoalState,
  nextRevision: number,
  tokens: number,
): GoalState => {
  if (!GoalState.guards.Active(current)) return current;
  const consumedTokens = current.consumedTokens + tokens;
  const fields = { ...goalData(current), consumedTokens, revision: nextRevision };
  if (Option.exists(current.tokenBudget, (value) => consumedTokens >= value)) {
    return GoalState.cases.BudgetExhausted.make(fields);
  }
  return GoalState.cases.Active.make(fields);
};
