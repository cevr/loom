export {
  accountGoalUsage,
  blockGoal,
  clearGoal,
  completeGoal,
  GoalTransitionError,
  MAX_GOAL_OBJECTIVE_LENGTH,
  pauseGoal,
  resumeGoal,
  startGoal,
} from "@cvr/loom-domain";

import { GoalState } from "@cvr/loom-domain";
import { Option } from "effect";

export { GoalState };

export const goalStatusKey = "loom.goal";

type OpenGoal = Exclude<GoalState, { readonly _tag: "Cleared" }>;

const statusWithUsage = (label: string, state: OpenGoal) =>
  `${label}: ${state.objective}. ${Option.match(state.tokenBudget, {
    onNone: () => `${state.consumedTokens} input and output tokens used`,
    onSome: (value) => `${state.consumedTokens} of ${value} input and output tokens used`,
  })}.`;

export const goalStatus = Option.match({
  onNone: () => "No Goal is stored for this session.",
  onSome: GoalState.match({
    Active: (state) => statusWithUsage("Goal Active", state),
    Paused: (state) => statusWithUsage("Goal Paused", state),
    Completed: (state) => statusWithUsage("Goal Completed", state),
    Blocked: (state) => `${statusWithUsage("Goal Blocked", state)} ${state.reason}`,
    BudgetExhausted: (state) => statusWithUsage("Goal BudgetExhausted", state),
    Cleared: () => "The Goal is cleared.",
  }),
});

export const goalTrayStatus = Option.flatMap(
  GoalState.match({
    Active: (state) =>
      Option.some(
        Option.match(state.tokenBudget, {
          onNone: () => "Goal active",
          onSome: (limit) => `Goal active ${state.consumedTokens}/${limit}`,
        }),
      ),
    Paused: () => Option.some("Goal paused"),
    Completed: () => Option.none<string>(),
    Blocked: () => Option.none<string>(),
    BudgetExhausted: () => Option.some("Goal budget used"),
    Cleared: () => Option.none<string>(),
  }),
);
