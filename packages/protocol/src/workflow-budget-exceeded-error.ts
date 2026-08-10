import { Schema } from "effect";

export const WorkflowBudgetName = Schema.Literals([
  "Steps",
  "Agents",
  "InlineResultBytes",
  "Tokens",
  "Duration",
]);
export type WorkflowBudgetName = typeof WorkflowBudgetName.Type;

export class WorkflowBudgetExceededError extends Schema.TaggedError<WorkflowBudgetExceededError>()(
  "WorkflowBudgetExceededError",
  {
    budget: WorkflowBudgetName,
    limit: Schema.Natural,
    actual: Schema.Natural,
  },
) {}
