import { Schema } from "effect";
import { WorkflowBudgetExceededError } from "./workflow-budget-exceeded-error.js";
import { WorkflowCapabilityDeniedError } from "./workflow-capability-denied-error.js";
import { WorkflowDuplicateStepError } from "./workflow-duplicate-step-error.js";
import { WorkflowInterpreterVersionMismatchError } from "./workflow-interpreter-version-mismatch-error.js";
import { WorkflowSourceError } from "./workflow-source-error.js";
import { WorkflowStepError } from "./workflow-step-error.js";
import { WorkflowSignalNotDeclaredError } from "./workflow-signal-not-declared-error.js";

export const WorkflowRunError = Schema.Union([
  WorkflowSourceError,
  WorkflowStepError,
  WorkflowBudgetExceededError,
  WorkflowCapabilityDeniedError,
  WorkflowDuplicateStepError,
  WorkflowSignalNotDeclaredError,
  WorkflowInterpreterVersionMismatchError,
]);
export type WorkflowRunError = typeof WorkflowRunError.Type;
