import { Schema } from "effect";

export const WorkflowCompensationDecision = Schema.Literals(["Retry", "Stop"]);
export type WorkflowCompensationDecision = typeof WorkflowCompensationDecision.Type;
