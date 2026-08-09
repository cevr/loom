import { AgentId, CellId, SessionId } from "@cvr/loom-domain";
import { Schema } from "effect";

export class EvaluateCellRequest extends Schema.Class<EvaluateCellRequest>(
  "@cvr/loom-protocol/EvaluateCellRequest",
)({
  sessionId: SessionId,
  agentId: AgentId,
  cellId: CellId,
  source: Schema.String,
}) {}
