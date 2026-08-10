import { AgentId, CellId, SessionId } from "@cvr/loom-domain";
import { Schema } from "effect";

export class CellIdentityConflictError extends Schema.TaggedError<CellIdentityConflictError>()(
  "CellIdentityConflictError",
  {
    sessionId: SessionId,
    agentId: AgentId,
    cellId: CellId,
  },
) {}
