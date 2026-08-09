import { AgentId, SessionId } from "@cvr/loom-domain";
import { Rpc } from "effect/unstable/rpc";

export class ResetCodeKernel extends Rpc.make("CodeKernel.Reset", {
  payload: {
    sessionId: SessionId,
    agentId: AgentId,
  },
}) {}
