import { SessionId, type AgentOwner } from "@cvr/loom-domain";
import { Option } from "effect";

export type KernelOwner = AgentOwner;

const untrackedSessionId = SessionId.make("untracked");

export const ownerEnvironment = (owner: Option.Option<AgentOwner>) =>
  Option.match(owner, {
    onNone: () => ({ LOOM_SESSION_ID: untrackedSessionId }),
    onSome: (value) => ({
      LOOM_AGENT_ID: value.agentId,
      LOOM_SESSION_ID: value.sessionId,
    }),
  });
