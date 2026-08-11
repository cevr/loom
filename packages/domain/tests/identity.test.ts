import { describe, expect, it } from "effect-bun-test";
import { Effect, Exit, Schema } from "effect";
import {
  AgentOwner,
  AgentParent,
  WorkflowBudget,
  WorkflowActivityKey,
  WorkflowName,
  WorkflowRunRequest,
  defaultWorkflowBudget,
  workflowAgentId,
  workflowArtifactId,
  workflowJobId,
} from "../src/index.js";

describe("Loom identity", () => {
  it.effect("decodes an agent owner", () =>
    Effect.gen(function* () {
      const owner = yield* Schema.decodeUnknownEffect(AgentOwner)({
        sessionId: "session-1",
        agentId: "agent-1",
      });

      expect(String(owner.sessionId)).toBe("session-1");
      expect(String(owner.agentId)).toBe("agent-1");
    }),
  );

  it.effect("rejects an empty identifier", () =>
    Effect.gen(function* () {
      const exit = yield* Schema.decodeUnknownEffect(AgentOwner)({
        sessionId: "",
        agentId: "agent-1",
      }).pipe(Effect.exit);

      expect(Exit.isFailure(exit)).toBe(true);
    }),
  );
});

describe("Workflow Budget", () => {
  it.effect("owns the default Workflow Budget", () =>
    Effect.gen(function* () {
      const decoded = yield* Schema.decodeUnknownEffect(WorkflowBudget)({});
      const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(WorkflowBudget))(decoded);

      expect(decoded).toEqual(defaultWorkflowBudget);
      expect(encoded).toBe(
        '{"maxSteps":32,"maxAgentRuns":8,"maxParallelism":4,"maxInlineStepResultBytes":65536,"maxTokens":null,"maxDurationMillis":null}',
      );
    }),
  );

  it.effect("keeps resolved defaults beside a budget override", () =>
    Effect.gen(function* () {
      const budget = yield* Schema.decodeUnknownEffect(WorkflowBudget)({ maxSteps: 12 });
      const encoded = yield* Schema.encodeEffect(WorkflowBudget)(budget);
      const encodedDefault = yield* Schema.encodeEffect(WorkflowBudget)(defaultWorkflowBudget);

      expect(WorkflowBudget.make({ maxSteps: 12 })).toEqual(budget);
      expect(budget).toEqual({ ...defaultWorkflowBudget, maxSteps: 12 });
      expect(encoded).toEqual({ ...encodedDefault, maxSteps: 12 });
    }),
  );
});

describe("Workflow identity", () => {
  it.effect("decodes a Workflow Run request with JSON input", () =>
    Effect.gen(function* () {
      const request = yield* Schema.decodeUnknownEffect(WorkflowRunRequest)({
        sessionId: "session-1",
        key: "daily-review",
        definition: {
          name: "ReviewRepository",
          version: "1",
          interpreterVersion: 1,
          source: "return await agent('review', { stepId: 'review' })",
          capabilities: ["agent.spawn"],
          signals: ["approval"],
        },
        input: { path: "/workspace" },
        budget: {
          maxSteps: 20,
          maxAgentRuns: 8,
          maxParallelism: 4,
          maxInlineStepResultBytes: 65_536,
          maxTokens: 100_000,
          maxDurationMillis: 300_000,
        },
      });

      expect(request.definition.name).toBe(WorkflowName.make("ReviewRepository"));
      expect(request.input).toEqual({ path: "/workspace" });
    }),
  );

  it.effect("models a Workflow Run as an Agent parent", () =>
    Effect.gen(function* () {
      const parent = yield* Schema.decodeUnknownEffect(AgentParent)({
        _tag: "WorkflowRun",
        sessionId: "session-1",
        workflowRunId: "workflow-run-1",
      });

      expect(parent).toHaveProperty("_tag", "WorkflowRun");
    }),
  );
});

describe("Workflow capability identity", () => {
  it.effect("derives distinct identities from one Activity key", () =>
    Effect.sync(() => {
      const activityKey = WorkflowActivityKey.make("workflow/step");

      expect(String(workflowAgentId(activityKey))).toBe("workflow-agent:workflow/step");
      expect(String(workflowJobId(activityKey))).toBe("workflow-job:workflow/step");
      expect(String(workflowArtifactId(activityKey))).toBe("workflow-artifact:workflow/step");
    }),
  );
});
