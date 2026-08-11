import {
  ActorActivity,
  ActorStateProjection,
  ActorSubject,
  AgentId,
  JobId,
  SessionId,
  WorkflowRunId,
} from "@cvr/loom-domain";
import { visibleWidth } from "@earendil-works/pi-tui";
import { expect, it } from "bun:test";
import { renderLoomHeader, type LoomInterfaceView } from "../src/internal/loom-interface.js";

const theme = {
  bold: (text: string) => text,
  fg: (_color: string, text: string) => text,
};

const sessionId = SessionId.make("session-tui");
const actors = [
  ActorStateProjection.make({
    subject: ActorSubject.cases.Agent.make({ sessionId, agentId: AgentId.make("pi") }),
    activity: ActorActivity.cases.Idle.make({}),
    revision: 1,
  }),
  ActorStateProjection.make({
    subject: ActorSubject.cases.Job.make({
      sessionId,
      jobId: JobId.make("build-12345678"),
    }),
    activity: ActorActivity.cases.Working.make({ message: "running tests" }),
    revision: 1,
  }),
  ActorStateProjection.make({
    subject: ActorSubject.cases.WorkflowRun.make({
      sessionId,
      workflowRunId: WorkflowRunId.make("release-87654321"),
    }),
    activity: ActorActivity.cases.Blocked.make({ message: "approval" }),
    revision: 1,
  }),
];

const ready = {
  daemon: { _tag: "Ready", idleLeaseMillis: 300_000 },
  actors,
} satisfies LoomInterfaceView;

it("renders a compact wide Loom header with active actor identities", () => {
  expect(renderLoomHeader(ready, 120, theme)).toEqual([
    "loom · daemon ready · lease 5m",
    "actor idle · job 12345678 working: running tests · workflow 87654321 blocked: approval",
  ]);
});

it("removes actor identities and truncates state at a narrow width", () => {
  const lines = renderLoomHeader(ready, 42, theme);

  expect(lines[0]).toBe("loom · daemon ready · lease 5m");
  expect(lines[1]).toStartWith("actor idle · job working · workflow");
  expect(lines.every((line) => visibleWidth(line) <= 42)).toBe(true);
});

it("shows a bounded daemon failure without stale actor state", () => {
  const lines = renderLoomHeader(
    {
      daemon: { _tag: "Failed", message: "socket closed\nwhile reading actor state" },
      actors: [],
    },
    32,
    theme,
  );

  expect(lines).toHaveLength(1);
  expect(lines[0]).toStartWith("loom · daemon failed: socket cl");
  expect(visibleWidth(lines[0] ?? "")).toBe(32);
  expect(lines[0]).not.toContain("\n");
});

it("shows the daemon connection state", () => {
  expect(renderLoomHeader({ daemon: { _tag: "Connecting" }, actors: [] }, 80, theme)).toEqual([
    "loom · daemon connecting",
  ]);
});
