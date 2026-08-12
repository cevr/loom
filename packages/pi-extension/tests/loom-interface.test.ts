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
import { Option } from "effect";
import {
  renderLoomHeader,
  type LoomHeaderDetails,
  type LoomInterfaceView,
} from "../src/internal/loom-splash.js";
import { renderLoomTray } from "../src/internal/loom-tray.js";

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
  daemon: { _tag: "Ready", protocolVersion: 1, idleLeaseMillis: 300_000 },
  actors,
} satisfies LoomInterfaceView;

const details = {
  model: "GPT-5.6 Luna",
  cwd: "/Users/cvr/Developer/personal/loom",
} satisfies LoomHeaderDetails;

it("renders a responsive Loom splash with runtime metadata", () => {
  const lines = renderLoomHeader(ready, details, 120, theme);

  expect(lines).toHaveLength(6);
  expect(lines.join("\n")).toContain("status   ready");
  expect(lines.join("\n")).toContain("protocol v1");
  expect(lines.join("\n")).toContain("model    GPT-5.6 Luna");
  expect(lines.join("\n")).toContain("cwd      /Users/cvr/Developer/personal/loom");
  expect(lines.join("\n")).toContain('Try "build and test @<filepath>"');
  expect(lines.every((line) => visibleWidth(line) === 120)).toBe(true);
});

it("uses one bounded status line at a narrow width", () => {
  const lines = renderLoomHeader(ready, details, 28, theme);

  expect(lines).toHaveLength(1);
  expect(lines[0]).toStartWith("loom · daemon ready");
  expect(lines.every((line) => visibleWidth(line) <= 28)).toBe(true);
});

it("shows a bounded daemon failure without stale actor state", () => {
  const lines = renderLoomHeader(
    {
      daemon: { _tag: "Failed", message: "socket closed\nwhile reading actor state" },
      actors: [],
    },
    details,
    32,
    theme,
  );

  expect(lines).toHaveLength(1);
  expect(lines[0]).toStartWith("loom · daemon failed");
  expect(visibleWidth(lines[0] ?? "")).toBe(32);
  expect(lines[0]).not.toContain("\n");
});

it("shows the daemon connection state", () => {
  const lines = renderLoomHeader(
    { daemon: { _tag: "Connecting" }, actors: [] },
    details,
    80,
    theme,
  );

  expect(lines.join("\n")).toContain("status   connecting");
});

it("renders the Prime-style model and context tray", () => {
  const lines = renderLoomTray(
    {
      model: "GPT-5.6 Luna",
      thinkingLevel: Option.some("medium"),
      showShortcutHint: true,
      goal: Option.some("Goal active 240/1000"),
      usage: Option.some({ tokens: 5_300, percent: 2 }),
      actors: [],
    },
    100,
    theme,
  );

  expect(lines).toHaveLength(1);
  expect(lines[0]).toStartWith("GPT-5.6 Luna • medium • ? for shortcuts");
  expect(lines[0]).toEndWith("Goal active 240/1000 · 5.3k (2%)");
  expect(visibleWidth(lines[0] ?? "")).toBe(100);
});

it("adds only active actor state below the tray", () => {
  const lines = renderLoomTray(
    {
      model: "GPT-5.6 Luna",
      thinkingLevel: Option.none(),
      showShortcutHint: false,
      goal: Option.none(),
      usage: Option.none(),
      actors,
    },
    120,
    theme,
  );

  expect(lines).toEqual([
    "GPT-5.6 Luna".padEnd(120),
    " job 12345678 working: running tests · workflow 87654321 blocked: approval",
  ]);
});
