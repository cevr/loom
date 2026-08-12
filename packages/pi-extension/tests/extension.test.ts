import type { ExtensionAPI, RegisteredCommand } from "@earendil-works/pi-coding-agent";
import { defaultWorkflowBudget, JobId, SessionId } from "@cvr/loom-domain";
import {
  ReadJobOutputRequest,
  StartJobRequest,
  WaitForJobRequest,
  workflowCapabilitiesGuide,
  workflowSignalsGuide,
  workflowSourceGuide,
} from "@cvr/loom-protocol";
import { expect, it } from "bun:test";
import { Effect, Option } from "effect";
import { TUI_KEYBINDINGS } from "@earendil-works/pi-tui";
import loomExtension, { type LoomExtensionApi, shouldCloseSession } from "../src/index.js";
import {
  readJobOutputRequest,
  startJobRequest,
  waitForJobRequest,
} from "../src/internal/job-tools.js";
import { runTool } from "../src/internal/tool-result.js";
import { workflowRequest } from "../src/internal/workflow-tools.js";

const workflowInput = {
  name: "release",
  version: "1",
  key: "release-1",
  source: "return input",
  input: "{}",
};

const makeRegisterTool = (toolNames: Set<string>): ExtensionAPI["registerTool"] => {
  const registerTool: ExtensionAPI["registerTool"] = (tool) => {
    toolNames.add(tool.name);
    if (tool.name !== "loom_workflow_start") return;
    expect(tool.parameters).toHaveProperty("properties.source.description", workflowSourceGuide);
    expect(tool.parameters).toHaveProperty(
      "properties.capabilities.description",
      workflowCapabilitiesGuide,
    );
    expect(tool.parameters).toHaveProperty("properties.signals.description", workflowSignalsGuide);
  };
  return registerTool;
};

it("registers the Loom development command", () => {
  let command = Option.none<Omit<RegisteredCommand, "name" | "sourceInfo">>();
  const commandNames = new Set<string>();
  const toolNames = new Set<string>();
  const events = new Map<string, number>();
  const on: LoomExtensionApi["on"] = (event) => {
    events.set(event, (events.get(event) ?? 0) + 1);
  };
  const pi = {
    on,
    registerCommand: (name: string, options: Omit<RegisteredCommand, "name" | "sourceInfo">) => {
      commandNames.add(name);
      if (name === "loom") command = Option.some(options);
    },
    registerTool: makeRegisterTool(toolNames),
    sendMessage: () => {},
  };

  loomExtension(pi);

  expect(Option.getOrThrow(command).description).toBe("Show the Loom daemon state");
  expect(commandNames).toEqual(new Set(["loom", "btw", "side", "goal"]));
  expect(toolNames).toEqual(
    new Set([
      "loom_cell",
      "loom_cell_reset",
      "loom_job_start",
      "loom_job_inspect",
      "loom_job_output",
      "loom_job_await",
      "loom_job_cancel",
      "loom_job_detach",
      "loom_workflow_start",
      "loom_workflow_inspect",
      "loom_workflow_signal",
      "loom_workflow_interrupt",
      "loom_workflow_compensation",
      "loom_goal_complete",
      "loom_goal_blocked",
    ]),
  );
  expect(events).toEqual(
    new Map([
      ["session_start", 4],
      ["session_shutdown", 2],
      ["message_end", 1],
      ["agent_settled", 1],
    ]),
  );
});

it("uses Pi input bindings for multiline prompts", () => {
  expect(TUI_KEYBINDINGS["tui.input.newLine"].defaultKeys).toContain("shift+enter");
  expect(TUI_KEYBINDINGS["tui.input.submit"].defaultKeys).toBe("enter");
});

it("keeps the Session active during extension reload", () => {
  expect(shouldCloseSession({ type: "session_shutdown", reason: "reload" })).toBe(false);
  expect(shouldCloseSession({ type: "session_shutdown", reason: "quit" })).toBe(true);
});

it("uses the domain Workflow Budget defaults", () =>
  expect(Effect.runPromise(workflowRequest("session-1", workflowInput))).resolves.toHaveProperty(
    "budget",
    defaultWorkflowBudget,
  ));

it("resolves a partial Workflow Budget through the domain schema", () =>
  expect(
    Effect.runPromise(
      workflowRequest("session-1", {
        ...workflowInput,
        budget: { maxSteps: 12, maxTokens: 1_000 },
      }),
    ),
  ).resolves.toHaveProperty("budget", {
    ...defaultWorkflowBudget,
    maxSteps: 12,
    maxTokens: Option.some(1_000),
  }));

it("uses the protocol Job request defaults", () =>
  expect(
    Effect.runPromise(
      Effect.all([
        startJobRequest("session-1", "job-1", { command: "true" }),
        waitForJobRequest("session-1", { jobId: "job-1" }),
        readJobOutputRequest("session-1", { jobId: "job-1", stream: "stdout" }),
      ]),
    ),
  ).resolves.toEqual([
    StartJobRequest.make({
      sessionId: SessionId.make("session-1"),
      jobId: JobId.make("job-1"),
      command: "true",
    }),
    WaitForJobRequest.make({
      sessionId: SessionId.make("session-1"),
      jobId: JobId.make("job-1"),
    }),
    ReadJobOutputRequest.make({
      sessionId: SessionId.make("session-1"),
      jobId: JobId.make("job-1"),
      stream: "stdout",
    }),
  ]));

it("reports a typed Loom tool failure to Pi", () =>
  expect(
    runTool(Effect.fail({ _tag: "AdapterProbeError", detail: "preserved" }), {
      signal: new AbortController().signal,
    }),
  ).rejects.toThrow(/AdapterProbeError.*preserved/u));
