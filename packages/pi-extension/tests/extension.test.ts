import type { ExtensionAPI, RegisteredCommand } from "@earendil-works/pi-coding-agent";
import {
  workflowCapabilitiesGuide,
  workflowSignalsGuide,
  workflowSourceGuide,
} from "@cvr/loom-protocol";
import { expect, it } from "bun:test";
import { Effect, Option } from "effect";
import loomExtension, { shouldCloseSession } from "../src/index.js";
import { runTool } from "../src/internal/tool-result.js";

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
  const toolNames = new Set<string>();
  const events = new Set<string>();
  const pi = {
    on: (event: "session_start" | "session_shutdown") => {
      events.add(event);
    },
    registerCommand: (name: string, options: Omit<RegisteredCommand, "name" | "sourceInfo">) => {
      expect(name).toBe("loom");
      command = Option.some(options);
    },
    registerTool: makeRegisterTool(toolNames),
  };

  loomExtension(pi);

  expect(Option.getOrThrow(command).description).toBe("Show the Loom daemon state");
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
    ]),
  );
  expect(events).toEqual(new Set(["session_start", "session_shutdown"]));
});

it("keeps the Session active during extension reload", () => {
  expect(shouldCloseSession({ type: "session_shutdown", reason: "reload" })).toBe(false);
  expect(shouldCloseSession({ type: "session_shutdown", reason: "quit" })).toBe(true);
});

it("reports a typed Loom tool failure to Pi", () =>
  expect(
    runTool(Effect.fail({ _tag: "AdapterProbeError", detail: "preserved" }), {
      signal: new AbortController().signal,
    }),
  ).rejects.toThrow(/AdapterProbeError.*preserved/u));
