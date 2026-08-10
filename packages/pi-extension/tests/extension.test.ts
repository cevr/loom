import type { RegisteredCommand } from "@earendil-works/pi-coding-agent";
import { expect, it } from "bun:test";
import { Effect, Option } from "effect";
import loomExtension, { shouldCloseSession } from "../src/index.js";
import { runTool } from "../src/internal/tool-result.js";

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
    registerTool: (tool: { readonly name: string }) => {
      toolNames.add(tool.name);
    },
  };

  loomExtension(pi);

  expect(Option.getOrThrow(command).description).toBe("Show the Loom daemon state");
  expect(toolNames).toEqual(
    new Set([
      "loom_cell",
      "loom_cell_reset",
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
