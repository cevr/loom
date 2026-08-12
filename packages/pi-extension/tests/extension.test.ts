import type { ExtensionAPI, RegisteredCommand } from "@earendil-works/pi-coding-agent";
import { expect, it } from "bun:test";
import { Effect, Option } from "effect";
import { TUI_KEYBINDINGS } from "@earendil-works/pi-tui";
import loomExtension, { type LoomExtensionApi, shouldCloseSession } from "../src/index.js";
import { runTool } from "../src/internal/tool-result.js";
import { activateLoomModel, loomModelTools } from "../src/internal/cell-tools.js";

const makeRegisterTool = (toolNames: Set<string>): ExtensionAPI["registerTool"] => {
  const registerTool: ExtensionAPI["registerTool"] = (tool) => {
    toolNames.add(tool.name);
    expect(tool.renderShell).toBe("self");
    expect(tool.renderCall).toBeFunction();
    expect(tool.renderResult).toBeFunction();
  };
  return registerTool;
};

it("registers the Loom development command", () => {
  let command = Option.none<Omit<RegisteredCommand, "name" | "sourceInfo">>();
  const commandNames = new Set<string>();
  const toolNames = new Set<string>();
  const events = new Map<string, number>();
  let activeTools: ReadonlyArray<string> = [];
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
    setActiveTools: (names: string[]) => {
      activeTools = names;
    },
  };

  loomExtension(pi);

  expect(Option.getOrThrow(command).description).toBe("Show the Loom daemon state");
  expect(commandNames).toEqual(new Set(["loom", "loom-reset", "btw", "side", "goal"]));
  expect(toolNames).toEqual(new Set(loomModelTools));
  expect(events).toEqual(
    new Map([
      ["session_start", 5],
      ["session_shutdown", 2],
      ["message_end", 1],
      ["agent_settled", 1],
    ]),
  );
  expect(activeTools).toEqual([]);
});

it("uses Pi input bindings for multiline prompts", () => {
  expect(TUI_KEYBINDINGS["tui.input.newLine"].defaultKeys).toContain("shift+enter");
  expect(TUI_KEYBINDINGS["tui.input.submit"].defaultKeys).toBe("enter");
});

it("activates only the Loom Cell model tool", () => {
  let activeTools: ReadonlyArray<string> = [];
  activateLoomModel({
    setActiveTools: (names) => {
      activeTools = names;
    },
  });
  expect(activeTools).toEqual(["loom_cell"]);
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
