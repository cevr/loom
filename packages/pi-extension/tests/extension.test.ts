import type { RegisteredCommand } from "@earendil-works/pi-coding-agent";
import { expect, it } from "bun:test";
import { Option } from "effect";
import loomExtension from "../src/index.js";

it("registers the Loom development command", () => {
  let command = Option.none<Omit<RegisteredCommand, "name" | "sourceInfo">>();
  let toolName = Option.none<string>();
  const pi = {
    on: (event: "session_start") => {
      expect(event).toBe("session_start");
    },
    registerCommand: (name: string, options: Omit<RegisteredCommand, "name" | "sourceInfo">) => {
      expect(name).toBe("loom");
      command = Option.some(options);
    },
    registerTool: (tool: { readonly name: string }) => {
      toolName = Option.some(tool.name);
    },
  };

  loomExtension(pi);

  expect(Option.getOrThrow(command).description).toBe("Show the Loom daemon state");
  expect(Option.getOrThrow(toolName)).toBe("loom_workflow");
});
