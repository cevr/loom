import type { RegisteredCommand } from "@earendil-works/pi-coding-agent";
import { expect, it } from "bun:test";
import loomExtension from "../src/index.js";

it("registers the Loom development command", () => {
  let command: Omit<RegisteredCommand, "name" | "sourceInfo"> | undefined;
  const pi = {
    registerCommand: (name: string, options: Omit<RegisteredCommand, "name" | "sourceInfo">) => {
      expect(name).toBe("loom");
      command = options;
    },
  };

  loomExtension(pi);

  expect(command?.description).toBe("Show the Loom extension state");
});
