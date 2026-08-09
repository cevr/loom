import { WorkspaceRoot } from "@cvr/loom-domain";
import { expect, it } from "bun:test";
import { layerNodeLoomClient } from "../src/index.js";

it("builds the Node client transport layer", () => {
  const layer = layerNodeLoomClient({
    workspaceRoot: WorkspaceRoot.make("/workspace"),
    socketPath: "/workspace/.loom/daemon.sock",
  });

  expect(layer).toBeDefined();
});
