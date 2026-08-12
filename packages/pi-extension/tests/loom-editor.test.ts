import { expect, it } from "bun:test";
import { promptEditorFrame } from "../src/internal/loom-editor.js";

it("renders a prompt mark without changing the editor width or autocomplete rows", () => {
  expect(
    promptEditorFrame(["────────", "   build", "────────", "   /btw"], "────────", 8, 3),
  ).toEqual(["        ", " > build", "        ", "   /btw"]);
});
