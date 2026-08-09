import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { Effect } from "effect";

export type LoomExtensionApi = Pick<ExtensionAPI, "registerCommand">;

export default function loomExtension(pi: LoomExtensionApi): void {
  pi.registerCommand("loom", {
    description: "Show the Loom extension state",
    handler: (_arguments, context) =>
      Effect.runPromise(
        Effect.sync(() =>
          context.ui.notify("Loom extension loaded. Use /reload after a source change.", "info"),
        ),
      ),
  });
}
