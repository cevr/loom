import { AgentId, CellId } from "@cvr/loom-domain";
import { EvaluateCellRequest } from "@cvr/loom-protocol";
import { Type } from "@earendil-works/pi-ai";
import { Effect } from "effect";
import type { LoomExtensionApi } from "./extension-api.js";
import { loomTool } from "./loom-tool-ui.js";
import { runLoomTool } from "./loom-tool.js";
import { toolResult } from "./tool-result.js";

const agentId = AgentId.make("pi");

const cellParameters = Type.Object({
  source: Type.String({ description: "TypeScript source for the persistent Code Kernel" }),
});

const registerEvaluateCell = (pi: LoomExtensionApi) =>
  pi.registerTool(
    loomTool({
      name: "loom_cell",
      label: "Loom Cell",
      description: "Evaluate TypeScript in this Agent's persistent Loom Code Kernel.",
      promptSnippet: "Use persistent TypeScript state through Loom",
      promptGuidelines: [
        "Use loom_cell for iterative code-mode work that benefits from persistent bindings.",
      ],
      parameters: cellParameters,
      execute: (toolCallId, parameters, signal, _onUpdate, context) =>
        runLoomTool(context, { signal }, "5 minutes", (client, sessionId) =>
          client
            .evaluateCell(
              EvaluateCellRequest.make({
                sessionId,
                agentId,
                cellId: CellId.make(toolCallId),
                source: parameters.source,
              }),
            )
            .pipe(
              Effect.map((result) => ({
                content: [{ type: "text", text: result.display }],
                details: { result },
              })),
            ),
        ),
    }),
  );

const registerResetCell = (pi: LoomExtensionApi) =>
  pi.registerTool(
    loomTool({
      name: "loom_cell_reset",
      label: "Reset Loom Cell State",
      description: "Replace this Agent's Loom Code Kernel and clear its bindings.",
      parameters: Type.Object({}),
      execute: (_toolCallId, _parameters, signal, _onUpdate, context) =>
        runLoomTool(context, { signal }, "5 seconds", (client, sessionId) =>
          client
            .resetCodeKernel({
              sessionId,
              agentId,
            })
            .pipe(Effect.as(toolResult("Code Kernel reset"))),
        ),
    }),
  );

export const registerCellTools = (pi: LoomExtensionApi): void => {
  registerEvaluateCell(pi);
  registerResetCell(pi);
};
