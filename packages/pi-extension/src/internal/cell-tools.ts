import { AgentId, CellId, SessionId } from "@cvr/loom-domain";
import { EvaluateCellRequest } from "@cvr/loom-protocol";
import { Type } from "@earendil-works/pi-ai";
import type { AgentToolResult } from "@earendil-works/pi-coding-agent";
import { Effect } from "effect";
import type { LoomExtensionApi } from "./extension-api.js";
import { runWithLoomClient } from "./loom-connection.js";
import { runTool, toolResult } from "./tool-result.js";

const agentId = AgentId.make("pi");

const cellParameters = Type.Object({
  source: Type.String({ description: "TypeScript source for the persistent Code Kernel" }),
});

const registerEvaluateCell = (pi: LoomExtensionApi) =>
  pi.registerTool({
    name: "loom_cell",
    label: "Loom Cell",
    description: "Evaluate TypeScript in this Agent's persistent Loom Code Kernel.",
    promptSnippet: "Use persistent TypeScript state through Loom",
    promptGuidelines: [
      "Use loom_cell for iterative code-mode work that benefits from persistent bindings.",
    ],
    parameters: cellParameters,
    execute: (toolCallId, parameters, signal, _onUpdate, context) =>
      runTool(
        runWithLoomClient(context.cwd, "5 minutes", (client) =>
          client
            .evaluateCell(
              EvaluateCellRequest.make({
                sessionId: SessionId.make(context.sessionManager.getSessionId()),
                agentId,
                cellId: CellId.make(toolCallId),
                source: parameters.source,
              }),
            )
            .pipe(
              Effect.map(
                (result) =>
                  ({
                    content: [{ type: "text", text: result.display }],
                    details: { result },
                  }) satisfies AgentToolResult<{ readonly result: typeof result }>,
              ),
            ),
        ),
        { signal },
      ),
  });

const registerResetCell = (pi: LoomExtensionApi) =>
  pi.registerTool({
    name: "loom_cell_reset",
    label: "Reset Loom Cell State",
    description: "Replace this Agent's Loom Code Kernel and clear its bindings.",
    parameters: Type.Object({}),
    execute: (_toolCallId, _parameters, signal, _onUpdate, context) =>
      runTool(
        runWithLoomClient(context.cwd, "5 seconds", (client) =>
          client
            .resetCodeKernel({
              sessionId: SessionId.make(context.sessionManager.getSessionId()),
              agentId,
            })
            .pipe(Effect.as(toolResult("Code Kernel reset"))),
        ),
        { signal },
      ),
  });

export const registerCellTools = (pi: LoomExtensionApi): void => {
  registerEvaluateCell(pi);
  registerResetCell(pi);
};
