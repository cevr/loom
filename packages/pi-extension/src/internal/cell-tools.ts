import { AgentId, CellId } from "@cvr/loom-domain";
import { type CellEvaluation, EvaluateCellRequest } from "@cvr/loom-protocol";
import { Type } from "@earendil-works/pi-ai";
import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import type { ToolDefinition } from "@earendil-works/pi-coding-agent";
import { Effect, Option } from "effect";
import type { LoomExtensionApi } from "./extension-api.js";
import { runLoomTool } from "./loom-tool.js";
import { toolResult } from "./tool-result.js";
import { cellResultText } from "./loom-cell-file-ui.js";
import { type CellRendererState, renderCellCall, renderCellResult } from "./loom-cell-ui.js";

const agentId = AgentId.make("pi");
export const loomModelTools = ["loom_cell"] satisfies ReadonlyArray<string>;
export const activateLoomModel = (pi: Pick<LoomExtensionApi, "setActiveTools">): void =>
  pi.setActiveTools([...loomModelTools]);

const cellParameters = Type.Object({
  source: Type.String({ description: "TypeScript source for the persistent Code Kernel" }),
});

const cellDefinition: ToolDefinition<typeof cellParameters, CellEvaluation, CellRendererState> = {
  name: "loom_cell",
  label: "Loom Cell",
  description:
    "Execute TypeScript in the persistent Loom Code Kernel. The global loom object provides file, Job, Workflow, and Goal control.",
  promptSnippet: "Use the persistent TypeScript Loom Cell for all computer interaction",
  promptGuidelines: [
    "Use loom_cell for all file, search, shell, and persistent code work.",
    "Use await loom.read(path, { offset, limit }), loom.find(glob), and loom.grep(text, glob) to inspect files.",
    "Use await loom.write(path, content) and loom.edit(path, oldText, newText) to change files.",
    "Use await loom.run(command, { foregroundLeaseMillis, attached }) for shell commands. It starts a durable Job and returns when the command ends or its foreground lease expires.",
    "Use loom.jobs.inspect, output, wait, cancel, and detach to control durable Jobs.",
    "Use loom.workflows to start, inspect, signal, interrupt, or compensate durable Workflows.",
    "Use loom.goal.complete() or loom.goal.block(reason) to finish an active Goal.",
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
            details: result,
          })),
        ),
    ),
  renderShell: "self",
  renderCall: (args, theme, context) => {
    let source = "";
    if (context.argsComplete) source = args.source;
    return renderCellCall(source, theme, context);
  },
  renderResult: (result, options, theme, context) => {
    let evaluation = Option.none<CellEvaluation>();
    if (!options.isPartial && !context.isError) {
      evaluation = Option.fromNullishOr(result.details);
    }
    let error = Option.none<string>();
    if (context.isError) error = Option.some(cellResultText(result.content));
    return renderCellResult(
      context.args.source,
      evaluation,
      error,
      options.expanded,
      theme,
      context,
    );
  },
};

const resetCell = (context: ExtensionContext) =>
  runLoomTool(context, {}, "5 seconds", (client, sessionId) =>
    client
      .resetCodeKernel({
        sessionId,
        agentId,
      })
      .pipe(Effect.as(toolResult("Code Kernel reset"))),
  ).then(() => context.ui.notify("Loom Code Kernel reset", "info"));

export const registerCellTools = (pi: LoomExtensionApi): void => {
  pi.registerTool(cellDefinition);
  pi.registerCommand("loom-reset", {
    description: "Reset the persistent Loom Code Kernel",
    handler: (_arguments, context) => resetCell(context),
  });
  pi.on("session_start", () => activateLoomModel(pi));
};
