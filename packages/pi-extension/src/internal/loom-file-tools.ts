import type { LoomExtensionApi } from "./extension-api.js";
import { createPrimeEditTool } from "./loom-edit-tool.js";
import { createPrimeWriteTool } from "./loom-write-tool.js";

export const registerPrimeFileTools = (pi: LoomExtensionApi): void => {
  pi.on("session_start", (_event, context) => {
    pi.registerTool(createPrimeEditTool(context.cwd));
    pi.registerTool(createPrimeWriteTool(context.cwd));
  });
};
