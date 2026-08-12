import type { LoomExtensionApi } from "./extension-api.js";
import { makeLoomEditorState, registerLoomEditor } from "./loom-editor.js";
import { registerPrimeEditTool } from "./loom-edit-tool.js";
import { type EnsureLoomDaemon } from "./loom-connection.js";
import { registerLoomInterface } from "./loom-interface.js";
import { registerLoomTheme } from "./loom-theme.js";

export const registerLoomUi = (pi: LoomExtensionApi, ensureDaemon: EnsureLoomDaemon): void => {
  const editorState = makeLoomEditorState();
  registerLoomTheme(pi);
  registerPrimeEditTool(pi);
  registerLoomEditor(pi, editorState);
  registerLoomInterface(pi, ensureDaemon, editorState);
};
