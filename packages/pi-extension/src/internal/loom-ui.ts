import type { LoomExtensionApi } from "./extension-api.js";
import { registerLoomEditor } from "./loom-editor.js";
import { type EnsureLoomDaemon } from "./loom-connection.js";
import { registerLoomInterface } from "./loom-interface.js";
import { registerLoomTheme } from "./loom-theme.js";

export const registerLoomUi = (pi: LoomExtensionApi, ensureDaemon: EnsureLoomDaemon): void => {
  registerLoomTheme(pi);
  registerLoomEditor(pi);
  registerLoomInterface(pi, ensureDaemon);
};
