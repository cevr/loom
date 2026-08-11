import { CustomEditor, type KeybindingsManager } from "@earendil-works/pi-coding-agent";
import type { EditorTheme, TUI } from "@earendil-works/pi-tui";
import type { LoomExtensionApi } from "./extension-api.js";

export const loomEditorPadding = 2;

class LoomEditor extends CustomEditor {
  constructor(tui: TUI, theme: EditorTheme, keybindings: KeybindingsManager) {
    super(tui, theme, keybindings, { paddingX: loomEditorPadding });
  }

  override setPaddingX(padding: number): void {
    super.setPaddingX(Math.max(loomEditorPadding, padding));
  }
}

export const registerLoomEditor = (pi: LoomExtensionApi): void => {
  pi.on("session_start", (_event, context) => {
    context.ui.setEditorComponent(
      (tui, theme, keybindings) => new LoomEditor(tui, theme, keybindings),
    );
  });
};
