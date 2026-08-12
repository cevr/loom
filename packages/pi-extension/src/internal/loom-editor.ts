import { CustomEditor, type KeybindingsManager } from "@earendil-works/pi-coding-agent";
import type { EditorTheme, Keybinding, TUI } from "@earendil-works/pi-tui";
import { MutableRef, Option } from "effect";
import type { LoomExtensionApi } from "./extension-api.js";

export const loomEditorPadding = 3;

export interface LoomEditorState {
  readonly inputEmpty: MutableRef.MutableRef<boolean>;
}

export const makeLoomEditorState = (): LoomEditorState => ({
  inputEmpty: MutableRef.make(true),
});

export const quietEditorFrame = (
  lines: ReadonlyArray<string>,
  border: string,
  width: number,
): ReadonlyArray<string> =>
  lines.map((line) => {
    if (line === border) return " ".repeat(width);
    return line;
  });

export const promptEditorFrame = (
  lines: ReadonlyArray<string>,
  border: string,
  width: number,
  padding: number,
): Array<string> => {
  const framed = [...quietEditorFrame(lines, border, width)];
  if (padding < 3) return framed;
  const input = Option.fromNullishOr(framed[1]);
  if (Option.isNone(input)) return framed;
  const inset = " ".repeat(padding);
  if (!input.value.startsWith(inset)) return framed;
  framed[1] = ` > ${" ".repeat(padding - 3)}${input.value.slice(padding)}`;
  return framed;
};

export class LoomEditor extends CustomEditor {
  private readonly requestRender: () => void;

  constructor(
    tui: TUI,
    theme: EditorTheme,
    keybindings: KeybindingsManager,
    private readonly viewState: LoomEditorState,
    private readonly showShortcuts: () => void,
  ) {
    super(tui, theme, keybindings, { paddingX: loomEditorPadding });
    this.requestRender = () => tui.requestRender();
  }

  override setPaddingX(padding: number): void {
    super.setPaddingX(Math.max(loomEditorPadding, padding));
  }

  override setText(text: string): void {
    super.setText(text);
    this.setInputEmpty(text.length === 0);
  }

  override handleInput(data: string): void {
    if (data === "?" && this.getText().length === 0) {
      this.showShortcuts();
      return;
    }
    super.handleInput(data);
    this.setInputEmpty(this.getText().length === 0);
  }

  override render(width: number): string[] {
    const border = this.borderColor("─").repeat(width);
    return promptEditorFrame(super.render(width), border, width, this.getPaddingX());
  }

  private setInputEmpty(empty: boolean): void {
    if (MutableRef.get(this.viewState.inputEmpty) === empty) return;
    MutableRef.set(this.viewState.inputEmpty, empty);
    this.requestRender();
  }
}

const shortcutSummary = (keybindings: KeybindingsManager) => {
  const shortcuts: ReadonlyArray<readonly [Keybinding, string]> = [
    ["tui.input.newLine", "new line"],
    ["app.model.select", "models"],
    ["app.tools.expand", "tool output"],
    ["app.thinking.toggle", "thinking"],
    ["app.message.followUp", "follow-up"],
  ];
  return shortcuts
    .map(([action, label]) => `${keybindings.getKeys(action).join("/")} ${label}`)
    .join(" · ");
};

export const registerLoomEditor = (pi: LoomExtensionApi, state: LoomEditorState): void => {
  pi.on("session_start", (_event, context) => {
    MutableRef.set(state.inputEmpty, context.ui.getEditorText().length === 0);
    context.ui.setEditorComponent(
      (tui, theme, keybindings) =>
        new LoomEditor(tui, theme, keybindings, state, () =>
          context.ui.notify(shortcutSummary(keybindings), "info"),
        ),
    );
  });
};
