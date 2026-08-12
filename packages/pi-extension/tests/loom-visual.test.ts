import { visibleWidth } from "@earendil-works/pi-tui";
import { expect, it } from "bun:test";
import { Option } from "effect";
import { promptEditorFrame } from "../src/internal/loom-editor.js";
import { renderLoomEditRow } from "../src/internal/loom-edit-tool.js";
import { LoomDaemonView, renderLoomHeader } from "../src/internal/loom-splash.js";
import { type LoomToolPanelView, renderLoomToolPanel } from "../src/internal/loom-tool-ui.js";
import { renderLoomTray } from "../src/internal/loom-tray.js";

const theme = {
  bg: (_color: string, text: string) => text,
  bold: (text: string) => text,
  fg: (_color: string, text: string) => text,
};

const headerAt = (width: number) =>
  renderLoomHeader(
    {
      daemon: LoomDaemonView.cases.Ready.make({
        protocolVersion: 13,
        idleLeaseMillis: 300_000,
      }),
      actors: [],
    },
    { model: "GPT-5.6 Luna", cwd: "/Users/cvr/Developer/personal/loom" },
    width,
    theme,
  );

const runningToolAt = (width: number) =>
  renderLoomToolPanel(
    {
      label: "Start Loom Job",
      status: "running",
      input: '{ "command": "bun test", "foregroundLeaseMillis": 1000 }',
      output: Option.none(),
      frame: 1,
      expanded: false,
      expandHint: "Ctrl+O to expand",
    },
    width,
    theme,
  );

const completedToolAt = (width: number) =>
  renderLoomToolPanel(
    {
      label: "Read Loom Job Output",
      status: "done",
      input: '{ "jobId": "job-12345678", "stream": "stdout" }',
      output: Option.some(
        Array.from({ length: 16 }, (_, index) => `output ${index + 1}`).join("\n"),
      ),
      frame: 0,
      expanded: false,
      expandHint: "Ctrl+O to expand",
    },
    width,
    theme,
  );

const trayAt = (width: number) =>
  renderLoomTray(
    {
      model: "GPT-5.6 Luna",
      thinkingLevel: Option.some("medium"),
      showShortcutHint: false,
      goal: Option.some("Goal active 240/4000"),
      usage: Option.some({ tokens: 14_200, percent: 5 }),
      actors: [],
    },
    width,
    theme,
  );

const editedFileAt = (width: number) =>
  renderLoomEditRow(
    {
      path: "src/release.ts",
      status: "done",
      expanded: false,
      expandHint: "Ctrl+O to expand",
      diff: Option.some("-4 const ready = false;\n+4 const ready = true;"),
      error: Option.none(),
    },
    width,
    theme,
  );

const surfaceAt = (width: number) => {
  const border = "─".repeat(width);
  const editor = promptEditorFrame(
    [border, "   Continue the workflow".padEnd(width), border],
    border,
    width,
    3,
  );

  return [
    ...headerAt(width),
    "",
    ...runningToolAt(width),
    "",
    ...completedToolAt(width),
    "",
    ...editedFileAt(width),
    "",
    ...editor,
    ...trayAt(width),
  ]
    .map((line) => line.replaceAll("\x1b[0m", ""))
    .map((line) => `${visibleWidth(line).toString().padStart(3)} │${line}│`)
    .join("\n");
};

it("keeps the Loom interface readable at narrow, medium, and wide terminal widths", () => {
  expect(
    [
      `NARROW 40\n${surfaceAt(40)}`,
      `MEDIUM 72\n${surfaceAt(72)}`,
      `WIDE 120\n${surfaceAt(120)}`,
    ].join("\n\n"),
  ).toMatchSnapshot();
});

it("bounds every tool row and marks truncated content", () => {
  const view = {
    label: "A Loom tool label that exceeds the terminal width",
    status: "error",
    input: "input",
    output: Option.some(Array.from({ length: 20 }, (_, index) => `output ${index + 1}`).join("\n")),
    frame: 0,
    expanded: false,
    expandHint: "Ctrl+O to expand",
  } satisfies LoomToolPanelView;

  for (const width of [1, 2, 3, 24, 40, 72, 120]) {
    expect(
      renderLoomToolPanel(view, width, theme).every((line) => visibleWidth(line) === width),
    ).toBe(true);
  }

  const lines = renderLoomToolPanel(view, 24, theme);
  expect(lines[0]).toContain("…");
  expect(lines.at(-1)).toContain("more lines");
});

it("shows full tool output only when expanded", () => {
  const output = Array.from({ length: 20 }, (_, index) => `output ${index + 1}`).join("\n");
  const view = {
    label: "Read Loom Job Output",
    status: "done",
    input: "{}",
    output: Option.some(output),
    frame: 0,
    expanded: false,
    expandHint: "Ctrl+O to expand",
  } satisfies LoomToolPanelView;

  expect(renderLoomToolPanel(view, 80, theme).join("\n")).not.toContain("output 20");
  expect(renderLoomToolPanel({ ...view, expanded: true }, 80, theme).join("\n")).toContain(
    "output 20",
  );
});

it("renders a collapsed Prime-style file edit row", () => {
  const row = editedFileAt(72).join("\n");

  expect(row).toContain("edit src/release.ts");
  expect(row).toContain("+1 -1");
  expect(row).toContain("to expand");
});

it("renders the collapse action for an expanded file edit row", () => {
  const row = renderLoomEditRow(
    {
      path: "src/release.ts",
      status: "done",
      expanded: true,
      expandHint: "Ctrl+O to collapse",
      diff: Option.none(),
      error: Option.none(),
    },
    72,
    theme,
  ).join("\n");

  expect(row).toContain("to collapse");
});

it("renders the queued tool state", () => {
  expect(
    renderLoomToolPanel(
      {
        label: "Start Loom Job",
        status: "queued",
        input: "{}",
        output: Option.none(),
        frame: 0,
        expanded: false,
        expandHint: "Ctrl+O to expand",
      },
      40,
      theme,
    )[0],
  ).toContain("queued");
});
