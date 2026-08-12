import { visibleWidth } from "@earendil-works/pi-tui";
import { expect, it } from "bun:test";
import { Option } from "effect";
import { CellEvaluation, CellFileChange } from "@cvr/loom-protocol";
import { promptEditorFrame } from "../src/internal/loom-editor.js";
import { LoomDaemonView, renderLoomHeader } from "../src/internal/loom-splash.js";
import { renderLoomTray } from "../src/internal/loom-tray.js";
import { cellResultStatus } from "../src/internal/loom-cell-status.js";
import { type LoomCellView, renderLoomCell } from "../src/internal/loom-cell-ui.js";
import { CellId } from "@cvr/loom-domain";

const theme = {
  bg: (_color: string, text: string) => text,
  bold: (text: string) => text,
  fg: (_color: string, text: string) => text,
};

const cell = (
  source: string,
  display: string,
  fileChanges: ReadonlyArray<CellFileChange> = [],
): LoomCellView => ({
  source,
  status: "done",
  frame: 0,
  expanded: false,
  showExpandHint: true,
  expandHint: "Ctrl+O",
  evaluation: Option.some(
    CellEvaluation.make({
      cellId: CellId.make("cell-1"),
      display,
      bindings: ["loom"],
      durationMillis: 8,
      fileChanges,
    }),
  ),
  error: Option.none<string>(),
  cwd: "/workspace",
});

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
  renderLoomCell(
    {
      ...cell('await loom.run("bun test", { foregroundLeaseMillis: 1000 })', ""),
      status: "running",
      frame: 1,
      evaluation: Option.none(),
    },
    width,
    theme,
  );

const completedToolAt = (width: number) =>
  renderLoomCell(
    cell(
      'await loom.jobs.output("job-12345678")',
      Array.from({ length: 16 }, (_, index) => `output ${index + 1}`).join("\n"),
    ),
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
  renderLoomCell(
    cell('await loom.edit("src/release.ts", "false", "true")', "Edited src/release.ts", [
      CellFileChange.make({
        path: "src/release.ts",
        oldText: "const ready = false;\n",
        newText: "const ready = true;\n",
      }),
    ]),
    width,
    theme,
  );

const writtenFileAt = (width: number) =>
  renderLoomCell(
    cell('await loom.write("src/generated.ts", content)', "Wrote src/generated.ts", [
      CellFileChange.make({
        path: "src/generated.ts",
        oldText: "",
        newText: Array.from({ length: 18 }, (_, index) => `line ${index + 1}`).join("\n"),
      }),
    ]),
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
    ...writtenFileAt(width),
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

it("bounds every Cell row and marks truncated content", () => {
  const view = {
    ...cell(
      'await loom.run("a command that exceeds every narrow terminal width by a large amount")',
      "",
    ),
    status: "error",
  } satisfies LoomCellView;

  for (const width of [1, 2, 3, 24, 40, 72, 120]) {
    expect(renderLoomCell(view, width, theme).every((line) => visibleWidth(line) <= width)).toBe(
      true,
    );
  }

  expect(renderLoomCell(view, 24, theme)[0]).not.toContain("amount");
});

it("shows full Cell output only when expanded", () => {
  const output = Array.from({ length: 20 }, (_, index) => `output ${index + 1}`).join("\n");
  const view = cell('await loom.jobs.output("job-1")', output);

  expect(renderLoomCell(view, 80, theme).join("\n")).not.toContain("output 20");
  expect(renderLoomCell({ ...view, expanded: true }, 80, theme).join("\n")).toContain("output 20");
});

it("renders queued and animated Cell states", () => {
  const view = cell('await loom.run("bun test")', "");
  expect(renderLoomCell({ ...view, status: "queued" }, 40, theme)[0]).toContain("◇");
  expect(renderLoomCell({ ...view, status: "running", frame: 1 }, 40, theme)[0]).toContain("◈");
});

it("keeps completed Cell results complete after Pi reloads history", () => {
  expect(cellResultStatus({ isError: false, isPartial: false })).toBe("done");
  expect(cellResultStatus({ isError: false, isPartial: true })).toBe("running");
  expect(cellResultStatus({ isError: true, isPartial: false })).toBe("error");
});

it("renders a Cell before Pi completes its source argument", () => {
  const row = renderLoomCell(cell("", ""), 40, theme);
  expect(row).toHaveLength(1);
  expect(row[0]).toContain("typescript");
});

it("matches Prime-style read, find, and grep Cell rows", () => {
  const rows = [
    cell('await loom.read("src/sample.ts", { offset: 40, limit: 5 })', "0041 line"),
    cell('await loom.find("**/*.ts")', '["src/sample.ts"]'),
    cell('await loom.grep("line_0042", "**/*.ts")', '[{ path: "src/sample.ts" }]'),
  ].map((view) => renderLoomCell(view, 80, theme).join("\n"));

  expect(rows[0]).toContain("typescript · read src/sample.ts");
  expect(rows[1]).toContain("typescript · find **/*.ts");
  expect(rows[2]).toContain("typescript · grep line_0042");
});

it("keeps Cell edits collapsed with one file summary", () => {
  const row = renderLoomCell(
    cell(
      'await loom.edit("src/sample.ts", "const ready = false", "const ready = true")',
      "Edited src/sample.ts",
      [
        CellFileChange.make({
          path: "src/sample.ts",
          oldText: "const ready = false;\n",
          newText: "const ready = true;\n",
        }),
      ],
    ),
    72,
    theme,
  ).join("\n");

  expect(row).toContain("typescript · edit src/sample.ts");
  expect(row).toContain("╰─ src/sample.ts +1 -1");
  expect(row).not.toContain("const ready = false");
});

it("bounds a 5000-line write preview at every terminal width", () => {
  const content = Array.from(
    { length: 5_000 },
    (_, index) => `export const line_${index} = ${index};`,
  ).join("\n");
  const view = cell('await loom.write("src/large.ts", content)', "Wrote src/large.ts", [
    CellFileChange.make({ path: "src/large.ts", oldText: "", newText: content }),
  ]);

  for (const width of [40, 72, 120]) {
    const rows = renderLoomCell(view, width, theme);
    expect(rows).toHaveLength(2);
    expect(rows.every((line) => visibleWidth(line) <= width)).toBe(true);
    expect(rows.join("\n")).toContain("+5000 -0");
    expect(rows.join("\n")).not.toContain("line_4999");
  }
});
