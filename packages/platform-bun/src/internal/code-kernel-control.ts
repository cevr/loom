/* oxlint-disable effect/noGlobals -- This process adapter derives the workspace of the kernel child. */
import { JobId, type SessionId, WorkspaceRoot } from "@cvr/loom-domain";
import { makeKernelFileControls } from "./code-kernel-file-control.js";
import { makeKernelRuntimeControls } from "./code-kernel-runtime-control.js";

export interface CodeKernelControl {
  readonly value: ReturnType<typeof makeKernelFileControls>["api"] &
    ReturnType<typeof makeKernelRuntimeControls>;
  readonly beginCell: (cellId: string) => void;
  readonly fileChanges: ReturnType<typeof makeKernelFileControls>["changes"];
}

export const makeCodeKernelControl = (sessionId: SessionId): CodeKernelControl => {
  const workspaceRoot = WorkspaceRoot.make(process.cwd());
  const files = makeKernelFileControls(workspaceRoot);
  let cellId = "cell";
  let jobIndex = 0;
  const runtime = makeKernelRuntimeControls({
    sessionId,
    workspaceRoot,
    nextJobId: () => {
      jobIndex += 1;
      return JobId.make(`${cellId}-${jobIndex}`);
    },
  });

  return {
    value: { ...files.api, ...runtime },
    beginCell: (nextCellId) => {
      cellId = nextCellId;
      jobIndex = 0;
      files.beginCell();
    },
    fileChanges: files.changes,
  };
};
