const workingFrameMillis = 250;

export type LoomCellStatus = "queued" | "running" | "done" | "error";

interface CellStatusContext {
  readonly isError: boolean;
  readonly isPartial: boolean;
  readonly executionStarted: boolean;
  readonly invalidate: () => void;
  readonly state: {
    interval?: ReturnType<typeof setInterval>;
    startedAt?: number;
  };
}

export const cellCallStatus = (context: CellStatusContext): LoomCellStatus => {
  if (context.isError) return "error";
  if (!context.executionStarted) return "queued";
  if (context.isPartial) return "running";
  return "done";
};

export const cellResultStatus = (
  context: Pick<CellStatusContext, "isError" | "isPartial">,
): LoomCellStatus => {
  if (context.isError) return "error";
  if (context.isPartial) return "running";
  return "done";
};

export const updateWorkingIndicator = (
  context: CellStatusContext,
  status: LoomCellStatus,
): void => {
  if (status === "running") {
    // Pi renderers run outside the Effect runtime.
    // oxlint-disable-next-line effect/noGlobals
    context.state.startedAt ??= Date.now();
    // oxlint-disable-next-line effect/noGlobals
    context.state.interval ??= setInterval(context.invalidate, workingFrameMillis);
  } else if (context.state.interval) {
    clearInterval(context.state.interval);
    delete context.state.interval;
  }
};

export const workingFrameIndex = (context: CellStatusContext, status: LoomCellStatus): number => {
  if (status !== "running" || !context.state.startedAt) return 0;
  // oxlint-disable-next-line effect/noGlobals
  return Math.floor((Date.now() - context.state.startedAt) / workingFrameMillis);
};
