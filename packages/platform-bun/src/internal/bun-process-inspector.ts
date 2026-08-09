import { ProcessIdentity } from "@cvr/loom-domain";
import {
  ProcessInspectionError,
  ProcessInspector,
  ProcessObservation,
  type ProcessInspectorShape,
} from "@cvr/loom-runtime";
import { Effect, Layer, Schema } from "effect";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

const processLine = /^\s*(\d+)\s+(\d+)\s+(.+?)\s*$/u;
const decodeIdentity = Schema.decodeUnknownEffect(ProcessIdentity);

const parseProcessTable = Effect.fn("BunProcessInspector.parseProcessTable")(function* (
  output: string,
  pid: number,
) {
  for (const line of output.split("\n")) {
    const match = processLine.exec(line);
    if (match?.[1] !== String(pid)) continue;
    return yield* decodeIdentity({
      pid,
      processGroupId: Number(match[2]),
      processStartId: match[3],
    });
  }
  return null;
});

export const makeBunProcessInspector: Effect.Effect<
  ProcessInspectorShape,
  never,
  ChildProcessSpawner.ChildProcessSpawner
> = Effect.gen(function* () {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const command = ChildProcess.make("ps", ["-axo", "pid=,pgid=,lstart="], {
    env: { LC_ALL: "C" },
    extendEnv: true,
    detached: false,
  });

  const inspect = Effect.fn("BunProcessInspector.inspect")(
    function* (pid: number) {
      const output = yield* spawner.string(command);
      const identity = yield* parseProcessTable(output, pid);
      if (identity === null) return ProcessObservation.Missing({ pid });
      return ProcessObservation.Found({ identity });
    },
    (effect, pid) =>
      effect.pipe(Effect.mapError((cause) => new ProcessInspectionError({ pid, cause }))),
  );

  return ProcessInspector.of({ inspect });
});

export const layerBunProcessInspector: Layer.Layer<
  ProcessInspector,
  never,
  ChildProcessSpawner.ChildProcessSpawner
> = Layer.effect(ProcessInspector, makeBunProcessInspector);
