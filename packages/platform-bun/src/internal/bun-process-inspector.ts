import { ProcessIdentity } from "@cvr/loom-domain";
import {
  ProcessInspectionError,
  ProcessInspector,
  ProcessObservation,
  type ProcessInspectorShape,
} from "@cvr/loom-runtime";
import { Effect, Layer, Option, Schema } from "effect";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

const processLine = /^\s*(\d+)\s+(\d+)\s+(.+?)\s*$/u;
const decodeIdentity = Schema.decodeUnknownEffect(ProcessIdentity);

const parseProcess = Effect.fn("BunProcessInspector.parseProcess")(function* (
  output: string,
  pid: number,
) {
  const match = processLine.exec(output);
  if (match?.[1] !== String(pid)) return Option.none<ProcessIdentity>();
  return Option.some(
    yield* decodeIdentity({
      pid,
      processGroupId: Number(match[2]),
      processStartId: match[3],
    }),
  );
});

export const makeBunProcessInspector: Effect.Effect<
  ProcessInspectorShape,
  never,
  ChildProcessSpawner.ChildProcessSpawner
> = Effect.gen(function* () {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const inspect = Effect.fn("BunProcessInspector.inspect")(
    function* (pid: number) {
      const output = yield* spawner.string(
        ChildProcess.make("ps", ["-o", "pid=,pgid=,lstart=", "-p", String(pid)], {
          env: { LC_ALL: "C" },
          extendEnv: true,
          detached: false,
        }),
      );
      const identity = yield* parseProcess(output, pid);
      return Option.match(identity, {
        onNone: () => ProcessObservation.Missing({ pid }),
        onSome: (foundIdentity) => ProcessObservation.Found({ identity: foundIdentity }),
      });
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
