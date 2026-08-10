import { WorkspaceRoot } from "@cvr/loom-domain";
import {
  currentProtocolVersion,
  maximumFrameSize,
  WorkspaceMismatchError,
} from "@cvr/loom-protocol";
import { expect, it } from "effect-bun-test";
import { Effect } from "effect";
import { makeConnectionHandshake } from "../src/index.js";

const workspaceRoot = WorkspaceRoot.make("/workspace");
const handshake = makeConnectionHandshake({
  workspaceRoot,
  daemonStartedAtMillis: 1234,
});

it.effect("selects the highest common protocol version", () =>
  Effect.gen(function* () {
    const result = yield* handshake.handshake({
      workspaceRoot,
      minimumProtocolVersion: 1,
      maximumProtocolVersion: currentProtocolVersion,
    });

    expect(result).toEqual({
      workspaceRoot,
      protocolVersion: currentProtocolVersion,
      maximumFrameSize,
      daemonStartedAtMillis: 1234,
    });
  }),
);

it.effect("rejects another Workspace", () =>
  Effect.gen(function* () {
    const error = yield* handshake
      .handshake({
        workspaceRoot: WorkspaceRoot.make("/other"),
        minimumProtocolVersion: 1,
        maximumProtocolVersion: 1,
      })
      .pipe(Effect.flip);

    expect(error).toBeInstanceOf(WorkspaceMismatchError);
  }),
);

it.effect("rejects a client with no common protocol version", () =>
  Effect.gen(function* () {
    const error = yield* handshake
      .handshake({
        workspaceRoot,
        minimumProtocolVersion: currentProtocolVersion + 1,
        maximumProtocolVersion: currentProtocolVersion + 2,
      })
      .pipe(Effect.flip);

    expect(error).toHaveProperty("_tag", "IncompatibleProtocolError");
  }),
);
