import { type Duration, Effect, Option } from "effect";
import { RpcClientError } from "effect/unstable/rpc";
import {
  DaemonUnavailableError,
  type DaemonUnavailableReason,
} from "./daemon-unavailable-error.js";
import type { LoomClientShape } from "./loom-client.js";
import type { LoomRpcClientConfig } from "./loom-rpc-client-config.js";

export const unavailable = (
  config: LoomRpcClientConfig,
  operation: string,
  reason: DaemonUnavailableReason,
  cause: Option.Option<unknown>,
) => new DaemonUnavailableError({ socketPath: config.socketPath, operation, reason, cause });

export const withTimeout = <A, E, R>(
  config: LoomRpcClientConfig,
  operation: string,
  reason: DaemonUnavailableReason,
  effect: Effect.Effect<A, E, R>,
  duration: Duration.Input,
) =>
  effect.pipe(
    Effect.timeoutOrElse({
      duration,
      orElse: () => Effect.fail(unavailable(config, operation, reason, Option.none())),
    }),
  );

export const makeDaemonRequest = <Request, Success, Error, Requirements>(
  name: string,
  operation: string,
  config: LoomRpcClientConfig,
  runHandshake: LoomClientShape["handshake"],
  send: (
    request: Request,
  ) => Effect.Effect<Success, Error | RpcClientError.RpcClientError, Requirements>,
  timeoutFor?: (request: Request) => Duration.Input,
) =>
  Effect.fn(name)((request: Request) =>
    withTimeout(
      config,
      operation,
      "RequestTimeout",
      runHandshake.pipe(
        Effect.flatMap(() => send(request)),
        Effect.catchTag("RpcClientError", (cause) =>
          Effect.fail(unavailable(config, operation, "TransportFailure", Option.some(cause))),
        ),
      ),
      timeoutFor?.(request) ?? config.requestTimeout ?? "10 seconds",
    ),
  );
