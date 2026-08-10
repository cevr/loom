import { expect, it } from "bun:test";
import { Option } from "effect";
import { DaemonUnavailableError, MessageTooLargeError } from "../src/index.js";

it("keeps daemon connection context", () => {
  const error = new DaemonUnavailableError({
    operation: "handshake",
    socketPath: "/workspace/.loom/daemon.sock",
    reason: "ConnectionTimeout",
    cause: Option.none(),
  });

  expect(error.operation).toBe("handshake");
  expect(error.socketPath).toBe("/workspace/.loom/daemon.sock");
  expect(error.reason).toBe("ConnectionTimeout");
});

it("keeps the frame size boundary", () => {
  const error = new MessageTooLargeError({
    operation: "evaluateCell",
    size: 101,
    maximum: 100,
  });

  expect(error.size).toBe(101);
  expect(error.maximum).toBe(100);
});
