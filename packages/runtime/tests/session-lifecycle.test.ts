import { SessionId } from "@cvr/loom-domain";
import { SessionClosingError } from "@cvr/loom-protocol";
import { expect, it } from "effect-bun-test";
import { Deferred, Effect, Fiber, Layer, Ref } from "effect";
import { TestClock } from "effect/testing";
import {
  layerSessionLifecycle,
  makeSessionLifecycle,
  SessionClosureStore,
  SessionLifecycle,
} from "../src/index.js";
import {
  makeTestSessionClosureStore,
  makeTestSessionLifecycle,
} from "./session-closure-test-support.js";

const sessionId = SessionId.make("session-lifecycle");

const makeLifecycle = (closures: SessionClosureStore["Service"]) =>
  makeSessionLifecycle({ closureLease: "5 minutes" }).pipe(
    Effect.provideService(SessionClosureStore, closures),
    Effect.scoped,
  );

it.effect("admits Session work concurrently", () =>
  Effect.gen(function* () {
    const sessions = yield* makeTestSessionLifecycle;
    const firstStarted = yield* Deferred.make<true>();
    const secondStarted = yield* Deferred.make<true>();
    const release = yield* Deferred.make<true>();
    const start = (started: Deferred.Deferred<true>) =>
      sessions.admit(
        sessionId,
        Deferred.succeed(started, true).pipe(Effect.andThen(Deferred.await(release))),
      );

    const fibers = yield* Effect.all([start(firstStarted), start(secondStarted)], {
      concurrency: "unbounded",
    }).pipe(Effect.forkChild({ startImmediately: true }));

    yield* Effect.all([Deferred.await(firstStarted), Deferred.await(secondStarted)], {
      concurrency: "unbounded",
    });
    yield* Deferred.succeed(release, true);
    yield* Fiber.join(fibers);
  }),
);

it.effect("reads unknown Session closure state without allocation", () =>
  Effect.gen(function* () {
    const closures = yield* makeTestSessionClosureStore;
    const sessions = yield* makeLifecycle(closures);
    const results = yield* Effect.forEach(
      Array.from({ length: 1_000 }, (_, index) => SessionId.make(`unknown-session-${index}`)),
      sessions.isClosing,
      { concurrency: "unbounded" },
    );

    expect(results.every((closing) => !closing)).toBe(true);
    expect(yield* closures.list).toEqual([]);
  }),
);

it.effect("rejects work after Session close starts", () =>
  Effect.gen(function* () {
    const sessions = yield* makeTestSessionLifecycle;
    const admitted = yield* Deferred.make<true>();
    const release = yield* Deferred.make<true>();
    const cleaned = yield* Ref.make(false);
    const lateWorkStarted = yield* Ref.make(false);
    const active = yield* sessions
      .admit(
        sessionId,
        Deferred.succeed(admitted, true).pipe(Effect.andThen(Deferred.await(release))),
      )
      .pipe(Effect.forkChild({ startImmediately: true }));

    yield* Deferred.await(admitted);
    const closing = yield* sessions
      .close(sessionId, Ref.set(cleaned, true))
      .pipe(Effect.forkChild({ startImmediately: true }));
    const late = yield* sessions
      .admit(sessionId, Ref.set(lateWorkStarted, true))
      .pipe(Effect.forkChild({ startImmediately: true }));

    expect(yield* Ref.get(cleaned)).toBe(false);
    expect(yield* Ref.get(lateWorkStarted)).toBe(false);
    yield* Deferred.succeed(release, true);
    yield* Fiber.join(active);
    yield* Fiber.join(closing);
    const error = yield* Fiber.join(late).pipe(Effect.flip);

    expect(error).toBeInstanceOf(SessionClosingError);
    expect(yield* Ref.get(cleaned)).toBe(true);
    expect(yield* Ref.get(lateWorkStarted)).toBe(false);
  }),
);

it.effect("accepts repeated Session close", () =>
  Effect.gen(function* () {
    const sessions = yield* makeTestSessionLifecycle;

    yield* sessions.close(sessionId, Effect.void);
    yield* sessions.close(sessionId, Effect.void);

    const error = yield* sessions.admit(sessionId, Effect.void).pipe(Effect.flip);
    expect(error).toBeInstanceOf(SessionClosingError);
  }),
);

it.effect("retries Session cleanup after a failure", () =>
  Effect.gen(function* () {
    const sessions = yield* makeTestSessionLifecycle;
    const firstError = yield* sessions
      .close(sessionId, Effect.fail("cleanup failed"))
      .pipe(Effect.flip);

    expect(firstError).toBe("cleanup failed");
    yield* sessions.close(sessionId, Effect.void);
    const admissionError = yield* sessions.admit(sessionId, Effect.void).pipe(Effect.flip);
    expect(admissionError).toBeInstanceOf(SessionClosingError);
  }),
);

it.effect("does not extend the Session Closure Lease on repeated close", () =>
  Effect.gen(function* () {
    const closures = yield* makeTestSessionClosureStore;
    const sessions = yield* makeLifecycle(closures);

    yield* sessions.close(sessionId, Effect.void);
    yield* TestClock.adjust("4 minutes");
    yield* sessions.close(sessionId, Effect.void);
    yield* TestClock.adjust("2 minutes");

    yield* sessions.admit(sessionId, Effect.void);
  }),
);

it.effect("rejects late work after restart during the Session Closure Lease", () =>
  Effect.gen(function* () {
    const closures = yield* makeTestSessionClosureStore;
    const make = makeLifecycle(closures);
    const first = yield* make;

    yield* first.close(sessionId, Effect.void);
    const restarted = yield* make;
    const error = yield* restarted.admit(sessionId, Effect.void).pipe(Effect.flip);

    expect(error).toBeInstanceOf(SessionClosingError);
  }),
);

it.effect("releases high-cardinality Session state after the Closure Lease", () =>
  Effect.gen(function* () {
    const closures = yield* makeTestSessionClosureStore;
    const sessions = yield* makeLifecycle(closures);
    const sessionIds = Array.from({ length: 1_000 }, (_, index) =>
      SessionId.make(`session-lifecycle-${index}`),
    );

    yield* Effect.forEach(sessionIds, (id) => sessions.close(id, Effect.void), {
      concurrency: "unbounded",
      discard: true,
    });
    yield* TestClock.adjust("5 minutes");
    expect(yield* closures.prune).toBe(1_000);
    yield* Effect.forEach(sessionIds, (id) => sessions.admit(id, Effect.void), {
      concurrency: "unbounded",
      discard: true,
    });
  }),
);

it.effect("prunes expired Session closures on the lease interval", () =>
  Effect.gen(function* () {
    const closures = yield* makeTestSessionClosureStore;
    const sessions = layerSessionLifecycle({ closureLease: "5 minutes" }).pipe(
      Layer.provide(Layer.succeed(SessionClosureStore, closures)),
    );

    yield* Effect.gen(function* () {
      const lifecycle = yield* SessionLifecycle;
      yield* lifecycle.close(sessionId, Effect.void);
      yield* TestClock.adjust("5 minutes");
      expect(yield* closures.list).toEqual([]);
    }).pipe(Effect.provide(sessions), Effect.scoped);
  }),
);
