import { SessionId } from "@cvr/loom-domain";
import { SessionClosingError } from "@cvr/loom-protocol";
import { expect, it } from "effect-bun-test";
import { Deferred, Effect, Fiber, Ref } from "effect";
import { makeSessionLifecycle } from "../src/index.js";

const sessionId = SessionId.make("session-lifecycle");

it.effect("admits Session work concurrently", () =>
  Effect.gen(function* () {
    const sessions = yield* makeSessionLifecycle;
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

it.effect("rejects work after Session close starts", () =>
  Effect.gen(function* () {
    const sessions = yield* makeSessionLifecycle;
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
    const sessions = yield* makeSessionLifecycle;

    yield* sessions.close(sessionId, Effect.void);
    yield* sessions.close(sessionId, Effect.void);

    const error = yield* sessions.admit(sessionId, Effect.void).pipe(Effect.flip);
    expect(error).toBeInstanceOf(SessionClosingError);
  }),
);

it.effect("retries Session cleanup after a failure", () =>
  Effect.gen(function* () {
    const sessions = yield* makeSessionLifecycle;
    const firstError = yield* sessions
      .close(sessionId, Effect.fail("cleanup failed"))
      .pipe(Effect.flip);

    expect(firstError).toBe("cleanup failed");
    yield* sessions.close(sessionId, Effect.void);
    const admissionError = yield* sessions.admit(sessionId, Effect.void).pipe(Effect.flip);
    expect(admissionError).toBeInstanceOf(SessionClosingError);
  }),
);
