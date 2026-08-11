import type {
  AssistantMessage,
  Context,
  Model,
  StreamOptions,
  UserMessage,
} from "@earendil-works/pi-ai";
import { type ExtensionCommandContext, SessionManager } from "@earendil-works/pi-coding-agent";
import { expect, it } from "bun:test";
import { Cause, Effect, Exit, Fiber, Option } from "effect";
import {
  interruptOnAbortSignal,
  runSideConversationTurn,
  runSideConversationTurnInScope,
  SideConversationError,
  type SideConversationTurn,
} from "../src/internal/side-conversation.js";

const usage = {
  input: 20,
  output: 4,
  cacheRead: 0,
  cacheWrite: 0,
  totalTokens: 24,
  cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
};

const model: Model<"openai-responses"> = {
  id: "gpt-test",
  name: "GPT Test",
  api: "openai-responses",
  provider: "openai",
  baseUrl: "https://example.test",
  reasoning: true,
  input: ["text"],
  cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
  contextWindow: 128_000,
  maxTokens: 8_000,
};

const assistant = (text: string): AssistantMessage => ({
  role: "assistant",
  content: [{ type: "text", text }],
  api: model.api,
  provider: model.provider,
  model: model.id,
  usage,
  stopReason: "stop",
  timestamp: 2,
});

const user = (content: string, timestamp: number): UserMessage => ({
  role: "user",
  content,
  timestamp,
});

const makeCompactedSession = () => {
  const session = SessionManager.inMemory("/tmp/loom-side-conversation");
  session.appendMessage(user("old context", 1));
  const keptId = session.appendMessage(user("kept context", 2));
  session.appendCompaction("summary", keptId, 12_000);
  session.appendMessage(user("new context", 3));
  return session;
};

it("uses a fresh compacted snapshot and keeps side turns outside the Session", () =>
  Effect.gen(function* () {
    const session = makeCompactedSession();
    const before = structuredClone(session.getEntries());
    const priorTurn: SideConversationTurn = {
      question: user("prior side question", 4),
      answer: assistant("prior side answer"),
    };
    let requestContext: Context = { messages: [] };
    let requestOptions: StreamOptions = {};
    const result = yield* runSideConversationTurn(
      {
        getSystemPrompt: () => "system",
        model,
        modelRegistry: {
          complete: (_selectedModel, context, options) => {
            requestContext = context;
            requestOptions = options ?? {};
            return globalThis.Promise.resolve(assistant("current side answer"));
          },
        },
        sessionManager: session,
      },
      "current side question",
      [priorTurn],
    );

    expect(requestContext.systemPrompt).toBe("system");
    expect(requestContext.tools).toEqual([]);
    expect(requestContext.messages.map((message) => message.content)).toEqual([
      [{ type: "text", text: expect.stringContaining("summary") }],
      "kept context",
      "new context",
      "prior side question",
      [{ type: "text", text: "prior side answer" }],
      "current side question",
    ]);
    expect(requestOptions).toMatchObject({
      cacheRetention: "none",
      sessionId: `${session.getSessionId()}.side.2`,
    });
    expect(requestOptions).not.toHaveProperty("reasoning");
    expect(result.text).toBe("current side answer");
    expect(result.usage).toEqual(usage);
    expect(session.getEntries()).toEqual(before);
  }).pipe(Effect.runPromise));

it("reports context overflow as a typed failure", () => {
  const overflow: AssistantMessage = {
    ...assistant(""),
    stopReason: "error",
    errorMessage: "Your input exceeds the context window of this model",
  };
  return Effect.gen(function* () {
    const session = SessionManager.inMemory("/tmp/loom-side-overflow");
    const error = yield* Effect.flip(
      runSideConversationTurn(
        {
          getSystemPrompt: () => "system",
          model,
          modelRegistry: {
            complete: () => globalThis.Promise.resolve(overflow),
          },
          sessionManager: session,
        },
        "too much context",
        [],
      ),
    );
    expect(error).toEqual(
      new SideConversationError({
        reason: "ContextOverflow",
        message: "The side question exceeds the model context.",
      }),
    );
  }).pipe(Effect.runPromise);
});

it("aborts the provider request when its Effect Scope closes", () =>
  Effect.gen(function* () {
    const session = SessionManager.inMemory("/tmp/loom-side-interruption");
    let requestSignal = Option.none<AbortSignal>();
    let loaderClosed = false;
    const complete: ExtensionCommandContext["modelRegistry"]["complete"] = (
      _selectedModel,
      _context,
      options,
    ) => {
      requestSignal = Option.fromNullishOr(options?.signal);
      // oxlint-disable-next-line effect/noNewPromise -- Simulate the Promise-based Pi provider boundary.
      return new globalThis.Promise<AssistantMessage>(() => {});
    };
    const context = {
      getSystemPrompt: () => "system",
      model,
      modelRegistry: { complete },
      sessionManager: session,
    };
    const turn = runSideConversationTurnInScope(
      context,
      "interrupt me",
      [],
      Effect.succeed({
        cancelled: Effect.never,
        close: () => {
          loaderClosed = true;
        },
      }),
    );

    const fiber = yield* Effect.forkChild(turn);
    yield* Effect.yieldNow;
    const signal = Option.getOrThrow(requestSignal);
    yield* Fiber.interrupt(fiber);
    expect(signal.aborted).toBe(true);
    expect(loaderClosed).toBe(true);
    const exit = yield* Fiber.await(fiber);
    expect(Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause)).toBe(true);
  }).pipe(Effect.runPromise));

it("interrupts from an already-aborted loader signal", () => {
  const controller = new AbortController();
  controller.abort();
  return Effect.gen(function* () {
    const exit = yield* interruptOnAbortSignal(controller.signal).pipe(Effect.exit);
    expect(Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause)).toBe(true);
  }).pipe(Effect.runPromise);
});
