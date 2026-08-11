import type { AssistantMessage, Message, Model, Usage, UserMessage } from "@earendil-works/pi-ai";
import { isContextOverflow } from "@earendil-works/pi-ai";
import {
  BorderedLoader,
  convertToLlm,
  type ExtensionCommandContext,
  sessionEntryToContextMessages,
} from "@earendil-works/pi-coding-agent";
import { Clock, Effect, Option, Schema, Semaphore } from "effect";
import type { LoomExtensionApi } from "./extension-api.js";

const sideConversationWidget = "loom-side-conversation";

export class SideConversationError extends Schema.TaggedError<SideConversationError>()(
  "SideConversationError",
  {
    reason: Schema.Literals(["ContextOverflow", "MissingModel", "RequestFailed"]),
    message: Schema.String,
  },
) {}

export interface SideConversationTurn {
  readonly question: UserMessage;
  readonly answer: AssistantMessage;
}

export interface SideConversationResult {
  readonly text: string;
  readonly usage: Usage;
  readonly turn: SideConversationTurn;
}

interface SideConversationContext {
  readonly getSystemPrompt: ExtensionCommandContext["getSystemPrompt"];
  readonly model: ExtensionCommandContext["model"];
  readonly modelRegistry: Pick<ExtensionCommandContext["modelRegistry"], "complete">;
  readonly sessionManager: Pick<
    ExtensionCommandContext["sessionManager"],
    "buildContextEntries" | "getSessionId"
  >;
}

const cloneMainMessages = (context: SideConversationContext): Message[] =>
  structuredClone(
    convertToLlm(
      context.sessionManager.buildContextEntries().flatMap(sessionEntryToContextMessages),
    ),
  );

const turnMessages = (turn: SideConversationTurn): Message[] => [turn.question, turn.answer];

const answerText = (answer: AssistantMessage): string =>
  answer.content
    .filter((content) => content.type === "text")
    .map((content) => content.text)
    .join("\n");

const questionText = (question: UserMessage): string => {
  if (typeof question.content === "string") return question.content;
  return question.content
    .filter((content) => content.type === "text")
    .map((content) => content.text)
    .join("\n");
};

const resolveAnswer = (
  model: Model<string>,
  answer: AssistantMessage,
): Effect.Effect<AssistantMessage, SideConversationError> => {
  if (isContextOverflow(answer, model.contextWindow)) {
    return Effect.fail(
      new SideConversationError({
        reason: "ContextOverflow",
        message: "The side question exceeds the model context.",
      }),
    );
  }
  if (answer.stopReason === "aborted") return Effect.interrupt;
  if (answer.stopReason === "stop" || answer.stopReason === "length") return Effect.succeed(answer);
  return Effect.fail(
    new SideConversationError({
      reason: "RequestFailed",
      message: answer.errorMessage ?? `Side turn stopped with ${answer.stopReason}`,
    }),
  );
};

export const runSideConversationTurn = Effect.fn("LoomPiExtension.runSideConversationTurn")(
  function* (
    context: SideConversationContext,
    question: string,
    priorTurns: readonly SideConversationTurn[],
  ) {
    const model = context.model;
    if (!model) {
      return yield* new SideConversationError({
        reason: "MissingModel",
        message: "Select a model before using /btw.",
      });
    }

    const timestamp = yield* Clock.currentTimeMillis;
    const userMessage: UserMessage = { role: "user", content: question, timestamp };
    const requestSessionId = `${context.sessionManager.getSessionId()}.side.${priorTurns.length + 1}`;
    const answer = yield* Effect.tryPromise({
      try: (signal) =>
        context.modelRegistry.complete(
          model,
          {
            systemPrompt: context.getSystemPrompt(),
            messages: [
              ...cloneMainMessages(context),
              ...priorTurns.flatMap(turnMessages),
              userMessage,
            ],
            tools: [],
          },
          { cacheRetention: "none", sessionId: requestSessionId, signal },
        ),
      catch: (cause) =>
        new SideConversationError({
          reason: "RequestFailed",
          message: `Side question failed: ${String(cause)}`,
        }),
    }).pipe(Effect.flatMap((response) => resolveAnswer(model, response)));

    return {
      text: answerText(answer),
      usage: answer.usage,
      turn: { question: userMessage, answer },
    };
  },
);

const renderConversation = (
  context: ExtensionCommandContext,
  turns: readonly SideConversationTurn[],
) =>
  Effect.sync(() =>
    context.ui.setWidget(
      sideConversationWidget,
      turns.flatMap((turn) => [
        `You: ${questionText(turn.question)}`,
        `Loom: ${answerText(turn.answer)}`,
        `Tokens: ${turn.answer.usage.totalTokens}`,
        "",
      ]),
    ),
  );

const requestQuestion = (context: ExtensionCommandContext, title: string) =>
  Effect.tryPromise({
    try: (signal) => context.ui.input(title, "Ask a side question", { signal }),
    catch: (cause) =>
      new SideConversationError({
        reason: "RequestFailed",
        message: `Side question input failed: ${String(cause)}`,
      }),
  }).pipe(Effect.map(Option.fromNullishOr));

interface SideConversationLoader {
  readonly cancelled: Effect.Effect<never>;
  readonly close: () => void;
}

export const interruptOnAbortSignal = (signal: AbortSignal) =>
  Effect.suspend(() => {
    if (signal.aborted) return Effect.interrupt;
    return Effect.callback<never>((resume) => {
      const interrupt = () => resume(Effect.interrupt);
      signal.addEventListener("abort", interrupt, { once: true });
      return Effect.sync(() => {
        signal.removeEventListener("abort", interrupt);
      });
    });
  });

const acquireLoader = (context: ExtensionCommandContext) =>
  Effect.callback<SideConversationLoader, SideConversationError>((resume) => {
    context.ui
      .custom<void>((tui, theme, _keybindings, done) => {
        const loader = new BorderedLoader(tui, theme, `Asking ${context.model?.id ?? "model"}...`);
        resume(Effect.succeed({ cancelled: interruptOnAbortSignal(loader.signal), close: done }));
        return loader;
      })
      .catch((cause) =>
        resume(
          Effect.fail(
            new SideConversationError({
              reason: "RequestFailed",
              message: `Side question UI failed: ${String(cause)}`,
            }),
          ),
        ),
      );
  });

export const runSideConversationTurnInScope = (
  context: SideConversationContext,
  question: string,
  priorTurns: readonly SideConversationTurn[],
  loader: Effect.Effect<SideConversationLoader, SideConversationError>,
) =>
  Effect.acquireUseRelease(
    loader,
    ({ cancelled }) =>
      Effect.raceFirst(runSideConversationTurn(context, question, priorTurns), cancelled),
    ({ close }) => Effect.sync(close),
  );

const showTurnLoader = (
  context: ExtensionCommandContext,
  question: string,
  priorTurns: readonly SideConversationTurn[],
) => runSideConversationTurnInScope(context, question, priorTurns, acquireLoader(context));

const continueConversation = (
  context: ExtensionCommandContext,
  question: string,
  turns: readonly SideConversationTurn[],
): Effect.Effect<void, SideConversationError> =>
  Effect.gen(function* () {
    const result = yield* showTurnLoader(context, question, turns);
    const nextTurns = [...turns, result.turn];
    yield* renderConversation(context, nextTurns);
    const followUp = yield* requestQuestion(context, "Follow up, or press Escape to close");
    if (Option.isNone(followUp)) return;
    const nextQuestion = followUp.value.trim();
    if (nextQuestion.length === 0) return;
    return yield* continueConversation(context, nextQuestion, nextTurns);
  });

const commandProgram = (arguments_: string, context: ExtensionCommandContext) =>
  Effect.gen(function* () {
    if (context.mode !== "tui") {
      context.ui.notify("/btw requires interactive mode.", "error");
      return;
    }
    const initial = arguments_.trim();
    let question = Option.some(initial);
    if (initial.length === 0) {
      question = yield* requestQuestion(context, "Ask without changing the main transcript");
    }
    if (Option.isNone(question)) return;
    yield* continueConversation(context, question.value, []);
  }).pipe(
    Effect.catchTag("SideConversationError", (error) =>
      Effect.sync(() => context.ui.notify(error.message, "error")),
    ),
    Effect.ensuring(
      Effect.sync(() =>
        // oxlint-disable-next-line effect/noNullish -- Pi uses undefined as its widget-removal command.
        context.ui.setWidget(sideConversationWidget, undefined),
      ),
    ),
  );

export const registerSideConversation = (pi: LoomExtensionApi): void => {
  const semaphore = Semaphore.makeUnsafe(1);
  const handler = (arguments_: string, context: ExtensionCommandContext) =>
    Effect.runPromise(
      semaphore
        .withPermitsIfAvailable(1)(commandProgram(arguments_, context))
        .pipe(
          Effect.flatMap(
            Option.match({
              onNone: () =>
                Effect.sync(() =>
                  context.ui.notify("A side conversation is already open.", "warning"),
                ),
              onSome: () => Effect.void,
            }),
          ),
        ),
    );

  pi.registerCommand("btw", {
    description: "Ask without changing the main transcript",
    handler,
  });
  pi.registerCommand("side", {
    description: "Alias for /btw",
    handler,
  });
};
