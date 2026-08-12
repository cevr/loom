import type { PluginStateValue } from "@cvr/loom-client";
import { PluginId, PluginStateKey, PluginStateScope } from "@cvr/loom-domain";
import {
  PluginStateRevision,
  PluginStateRevisionConflictError,
  PluginStateVersion,
} from "@cvr/loom-protocol";
import { expect, it } from "effect-bun-test";
import { Effect, Exit, Option, Ref } from "effect";
import {
  GoalCommand,
  makeGoalComponent,
  parseGoalCommand,
  type GoalStateGrant,
} from "../src/internal/goal-component.js";
import { GoalState } from "../src/internal/goal-state.js";

const makeHarness = Effect.gen(function* () {
  const stored = yield* Ref.make(Option.none<PluginStateValue<GoalState>>());
  const events = yield* Ref.make<ReadonlyArray<string>>([]);
  const state: GoalStateGrant<never> = {
    read: Ref.get(stored),
    write: (value, expectedRevision) =>
      Effect.gen(function* () {
        const current = yield* Ref.get(stored);
        expect(expectedRevision).toEqual(Option.map(current, ({ revision }) => revision));
        yield* Ref.set(stored, Option.some({ value, revision: value.revision }));
        yield* Ref.update(events, (items) => [...items, "write"]);
        return PluginStateRevision.make(value.revision);
      }),
  };
  const grants = {
    state,
    turns: {
      ready: Effect.succeed(true),
      request: () => Ref.update(events, (items) => [...items, "turn"]),
      stop: Ref.update(events, (items) => [...items, "stop"]),
    },
    actions: {
      showStatus: () => Ref.update(events, (items) => [...items, "status"]),
    },
  };
  const component = makeGoalComponent(grants);
  return { component, events, grants, state, stored };
});

it.effect("stores a Goal before it requests the first turn", () =>
  Effect.gen(function* () {
    const { component, events, stored } = yield* makeHarness;
    yield* component.start("Ship the release", Option.some(1_000));

    expect(yield* Ref.get(events)).toEqual(["write", "status", "turn"]);
    expect(Option.getOrThrow(yield* Ref.get(stored)).value).toEqual(
      GoalState.cases.Active.make({
        objective: "Ship the release",
        tokenBudget: Option.some(1_000),
        consumedTokens: 0,
        revision: 1,
      }),
    );
  }),
);

it.effect("rejects a second open Goal", () =>
  Effect.gen(function* () {
    const { component } = yield* makeHarness;
    yield* component.start("First Goal", Option.none());
    const exit = yield* component.start("Second Goal", Option.none()).pipe(Effect.exit);

    expect(Exit.isFailure(exit)).toBe(true);
  }),
);

it.effect("pauses, resumes once, and clears without extra turns", () =>
  Effect.gen(function* () {
    const { component, events, stored } = yield* makeHarness;
    yield* component.start("Review all files", Option.none());
    yield* component.pause;
    expect(yield* Ref.get(events)).toEqual(["write", "status", "turn", "write", "status", "stop"]);

    yield* component.resume;
    expect(GoalState.guards.Active(Option.getOrThrow(yield* Ref.get(stored)).value)).toBe(true);
    expect((yield* Ref.get(events)).filter((event) => event === "turn")).toHaveLength(2);

    yield* component.clear;
    expect(GoalState.guards.Cleared(Option.getOrThrow(yield* Ref.get(stored)).value)).toBe(true);
    expect((yield* Ref.get(events)).filter((event) => event === "turn")).toHaveLength(2);

    yield* component.start("Clear while paused", Option.none());
    yield* component.pause;
    yield* component.clear;
    expect(GoalState.guards.Cleared(Option.getOrThrow(yield* Ref.get(stored)).value)).toBe(true);
  }),
);

it.effect("stops continuation when completed or blocked", () =>
  Effect.gen(function* () {
    const completed = yield* makeHarness;
    yield* completed.component.start("Complete me", Option.none());
    yield* completed.component.complete;
    yield* completed.component.continueIfActive;
    expect((yield* Ref.get(completed.events)).filter((event) => event === "turn")).toHaveLength(1);

    const blocked = yield* makeHarness;
    yield* blocked.component.start("Block me", Option.none());
    yield* blocked.component.block("The same service failed three times");
    yield* blocked.component.continueIfActive;
    expect((yield* Ref.get(blocked.events)).filter((event) => event === "turn")).toHaveLength(1);
  }),
);

it.effect("uses completed assistant usage to enforce the token budget", () =>
  Effect.gen(function* () {
    const { component, events, stored } = yield* makeHarness;
    yield* component.start("Stay within budget", Option.some(100));
    yield* component.accountUsage(60);
    yield* component.accountUsage(40);
    yield* component.continueIfActive;

    const goal = Option.getOrThrow(yield* Ref.get(stored)).value;
    expect(GoalState.guards.BudgetExhausted(goal)).toBe(true);
    if (!GoalState.guards.BudgetExhausted(goal)) return yield* Effect.die("invalid state");
    expect(goal.consumedTokens).toBe(100);
    expect((yield* Ref.get(events)).filter((event) => event === "turn")).toHaveLength(1);
    expect((yield* Ref.get(events)).filter((event) => event === "stop")).toHaveLength(1);
  }),
);

it.effect("restores an active Goal from durable state", () =>
  Effect.gen(function* () {
    const { component, events, grants } = yield* makeHarness;
    yield* component.start("Resume after reload", Option.none());
    yield* makeGoalComponent(grants).continueIfActive;

    expect((yield* Ref.get(events)).filter((event) => event === "turn")).toHaveLength(2);
  }),
);

it.effect("does not queue a Goal turn while Pi is busy", () =>
  Effect.gen(function* () {
    const { component, events, grants } = yield* makeHarness;
    yield* component.start("Wait for Pi to settle", Option.none());
    const busy = makeGoalComponent({
      ...grants,
      turns: { ...grants.turns, ready: Effect.succeed(false) },
    });
    yield* busy.continueIfActive;

    expect((yield* Ref.get(events)).filter((event) => event === "turn")).toHaveLength(1);
  }),
);

it.effect("re-reads and reapplies a transition after a compare-and-set conflict", () =>
  Effect.gen(function* () {
    const address = {
      pluginId: PluginId.make("loom.goal"),
      scope: PluginStateScope.cases.Workspace.make({}),
      key: PluginStateKey.make("goal"),
    };
    const attempts = yield* Ref.make(0);
    const stored = yield* Ref.make(Option.none<PluginStateValue<GoalState>>());
    const state: GoalStateGrant<PluginStateRevisionConflictError> = {
      read: Ref.get(stored),
      write: (value) =>
        Ref.updateAndGet(attempts, (count) => count + 1).pipe(
          Effect.flatMap((attempt) =>
            Effect.gen(function* () {
              if (attempt === 1) {
                yield* Ref.set(
                  stored,
                  Option.some({
                    value: GoalState.cases.Cleared.make({ revision: 1 }),
                    revision: PluginStateRevision.make(1),
                  }),
                );
                return yield* new PluginStateRevisionConflictError({
                  address,
                  expected: PluginStateVersion.cases.Missing.make({}),
                  actual: PluginStateVersion.cases.Present.make({ revision: 1 }),
                });
              }
              yield* Ref.set(stored, Option.some({ value, revision: value.revision }));
              return PluginStateRevision.make(value.revision);
            }),
          ),
        ),
    };
    const component = makeGoalComponent({
      state,
      turns: { ready: Effect.succeed(true), request: () => Effect.void, stop: Effect.void },
      actions: { showStatus: () => Effect.void },
    });

    yield* component.start("Conflict", Option.none());
    expect(yield* Ref.get(attempts)).toBe(2);
    const goal = Option.getOrThrow(yield* Ref.get(stored)).value;
    expect(GoalState.guards.Active(goal)).toBe(true);
    expect(goal.revision).toBe(2);
  }),
);

it.effect("parses Goal actions and positive budgets", () =>
  Effect.gen(function* () {
    expect(yield* parseGoalCommand("status")).toEqual(GoalCommand.cases.Status.make({}));
    expect(yield* parseGoalCommand("--budget 250 Build the release")).toEqual(
      GoalCommand.cases.Start.make({
        objective: "Build the release",
        tokenBudget: Option.some(250),
      }),
    );
    expect(
      Exit.isFailure(yield* parseGoalCommand("--budget 0 Build the release").pipe(Effect.exit)),
    ).toBe(true);
    expect(yield* parseGoalCommand("--budgeting behavior is part of the objective")).toEqual(
      GoalCommand.cases.Start.make({
        objective: "--budgeting behavior is part of the objective",
        tokenBudget: Option.none(),
      }),
    );
    expect(Exit.isFailure(yield* parseGoalCommand("x".repeat(4_001)).pipe(Effect.exit))).toBe(true);
  }),
);
