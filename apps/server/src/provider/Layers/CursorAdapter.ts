/**
 * CursorAdapterLive — Cursor CLI (`agent acp`) via ACP.
 *
 * @module CursorAdapterLive
 */

import * as NodeTimersPromises from "node:timers/promises";

import {
  ApprovalRequestId,
  type CursorSettings,
  type ProviderOptionSelection,
  EventId,
  type ProviderApprovalDecision,
  type ProviderInteractionMode,
  type ProviderRuntimeEvent,
  type ProviderSession,
  type ProviderUserInputAnswers,
  ProviderDriverKind,
  ProviderInstanceId,
  RuntimeRequestId,
  type RuntimeMode,
  type ThreadId,
  TurnId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Crypto from "effect/Crypto";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as PubSub from "effect/PubSub";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as SynchronizedRef from "effect/SynchronizedRef";
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner";
import * as EffectAcpErrors from "effect-acp/errors";
import type * as EffectAcpSchema from "effect-acp/schema";

import { resolveAttachmentPath } from "../../attachmentStore.ts";
import { ServerConfig } from "../../config.ts";
import { makeAcpMcpServers } from "../../mcp/McpProviderInjection.ts";
import * as McpProviderSession from "../../mcp/McpProviderSession.ts";
import {
  ProviderAdapterProcessError,
  ProviderAdapterRequestError,
  ProviderAdapterSessionNotFoundError,
  ProviderAdapterValidationError,
} from "../Errors.ts";
import { acpPermissionOutcome, mapAcpToAdapterError } from "../acp/AcpAdapterSupport.ts";
import type * as AcpSessionRuntime from "../acp/AcpSessionRuntime.ts";
import {
  makeAcpAssistantItemEvent,
  makeAcpContentDeltaEvent,
  makeAcpPlanUpdatedEvent,
  makeAcpRequestOpenedEvent,
  makeAcpRequestResolvedEvent,
  makeAcpToolCallEvent,
} from "../acp/AcpCoreRuntimeEvents.ts";
import {
  type AcpSessionMode,
  type AcpSessionModeState,
  parsePermissionRequest,
} from "../acp/AcpRuntimeModel.ts";
import { makeAcpNativeLoggerFactory } from "../acp/AcpNativeLogging.ts";
import { applyCursorAcpModelSelection, makeCursorAcpRuntime } from "../acp/CursorAcpSupport.ts";
import {
  CursorAskQuestionRequest,
  CursorCreatePlanRequest,
  CursorUpdateTodosRequest,
  extractAskQuestions,
  extractPlanMarkdown,
  extractTodosAsPlan,
} from "../acp/CursorAcpExtension.ts";
import { type CursorAdapterShape } from "../Services/CursorAdapter.ts";
import { resolveCursorAcpBaseModelId } from "./CursorProvider.ts";
import { type EventNdjsonLogger, makeEventNdjsonLogger } from "./EventNdjsonLogger.ts";
const encodeUnknownJsonStringExit = Schema.encodeUnknownExit(Schema.UnknownFromJsonString);

const PROVIDER = ProviderDriverKind.make("cursor");
const CURSOR_RESUME_VERSION = 1 as const;
const ACP_PLAN_MODE_ALIASES = ["plan", "architect"];
const ACP_IMPLEMENT_MODE_ALIASES = ["code", "agent", "default", "chat", "implement"];
const ACP_APPROVAL_MODE_ALIASES = ["ask"];
const CURSOR_COMPLETED_TURN_LATE_UPDATE_GRACE_MS = 200;
const CURSOR_CANCEL_REQUEST_TIMEOUT_MS = 500;
const CURSOR_CANCEL_DRAIN_TIMEOUT_MS = 500;

const liveDelay = (milliseconds: number) =>
  Effect.promise<void>(() => NodeTimersPromises.setTimeout(milliseconds).then(() => undefined));

function encodeJsonStringForDiagnostics(input: unknown): string | undefined {
  const result = encodeUnknownJsonStringExit(input);
  return Exit.isSuccess(result) ? result.value : undefined;
}

export interface CursorAdapterLiveOptions {
  readonly environment?: NodeJS.ProcessEnv;
  readonly nativeEventLogPath?: string;
  readonly nativeEventLogger?: EventNdjsonLogger;
  /**
   * Selections are honored when `modelSelection.instanceId` matches this value.
   * Defaults to the legacy built-in instance id (`cursor`).
   */
  readonly instanceId?: ProviderInstanceId;
  /**
   * Optional per-session settings resolver. When provided the adapter yields
   * this effect at the start of every session and uses the result instead of
   * the `cursorSettings` captured at construction.
   *
   * Production instances bind settings to the instance scope (the hydration
   * layer rebuilds the adapter on config change) and leave this undefined.
   * Test suites that mutate `ServerSettingsService` mid-flight — e.g. to
   * swap `binaryPath` to a mock ACP wrapper — pass a resolver that reads
   * the latest snapshot so the closure isn't stale.
   */
  readonly resolveSettings?: Effect.Effect<CursorSettings>;
}

interface PendingApproval {
  readonly decision: Deferred.Deferred<ProviderApprovalDecision>;
  readonly kind: string | "unknown";
}

interface PendingUserInput {
  readonly answers: Deferred.Deferred<ProviderUserInputAnswers>;
}

type CursorBoundModelSelection = NonNullable<
  Parameters<CursorAdapterShape["startSession"]>[0]["modelSelection"]
>;

function cloneCursorModelSelection(
  selection: CursorBoundModelSelection | undefined,
): CursorBoundModelSelection | undefined {
  if (selection === undefined) return undefined;
  return {
    instanceId: selection.instanceId,
    model: selection.model,
    ...(selection.options !== undefined ? { options: [...selection.options] } : {}),
  };
}

interface CursorSessionContext {
  readonly threadId: ThreadId;
  session: ProviderSession;
  readonly scope: Scope.Closeable;
  readonly acp: AcpSessionRuntime.AcpSessionRuntime["Service"];
  readonly mcpProviderSessionId?: string;
  notificationFiber: Fiber.Fiber<void, never> | undefined;
  readonly pendingApprovals: Map<ApprovalRequestId, PendingApproval>;
  readonly pendingUserInputs: Map<ApprovalRequestId, PendingUserInput>;
  readonly turns: Array<{ id: TurnId; items: Array<unknown> }>;
  lastPlanFingerprint: string | undefined;
  activeTurnId: TurnId | undefined;
  notificationTurnId: TurnId | undefined;
  lastModelSelection: CursorBoundModelSelection | undefined;
  dropAcpUpdatesAfterLocalCancel: boolean;
  readonly suppressedNotificationTurnIds: Set<string>;
  readonly preCompletedCancelledTurnIds: Set<string>;
  /** Turn ids that produced any user-visible output (assistant text, tool
   * calls, plan updates, proposed plans, user-input requests, or surfaced
   * permission requests). A non-cancelled turn that completes without any
   * entry here must fail loudly instead of persisting a silent empty
   * response. */
  readonly turnsWithVisibleOutput: Set<string>;
  pendingPromptTurnId: TurnId | undefined;
  localCancelRequestsInFlight: number;
  locallyCancelledPromptsInFlight: number;
  localCancelSettled: Deferred.Deferred<void> | undefined;
  promptStartSettled: Deferred.Deferred<void> | undefined;
  promptStartedDuringLocalCancel: boolean;
  restartBeforeNextPrompt: boolean;
  readonly stoppedSignal: Deferred.Deferred<void>;
  /** Number of sendTurn prompts currently in flight or being prepared.
   * >0 means a turn is actively running, so a new sendTurn is a steer that
   * continues it, and only the last remaining prompt settles the turn. */
  promptsInFlight: number;
  stopped: boolean;
}

interface StartSessionInternalOptions {
  readonly replaceExistingAfterStart?: boolean;
  readonly emitReplacedSessionExited?: boolean;
  readonly detachReplacedSessionStop?: boolean;
  readonly initialTurns?: CursorSessionContext["turns"];
  readonly initialDropAcpUpdatesAfterLocalCancel?: boolean;
  readonly initialSuppressedNotificationTurnIds?: ReadonlySet<string>;
}

function settlePendingApprovalsAsCancelled(
  pendingApprovals: ReadonlyMap<ApprovalRequestId, PendingApproval>,
): Effect.Effect<void> {
  const pendingEntries = Array.from(pendingApprovals.values());
  return Effect.forEach(
    pendingEntries,
    (pending) => Deferred.succeed(pending.decision, "cancel").pipe(Effect.ignore),
    {
      discard: true,
    },
  );
}

function settlePendingUserInputsAsEmptyAnswers(
  pendingUserInputs: ReadonlyMap<ApprovalRequestId, PendingUserInput>,
): Effect.Effect<void> {
  const pendingEntries = Array.from(pendingUserInputs.values());
  return Effect.forEach(
    pendingEntries,
    (pending) => Deferred.succeed(pending.answers, {}).pipe(Effect.ignore),
    {
      discard: true,
    },
  );
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function parseCursorResume(raw: unknown): { sessionId: string } | undefined {
  if (!isRecord(raw)) return undefined;
  if (raw.schemaVersion !== CURSOR_RESUME_VERSION) return undefined;
  if (typeof raw.sessionId !== "string" || !raw.sessionId.trim()) return undefined;
  return { sessionId: raw.sessionId.trim() };
}

function normalizeModeSearchText(mode: AcpSessionMode): string {
  return [mode.id, mode.name, mode.description]
    .filter((value): value is string => typeof value === "string" && value.length > 0)
    .join(" ")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function findModeByAliases(
  modes: ReadonlyArray<AcpSessionMode>,
  aliases: ReadonlyArray<string>,
): AcpSessionMode | undefined {
  const normalizedAliases = aliases.map((alias) => alias.toLowerCase());
  for (const alias of normalizedAliases) {
    const exact = modes.find((mode) => {
      const id = mode.id.toLowerCase();
      const name = mode.name.toLowerCase();
      return id === alias || name === alias;
    });
    if (exact) {
      return exact;
    }
  }
  for (const alias of normalizedAliases) {
    const partial = modes.find((mode) => normalizeModeSearchText(mode).includes(alias));
    if (partial) {
      return partial;
    }
  }
  return undefined;
}

function isPlanMode(mode: AcpSessionMode): boolean {
  return findModeByAliases([mode], ACP_PLAN_MODE_ALIASES) !== undefined;
}

function resolveRequestedModeId(input: {
  readonly interactionMode: ProviderInteractionMode | undefined;
  readonly runtimeMode: RuntimeMode;
  readonly modeState: AcpSessionModeState | undefined;
}): string | undefined {
  const modeState = input.modeState;
  if (!modeState) {
    return undefined;
  }

  if (input.interactionMode === "plan") {
    return findModeByAliases(modeState.availableModes, ACP_PLAN_MODE_ALIASES)?.id;
  }

  if (input.runtimeMode === "approval-required") {
    return (
      findModeByAliases(modeState.availableModes, ACP_APPROVAL_MODE_ALIASES)?.id ??
      findModeByAliases(modeState.availableModes, ACP_IMPLEMENT_MODE_ALIASES)?.id ??
      modeState.availableModes.find((mode) => !isPlanMode(mode))?.id ??
      modeState.currentModeId
    );
  }

  return (
    findModeByAliases(modeState.availableModes, ACP_IMPLEMENT_MODE_ALIASES)?.id ??
    findModeByAliases(modeState.availableModes, ACP_APPROVAL_MODE_ALIASES)?.id ??
    modeState.availableModes.find((mode) => !isPlanMode(mode))?.id ??
    modeState.currentModeId
  );
}

function applyRequestedSessionConfiguration<E>(input: {
  readonly runtime: AcpSessionRuntime.AcpSessionRuntime["Service"];
  readonly runtimeMode: RuntimeMode;
  readonly interactionMode: ProviderInteractionMode | undefined;
  readonly modelSelection:
    | {
        readonly model: string;
        readonly options?: ReadonlyArray<ProviderOptionSelection> | null | undefined;
      }
    | undefined;
  readonly mapError: (context: {
    readonly cause: import("effect-acp/errors").AcpError;
    readonly method: "session/set_config_option" | "session/set_mode";
  }) => E;
}): Effect.Effect<void, E> {
  return Effect.gen(function* () {
    if (input.modelSelection) {
      yield* applyCursorAcpModelSelection({
        runtime: input.runtime,
        model: input.modelSelection.model,
        selections: input.modelSelection.options,
        mapError: ({ cause }) =>
          input.mapError({
            cause,
            method: "session/set_config_option",
          }),
      });
    }

    const requestedModeId = resolveRequestedModeId({
      interactionMode: input.interactionMode,
      runtimeMode: input.runtimeMode,
      modeState: yield* input.runtime.getModeState,
    });
    if (!requestedModeId) {
      return;
    }

    yield* input.runtime.setMode(requestedModeId).pipe(
      Effect.mapError((cause) =>
        input.mapError({
          cause,
          method: "session/set_mode",
        }),
      ),
    );
  });
}

function selectAutoApprovedPermissionOption(
  request: EffectAcpSchema.RequestPermissionRequest,
): string | undefined {
  const allowAlwaysOption = request.options.find((option) => option.kind === "allow_always");
  if (typeof allowAlwaysOption?.optionId === "string" && allowAlwaysOption.optionId.trim()) {
    return allowAlwaysOption.optionId.trim();
  }

  const allowOnceOption = request.options.find((option) => option.kind === "allow_once");
  if (typeof allowOnceOption?.optionId === "string" && allowOnceOption.optionId.trim()) {
    return allowOnceOption.optionId.trim();
  }

  return undefined;
}

export function makeCursorAdapter(
  cursorSettings: CursorSettings,
  options?: CursorAdapterLiveOptions,
) {
  return Effect.gen(function* () {
    const boundInstanceId = options?.instanceId ?? ProviderInstanceId.make("cursor");
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const childProcessSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const serverConfig = yield* Effect.service(ServerConfig);
    const crypto = yield* Crypto.Crypto;
    const nativeEventLogger =
      options?.nativeEventLogger ??
      (options?.nativeEventLogPath !== undefined
        ? yield* makeEventNdjsonLogger(options.nativeEventLogPath, {
            stream: "native",
          })
        : undefined);
    const managedNativeEventLogger =
      options?.nativeEventLogger === undefined ? nativeEventLogger : undefined;
    const makeAcpNativeLoggers = yield* makeAcpNativeLoggerFactory();

    const sessions = new Map<ThreadId, CursorSessionContext>();
    const threadLocksRef = yield* SynchronizedRef.make(new Map<string, Semaphore.Semaphore>());
    const runtimeEventPubSub = yield* PubSub.unbounded<ProviderRuntimeEvent>();

    const nowIso = Effect.map(DateTime.now, DateTime.formatIso);
    const randomUUIDv4 = crypto.randomUUIDv4.pipe(
      Effect.mapError(
        (cause) =>
          new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "crypto/randomUUIDv4",
            detail: "Failed to generate Cursor runtime identifier.",
            cause,
          }),
      ),
    );
    const nextEventId = Effect.map(randomUUIDv4, (id) => EventId.make(id));
    const makeEventStamp = () => Effect.all({ eventId: nextEventId, createdAt: nowIso });
    const mapExtensionFailure = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
      effect.pipe(
        Effect.mapError(
          (cause) =>
            new EffectAcpErrors.AcpTransportError({
              detail: "Failed to process Cursor ACP extension event.",
              cause,
            }),
        ),
      );

    const offerRuntimeEvent = (event: ProviderRuntimeEvent) =>
      PubSub.publish(runtimeEventPubSub, event).pipe(Effect.asVoid);

    const getThreadSemaphore = (threadId: string) =>
      SynchronizedRef.modifyEffect(threadLocksRef, (current) => {
        const existing: Option.Option<Semaphore.Semaphore> = Option.fromNullishOr(
          current.get(threadId),
        );
        return Option.match(existing, {
          onNone: () =>
            Semaphore.make(1).pipe(
              Effect.map((semaphore) => {
                const next = new Map(current);
                next.set(threadId, semaphore);
                return [semaphore, next] as const;
              }),
            ),
          onSome: (semaphore) => Effect.succeed([semaphore, current] as const),
        });
      });

    const withThreadLock = <A, E, R>(threadId: string, effect: Effect.Effect<A, E, R>) =>
      Effect.flatMap(getThreadSemaphore(threadId), (semaphore) => semaphore.withPermit(effect));

    const logNative = (
      threadId: ThreadId,
      method: string,
      payload: unknown,
      _source: "acp.jsonrpc" | "acp.cursor.extension",
    ) =>
      Effect.gen(function* () {
        if (!nativeEventLogger) return;
        const observedAt = yield* nowIso;
        yield* nativeEventLogger.write(
          {
            observedAt,
            event: {
              id: yield* randomUUIDv4,
              kind: "notification",
              provider: PROVIDER,
              createdAt: observedAt,
              method,
              threadId,
              payload,
            },
          },
          threadId,
        );
      });

    const emitPlanUpdate = (
      ctx: CursorSessionContext,
      payload: {
        readonly explanation?: string | null;
        readonly plan: ReadonlyArray<{
          readonly step: string;
          readonly status: "pending" | "inProgress" | "completed";
        }>;
      },
      rawPayload: unknown,
      source: "acp.jsonrpc" | "acp.cursor.extension",
      method: string,
    ) =>
      Effect.gen(function* () {
        if (shouldDropAcpUpdateAfterLocalCancel(ctx)) {
          return;
        }
        const turnId = ctx.notificationTurnId;
        const fingerprint = `${turnId ?? "no-turn"}:${encodeJsonStringForDiagnostics(payload) ?? "[unserializable payload]"}`;
        if (ctx.lastPlanFingerprint === fingerprint) {
          return;
        }
        ctx.lastPlanFingerprint = fingerprint;
        yield* offerRuntimeEvent(
          makeAcpPlanUpdatedEvent({
            stamp: yield* makeEventStamp(),
            provider: PROVIDER,
            threadId: ctx.threadId,
            turnId,
            payload,
            source,
            method,
            rawPayload,
          }),
        );
      });

    const clearLocalCancelDropForQueuedPrompt = (ctx: CursorSessionContext) => {
      if (
        ctx.promptStartedDuringLocalCancel &&
        ctx.localCancelRequestsInFlight === 0 &&
        ctx.locallyCancelledPromptsInFlight === 0 &&
        ctx.localCancelSettled === undefined
      ) {
        ctx.dropAcpUpdatesAfterLocalCancel = false;
        ctx.promptStartedDuringLocalCancel = false;
      }
    };

    const isNotificationTurnSuppressed = (ctx: CursorSessionContext) =>
      ctx.notificationTurnId !== undefined &&
      ctx.suppressedNotificationTurnIds.has(String(ctx.notificationTurnId));

    const shouldDropAcpUpdateAfterLocalCancel = (ctx: CursorSessionContext) => {
      if (isNotificationTurnSuppressed(ctx)) {
        return true;
      }
      if (!ctx.dropAcpUpdatesAfterLocalCancel) {
        return false;
      }
      if (
        ctx.pendingPromptTurnId !== undefined &&
        ctx.notificationTurnId !== undefined &&
        ctx.notificationTurnId !== ctx.pendingPromptTurnId
      ) {
        return false;
      }
      return true;
    };

    const completeLocalCancelSettled = (ctx: CursorSessionContext) =>
      Effect.gen(function* () {
        if (
          ctx.localCancelSettled !== undefined &&
          ctx.localCancelRequestsInFlight === 0 &&
          ctx.locallyCancelledPromptsInFlight === 0 &&
          ctx.promptsInFlight === 0
        ) {
          yield* drainLocalCancelEventsOrTimeout(ctx);
          ctx.dropAcpUpdatesAfterLocalCancel = false;
          ctx.promptStartedDuringLocalCancel = false;
          const settled = ctx.localCancelSettled;
          ctx.localCancelSettled = undefined;
          yield* Deferred.succeed(settled, undefined).pipe(Effect.asVoid);
        }
      });

    const completeActiveTurnAsCancelledBeforeRestart = (ctx: CursorSessionContext) =>
      Effect.gen(function* () {
        const turnId = ctx.activeTurnId;
        if (turnId === undefined || ctx.preCompletedCancelledTurnIds.has(String(turnId))) {
          return;
        }
        ctx.preCompletedCancelledTurnIds.add(String(turnId));
        ctx.turnsWithVisibleOutput.delete(String(turnId));
        if (!ctx.turns.some((turn) => turn.id === turnId)) {
          ctx.turns.push({ id: turnId, items: [] });
        }
        const { activeTurnId: _cancelledActiveTurnId, ...sessionWithoutActiveTurn } = ctx.session;
        ctx.activeTurnId = undefined;
        ctx.notificationTurnId = turnId;
        ctx.session = {
          ...sessionWithoutActiveTurn,
          status: "ready",
          updatedAt: yield* nowIso,
        };
        yield* offerRuntimeEvent({
          type: "turn.completed",
          ...(yield* makeEventStamp()),
          provider: PROVIDER,
          threadId: ctx.threadId,
          turnId,
          payload: {
            state: "cancelled",
            stopReason: "cancelled",
          },
        });
      });

    const releaseLocalCancelSettledForRestart = (ctx: CursorSessionContext) =>
      Effect.gen(function* () {
        if (
          ctx.localCancelSettled !== undefined &&
          ctx.localCancelRequestsInFlight === 0 &&
          ctx.promptsInFlight > 0 &&
          ctx.restartBeforeNextPrompt
        ) {
          yield* completeActiveTurnAsCancelledBeforeRestart(ctx);
          const settled = ctx.localCancelSettled;
          ctx.localCancelSettled = undefined;
          yield* Deferred.succeed(settled, undefined).pipe(Effect.asVoid);
        }
      });

    const releasePromptStartSettled = (
      ctx: CursorSessionContext,
      promptStartSettled: Deferred.Deferred<void> | undefined,
    ) =>
      Effect.gen(function* () {
        if (promptStartSettled === undefined) {
          return;
        }
        if (ctx.promptStartSettled === promptStartSettled) {
          ctx.promptStartSettled = undefined;
        }
        yield* Deferred.succeed(promptStartSettled, undefined).pipe(Effect.asVoid);
      });

    const requireSession = (
      threadId: ThreadId,
    ): Effect.Effect<CursorSessionContext, ProviderAdapterSessionNotFoundError> => {
      const ctx = sessions.get(threadId);
      if (!ctx || ctx.stopped) {
        return Effect.fail(
          new ProviderAdapterSessionNotFoundError({ provider: PROVIDER, threadId }),
        );
      }
      return Effect.succeed(ctx);
    };

    const awaitNotificationFiberExit = (ctx: CursorSessionContext) =>
      ctx.notificationFiber === undefined
        ? Effect.void
        : Fiber.await(ctx.notificationFiber).pipe(Effect.asVoid);

    const drainEventsOrSessionEnd = (ctx: CursorSessionContext) =>
      Effect.raceFirst(
        ctx.acp.drainEvents,
        Effect.raceFirst(Deferred.await(ctx.stoppedSignal), awaitNotificationFiberExit(ctx)),
      );

    const drainLocalCancelEventsOrTimeout = (ctx: CursorSessionContext) =>
      Effect.raceFirst(drainEventsOrSessionEnd(ctx), liveDelay(CURSOR_CANCEL_DRAIN_TIMEOUT_MS));

    const shouldDrainCompletedTurnLateUpdatesBeforePrompt = (
      ctx: CursorSessionContext,
      nextTurnId: TurnId,
      previousNotificationTurnId: TurnId | undefined,
      previousPromptsInFlight: number,
    ) =>
      previousNotificationTurnId !== undefined &&
      previousNotificationTurnId !== nextTurnId &&
      previousPromptsInFlight === 0 &&
      !ctx.dropAcpUpdatesAfterLocalCancel;

    const drainCompletedTurnLateUpdatesBeforePrompt = (
      ctx: CursorSessionContext,
      nextTurnId: TurnId,
      previousNotificationTurnId: TurnId | undefined,
      previousPromptsInFlight: number,
    ) => {
      if (
        !shouldDrainCompletedTurnLateUpdatesBeforePrompt(
          ctx,
          nextTurnId,
          previousNotificationTurnId,
          previousPromptsInFlight,
        )
      ) {
        return Effect.void;
      }
      return liveDelay(CURSOR_COMPLETED_TURN_LATE_UPDATE_GRACE_MS).pipe(
        Effect.andThen(drainEventsOrSessionEnd(ctx)),
      );
    };

    const stopSessionInternal = (
      ctx: CursorSessionContext,
      options?: { readonly emitSessionExited?: boolean },
    ) =>
      Effect.gen(function* () {
        if (ctx.stopped) return;
        ctx.stopped = true;
        yield* Deferred.succeed(ctx.stoppedSignal, undefined).pipe(Effect.ignore);
        yield* settlePendingApprovalsAsCancelled(ctx.pendingApprovals);
        yield* settlePendingUserInputsAsEmptyAnswers(ctx.pendingUserInputs);
        if (ctx.notificationFiber) {
          yield* Fiber.interrupt(ctx.notificationFiber);
        }
        yield* Effect.ignore(Scope.close(ctx.scope, Exit.void));
        if (sessions.get(ctx.threadId) === ctx) {
          sessions.delete(ctx.threadId);
        }
        if (options?.emitSessionExited !== false) {
          yield* offerRuntimeEvent({
            type: "session.exited",
            ...(yield* makeEventStamp()),
            provider: PROVIDER,
            threadId: ctx.threadId,
            payload: {
              exitKind: "graceful",
              ...(ctx.mcpProviderSessionId
                ? { mcpProviderSessionId: ctx.mcpProviderSessionId }
                : {}),
            },
          });
        }
      });

    const startSessionUnlocked = (
      input: Parameters<CursorAdapterShape["startSession"]>[0],
      internalOptions?: StartSessionInternalOptions,
    ) =>
      Effect.gen(function* () {
        if (input.provider !== undefined && input.provider !== PROVIDER) {
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "startSession",
            issue: `Expected provider '${PROVIDER}' but received '${input.provider}'.`,
          });
        }
        if (!input.cwd?.trim()) {
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "startSession",
            issue: "cwd is required and must be non-empty.",
          });
        }

        const cwd = path.resolve(input.cwd.trim());
        const cursorModelSelection =
          input.modelSelection?.instanceId === boundInstanceId ? input.modelSelection : undefined;
        const existing = sessions.get(input.threadId);
        const existingToReplace = existing && !existing.stopped ? existing : undefined;
        if (existingToReplace && internalOptions?.replaceExistingAfterStart !== true) {
          yield* stopSessionInternal(existingToReplace);
        }

        const pendingApprovals = new Map<ApprovalRequestId, PendingApproval>();
        const pendingUserInputs = new Map<ApprovalRequestId, PendingUserInput>();
        const stoppedSignal = yield* Deferred.make<void>();
        const sessionScope = yield* Scope.make("sequential");
        let sessionScopeTransferred = false;
        yield* Effect.addFinalizer(() =>
          sessionScopeTransferred ? Effect.void : Scope.close(sessionScope, Exit.void),
        );
        let ctx!: CursorSessionContext;

        const resumeSessionId = parseCursorResume(input.resumeCursor)?.sessionId;
        const acpNativeLoggers = makeAcpNativeLoggers({
          nativeEventLogger,
          provider: PROVIDER,
          threadId: input.threadId,
        });

        // Resolve the CursorSettings used to spawn the ACP child. Production
        // leaves `options.resolveSettings` undefined so we use the value
        // captured at adapter construction — per-instance isolation is
        // enforced by the hydration layer rebuilding this adapter whenever
        // its config changes. Tests set `resolveSettings` to pull the latest
        // snapshot from `ServerSettingsService` so that mid-suite
        // `updateSettings({ providers: { cursor: { binaryPath } } })` calls
        // actually take effect when the next session spawns.
        const effectiveCursorSettings = options?.resolveSettings
          ? yield* options.resolveSettings
          : cursorSettings;

        const mcpSession = McpProviderSession.readMcpProviderSession(input.threadId);
        const acp = yield* makeCursorAcpRuntime({
          cursorSettings: effectiveCursorSettings,
          ...(options?.environment ? { environment: options.environment } : {}),
          childProcessSpawner,
          cwd,
          ...(resumeSessionId ? { resumeSessionId } : {}),
          clientInfo: { name: "t3-code", version: "0.0.0" },
          ...(mcpSession
            ? {
                mcpServers: makeAcpMcpServers(mcpSession),
              }
            : {}),
          ...acpNativeLoggers,
        }).pipe(
          Effect.provideService(Crypto.Crypto, crypto),
          Effect.provideService(Scope.Scope, sessionScope),
          Effect.mapError(
            (cause) =>
              new ProviderAdapterProcessError({
                provider: PROVIDER,
                threadId: input.threadId,
                detail: cause.message,
                cause,
              }),
          ),
        );
        const started = yield* Effect.gen(function* () {
          yield* acp.handleExtRequest("cursor/ask_question", CursorAskQuestionRequest, (params) =>
            mapExtensionFailure(
              Effect.gen(function* () {
                yield* logNative(
                  input.threadId,
                  "cursor/ask_question",
                  params,
                  "acp.cursor.extension",
                );
                if (ctx && shouldDropAcpUpdateAfterLocalCancel(ctx)) {
                  return { answers: {} as ProviderUserInputAnswers };
                }
                const requestId = ApprovalRequestId.make(yield* randomUUIDv4);
                const runtimeRequestId = RuntimeRequestId.make(requestId);
                const answers = yield* Deferred.make<ProviderUserInputAnswers>();
                pendingUserInputs.set(requestId, { answers });
                if (ctx?.notificationTurnId !== undefined) {
                  ctx.turnsWithVisibleOutput.add(String(ctx.notificationTurnId));
                }
                yield* offerRuntimeEvent({
                  type: "user-input.requested",
                  ...(yield* makeEventStamp()),
                  provider: PROVIDER,
                  threadId: input.threadId,
                  turnId: ctx?.notificationTurnId,
                  requestId: runtimeRequestId,
                  payload: { questions: extractAskQuestions(params) },
                  raw: {
                    source: "acp.cursor.extension",
                    method: "cursor/ask_question",
                    payload: params,
                  },
                });
                const resolved = yield* Deferred.await(answers);
                pendingUserInputs.delete(requestId);
                yield* offerRuntimeEvent({
                  type: "user-input.resolved",
                  ...(yield* makeEventStamp()),
                  provider: PROVIDER,
                  threadId: input.threadId,
                  turnId: ctx?.notificationTurnId,
                  requestId: runtimeRequestId,
                  payload: { answers: resolved },
                });
                return { answers: resolved };
              }),
            ),
          );
          yield* acp.handleExtRequest("cursor/create_plan", CursorCreatePlanRequest, (params) =>
            mapExtensionFailure(
              Effect.gen(function* () {
                yield* logNative(
                  input.threadId,
                  "cursor/create_plan",
                  params,
                  "acp.cursor.extension",
                );
                if (ctx && shouldDropAcpUpdateAfterLocalCancel(ctx)) {
                  return { accepted: false } as const;
                }
                if (ctx?.notificationTurnId !== undefined) {
                  ctx.turnsWithVisibleOutput.add(String(ctx.notificationTurnId));
                }
                yield* offerRuntimeEvent({
                  type: "turn.proposed.completed",
                  ...(yield* makeEventStamp()),
                  provider: PROVIDER,
                  threadId: input.threadId,
                  turnId: ctx?.notificationTurnId,
                  payload: { planMarkdown: extractPlanMarkdown(params) },
                  raw: {
                    source: "acp.cursor.extension",
                    method: "cursor/create_plan",
                    payload: params,
                  },
                });
                return { accepted: true } as const;
              }),
            ),
          );
          yield* acp.handleExtNotification(
            "cursor/update_todos",
            CursorUpdateTodosRequest,
            (params) =>
              mapExtensionFailure(
                Effect.gen(function* () {
                  yield* logNative(
                    input.threadId,
                    "cursor/update_todos",
                    params,
                    "acp.cursor.extension",
                  );
                  if (ctx && shouldDropAcpUpdateAfterLocalCancel(ctx)) {
                    return;
                  }
                  if (ctx) {
                    if (ctx.notificationTurnId !== undefined) {
                      ctx.turnsWithVisibleOutput.add(String(ctx.notificationTurnId));
                    }
                    yield* emitPlanUpdate(
                      ctx,
                      extractTodosAsPlan(params),
                      params,
                      "acp.cursor.extension",
                      "cursor/update_todos",
                    );
                  }
                }),
              ),
          );
          yield* acp.handleRequestPermission((params) =>
            mapExtensionFailure(
              Effect.gen(function* () {
                yield* logNative(
                  input.threadId,
                  "session/request_permission",
                  params,
                  "acp.jsonrpc",
                );
                if (ctx && shouldDropAcpUpdateAfterLocalCancel(ctx)) {
                  return {
                    outcome: { outcome: "cancelled" } as const,
                  };
                }
                if (input.runtimeMode === "full-access") {
                  const autoApprovedOptionId = selectAutoApprovedPermissionOption(params);
                  if (autoApprovedOptionId !== undefined) {
                    return {
                      outcome: {
                        outcome: "selected" as const,
                        optionId: autoApprovedOptionId,
                      },
                    };
                  }
                }
                const permissionRequest = parsePermissionRequest(params);
                const requestId = ApprovalRequestId.make(yield* randomUUIDv4);
                const runtimeRequestId = RuntimeRequestId.make(requestId);
                if (ctx?.notificationTurnId !== undefined) {
                  ctx.turnsWithVisibleOutput.add(String(ctx.notificationTurnId));
                }
                const decision = yield* Deferred.make<ProviderApprovalDecision>();
                pendingApprovals.set(requestId, {
                  decision,
                  kind: permissionRequest.kind,
                });
                yield* offerRuntimeEvent(
                  makeAcpRequestOpenedEvent({
                    stamp: yield* makeEventStamp(),
                    provider: PROVIDER,
                    threadId: input.threadId,
                    turnId: ctx?.notificationTurnId,
                    requestId: runtimeRequestId,
                    permissionRequest,
                    detail:
                      permissionRequest.detail ??
                      encodeJsonStringForDiagnostics(params)?.slice(0, 2000) ??
                      "[unserializable params]",
                    args: params,
                    source: "acp.jsonrpc",
                    method: "session/request_permission",
                    rawPayload: params,
                  }),
                );
                const resolved = yield* Deferred.await(decision);
                pendingApprovals.delete(requestId);
                yield* offerRuntimeEvent(
                  makeAcpRequestResolvedEvent({
                    stamp: yield* makeEventStamp(),
                    provider: PROVIDER,
                    threadId: input.threadId,
                    turnId: ctx?.notificationTurnId,
                    requestId: runtimeRequestId,
                    permissionRequest,
                    decision: resolved,
                  }),
                );
                return {
                  outcome:
                    resolved === "cancel"
                      ? ({ outcome: "cancelled" } as const)
                      : {
                          outcome: "selected" as const,
                          optionId: acpPermissionOutcome(resolved),
                        },
                };
              }),
            ),
          );
          return yield* acp.start();
        }).pipe(
          Effect.mapError((error) =>
            mapAcpToAdapterError(PROVIDER, input.threadId, "session/start", error),
          ),
        );

        yield* applyRequestedSessionConfiguration({
          runtime: acp,
          runtimeMode: input.runtimeMode,
          interactionMode: undefined,
          modelSelection: cursorModelSelection,
          mapError: ({ cause, method }) =>
            mapAcpToAdapterError(PROVIDER, input.threadId, method, cause),
        });

        const now = yield* nowIso;
        const session: ProviderSession = {
          provider: PROVIDER,
          providerInstanceId: boundInstanceId,
          status: input.activeTurnId !== undefined ? "running" : "ready",
          runtimeMode: input.runtimeMode,
          cwd,
          model: cursorModelSelection?.model,
          threadId: input.threadId,
          resumeCursor: {
            schemaVersion: CURSOR_RESUME_VERSION,
            sessionId: started.sessionId,
          },
          ...(input.activeTurnId !== undefined ? { activeTurnId: input.activeTurnId } : {}),
          createdAt: now,
          updatedAt: now,
        };

        ctx = {
          threadId: input.threadId,
          session,
          scope: sessionScope,
          acp,
          ...(mcpSession ? { mcpProviderSessionId: mcpSession.providerSessionId } : {}),
          notificationFiber: undefined,
          pendingApprovals,
          pendingUserInputs,
          turns:
            internalOptions?.initialTurns?.map((turn) => ({
              id: turn.id,
              items: [...turn.items],
            })) ?? [],
          lastPlanFingerprint: undefined,
          activeTurnId: input.activeTurnId,
          notificationTurnId: input.activeTurnId,
          lastModelSelection: cloneCursorModelSelection(cursorModelSelection),
          dropAcpUpdatesAfterLocalCancel:
            internalOptions?.initialDropAcpUpdatesAfterLocalCancel ?? false,
          suppressedNotificationTurnIds: new Set<string>(
            internalOptions?.initialSuppressedNotificationTurnIds ?? [],
          ),
          preCompletedCancelledTurnIds: new Set<string>(),
          turnsWithVisibleOutput: new Set<string>(),
          pendingPromptTurnId: undefined,
          localCancelRequestsInFlight: 0,
          locallyCancelledPromptsInFlight: 0,
          localCancelSettled: undefined,
          promptStartSettled: undefined,
          promptStartedDuringLocalCancel: false,
          restartBeforeNextPrompt: false,
          stoppedSignal,
          promptsInFlight: 0,
          stopped: false,
        };

        const nf = yield* Stream.runDrain(
          Stream.mapEffect(acp.getEvents(), (event) =>
            Effect.gen(function* () {
              switch (event._tag) {
                case "EventStreamBarrier":
                  yield* Deferred.succeed(event.acknowledge, undefined);
                  return;
                case "ModeChanged":
                  return;
              }
              if (shouldDropAcpUpdateAfterLocalCancel(ctx)) {
                return;
              }
              const eventTurnId = ctx.notificationTurnId;
              switch (event._tag) {
                case "AssistantItemStarted":
                  yield* offerRuntimeEvent(
                    makeAcpAssistantItemEvent({
                      stamp: yield* makeEventStamp(),
                      provider: PROVIDER,
                      threadId: ctx.threadId,
                      turnId: eventTurnId,
                      itemId: event.itemId,
                      lifecycle: "item.started",
                    }),
                  );
                  return;
                case "AssistantItemCompleted":
                  yield* offerRuntimeEvent(
                    makeAcpAssistantItemEvent({
                      stamp: yield* makeEventStamp(),
                      provider: PROVIDER,
                      threadId: ctx.threadId,
                      turnId: eventTurnId,
                      itemId: event.itemId,
                      lifecycle: "item.completed",
                    }),
                  );
                  return;
                case "PlanUpdated":
                  if (eventTurnId !== undefined) {
                    ctx.turnsWithVisibleOutput.add(String(eventTurnId));
                  }
                  yield* logNative(ctx.threadId, "session/update", event.rawPayload, "acp.jsonrpc");
                  yield* emitPlanUpdate(
                    ctx,
                    event.payload,
                    event.rawPayload,
                    "acp.jsonrpc",
                    "session/update",
                  );
                  return;
                case "ToolCallUpdated":
                  if (eventTurnId !== undefined) {
                    ctx.turnsWithVisibleOutput.add(String(eventTurnId));
                  }
                  yield* logNative(ctx.threadId, "session/update", event.rawPayload, "acp.jsonrpc");
                  yield* offerRuntimeEvent(
                    makeAcpToolCallEvent({
                      stamp: yield* makeEventStamp(),
                      provider: PROVIDER,
                      threadId: ctx.threadId,
                      turnId: eventTurnId,
                      toolCall: event.toolCall,
                      rawPayload: event.rawPayload,
                    }),
                  );
                  return;
                case "ContentDelta":
                  if (eventTurnId !== undefined && event.text.trim().length > 0) {
                    ctx.turnsWithVisibleOutput.add(String(eventTurnId));
                  }
                  yield* logNative(ctx.threadId, "session/update", event.rawPayload, "acp.jsonrpc");
                  yield* offerRuntimeEvent(
                    makeAcpContentDeltaEvent({
                      stamp: yield* makeEventStamp(),
                      provider: PROVIDER,
                      threadId: ctx.threadId,
                      turnId: eventTurnId,
                      ...(event.itemId ? { itemId: event.itemId } : {}),
                      text: event.text,
                      rawPayload: event.rawPayload,
                    }),
                  );
                  return;
              }
            }),
          ),
        ).pipe(
          Effect.catch((cause) =>
            Effect.logError("Failed to process Cursor runtime notification.", { cause }),
          ),
          Effect.forkChild,
        );

        ctx.notificationFiber = nf;
        sessions.set(input.threadId, ctx);
        sessionScopeTransferred = true;
        if (
          internalOptions?.replaceExistingAfterStart === true &&
          existingToReplace !== undefined &&
          existingToReplace !== ctx &&
          !existingToReplace.stopped
        ) {
          const stopReplacedSession = stopSessionInternal(
            existingToReplace,
            internalOptions.emitReplacedSessionExited === undefined
              ? undefined
              : { emitSessionExited: internalOptions.emitReplacedSessionExited },
          );
          if (internalOptions.detachReplacedSessionStop === true) {
            yield* stopReplacedSession.pipe(
              Effect.ignore,
              Effect.forkDetach({ startImmediately: true }),
            );
          } else {
            yield* stopReplacedSession;
          }
        }

        yield* offerRuntimeEvent({
          type: "session.started",
          ...(yield* makeEventStamp()),
          provider: PROVIDER,
          threadId: input.threadId,
          payload: { resume: started.initializeResult },
        });
        const sessionStatePayload =
          input.activeTurnId !== undefined
            ? {
                state: "running" as const,
                reason: "Cursor ACP session resumed with active turn",
              }
            : {
                state: "ready" as const,
                reason: "Cursor ACP session ready",
              };
        yield* offerRuntimeEvent({
          type: "session.state.changed",
          ...(yield* makeEventStamp()),
          provider: PROVIDER,
          threadId: input.threadId,
          payload:
            internalOptions?.replaceExistingAfterStart === true
              ? {
                  ...sessionStatePayload,
                  detail: { resumeCursor: session.resumeCursor },
                }
              : sessionStatePayload,
        });
        yield* offerRuntimeEvent({
          type: "thread.started",
          ...(yield* makeEventStamp()),
          provider: PROVIDER,
          threadId: input.threadId,
          payload: { providerThreadId: started.sessionId },
        });

        return session;
      }).pipe(Effect.scoped);

    const startSession: CursorAdapterShape["startSession"] = (input) =>
      withThreadLock(input.threadId, startSessionUnlocked(input));

    const restartSessionBeforeNextPrompt = (threadId: ThreadId) =>
      withThreadLock(
        threadId,
        Effect.gen(function* () {
          const ctx = yield* requireSession(threadId);
          if (!ctx.restartBeforeNextPrompt) {
            return ctx;
          }
          const previousSession = ctx.session;
          const previousModelSelection =
            cloneCursorModelSelection(ctx.lastModelSelection) ??
            (previousSession.model
              ? {
                  instanceId: boundInstanceId,
                  model: previousSession.model,
                }
              : undefined);
          const previousTurns = ctx.turns.map((turn) => ({
            id: turn.id,
            items: [...turn.items],
          }));
          yield* startSessionUnlocked(
            {
              threadId,
              provider: PROVIDER,
              cwd: previousSession.cwd,
              runtimeMode: previousSession.runtimeMode,
              ...(previousModelSelection ? { modelSelection: previousModelSelection } : {}),
              resumeCursor: previousSession.resumeCursor,
            },
            {
              replaceExistingAfterStart: true,
              emitReplacedSessionExited: false,
              detachReplacedSessionStop: true,
              initialTurns: previousTurns,
              initialDropAcpUpdatesAfterLocalCancel: true,
              initialSuppressedNotificationTurnIds: ctx.suppressedNotificationTurnIds,
            },
          );
          ctx.restartBeforeNextPrompt = false;
          const restartedCtx = yield* requireSession(threadId);
          if (restartedCtx.dropAcpUpdatesAfterLocalCancel) {
            yield* liveDelay(CURSOR_COMPLETED_TURN_LATE_UPDATE_GRACE_MS);
            yield* drainLocalCancelEventsOrTimeout(restartedCtx);
          }
          return restartedCtx;
        }),
      );

    const sendTurn: CursorAdapterShape["sendTurn"] = (input) =>
      Effect.gen(function* () {
        let ctx = yield* requireSession(input.threadId);
        if (ctx.promptStartSettled !== undefined) {
          yield* Deferred.await(ctx.promptStartSettled);
          ctx = yield* requireSession(input.threadId);
        }
        if (ctx.localCancelSettled !== undefined) {
          yield* Deferred.await(ctx.localCancelSettled);
        }
        ctx = yield* restartSessionBeforeNextPrompt(input.threadId);
        if (ctx.promptStartSettled !== undefined) {
          yield* Deferred.await(ctx.promptStartSettled);
          ctx = yield* requireSession(input.threadId);
        }
        // A sendTurn during active work is a steer: the agent folds the new
        // prompt into the ongoing work, so the active turn id is reused
        // instead of opening a new turn.
        const steeringTurnId =
          ctx.promptsInFlight > 0 || ctx.session.status === "running"
            ? ctx.activeTurnId
            : undefined;
        const turnId = steeringTurnId ?? TurnId.make(yield* randomUUIDv4);
        const previousActiveTurnId = ctx.activeTurnId;
        const previousNotificationTurnId = ctx.notificationTurnId;
        const previousDropAcpUpdatesAfterLocalCancel = ctx.dropAcpUpdatesAfterLocalCancel;
        const previousLocalCancelRequestsInFlight = ctx.localCancelRequestsInFlight;
        const previousLocallyCancelledPromptsInFlight = ctx.locallyCancelledPromptsInFlight;
        const previousPromptStartedDuringLocalCancel = ctx.promptStartedDuringLocalCancel;
        const previousRestartBeforeNextPrompt = ctx.restartBeforeNextPrompt;
        const previousPendingPromptTurnId = ctx.pendingPromptTurnId;
        const previousPromptsInFlight = ctx.promptsInFlight;
        const previousSession = ctx.session;
        const previousLastPlanFingerprint = ctx.lastPlanFingerprint;
        const previousLastModelSelection = cloneCursorModelSelection(ctx.lastModelSelection);
        let activeStateApplied = false;
        let promptCounted = false;
        const needsCompletedTurnDrain =
          steeringTurnId === undefined &&
          shouldDrainCompletedTurnLateUpdatesBeforePrompt(
            ctx,
            turnId,
            previousNotificationTurnId,
            previousPromptsInFlight,
          );
        const promptStartSettled = needsCompletedTurnDrain
          ? yield* Deferred.make<void>()
          : undefined;
        if (promptStartSettled !== undefined) {
          ctx.promptStartSettled = promptStartSettled;
        }
        const countPromptInFlight = () => {
          if (!promptCounted) {
            ctx.promptsInFlight += 1;
            promptCounted = true;
          }
        };
        // Reserve every prompt before async configuration so concurrent sends
        // classify themselves as steering instead of opening parallel turns.
        countPromptInFlight();
        if (steeringTurnId === undefined) {
          ctx.pendingPromptTurnId = turnId;
        }

        return yield* Effect.gen(function* () {
          const turnModelSelection =
            input.modelSelection?.instanceId === boundInstanceId ? input.modelSelection : undefined;
          const effectiveModelSelection = turnModelSelection ?? ctx.lastModelSelection;
          const model = effectiveModelSelection?.model ?? ctx.session.model;
          const resolvedModel = resolveCursorAcpBaseModelId(model);
          yield* applyRequestedSessionConfiguration({
            runtime: ctx.acp,
            runtimeMode: ctx.session.runtimeMode,
            interactionMode: input.interactionMode,
            modelSelection:
              model === undefined
                ? undefined
                : {
                    model,
                    options: effectiveModelSelection?.options,
                  },
            mapError: ({ cause, method }) =>
              mapAcpToAdapterError(PROVIDER, input.threadId, method, cause),
          });
          if (turnModelSelection !== undefined) {
            ctx.lastModelSelection = cloneCursorModelSelection(turnModelSelection);
          } else if (ctx.lastModelSelection === undefined && model !== undefined) {
            ctx.lastModelSelection = { instanceId: boundInstanceId, model };
          }
          if (needsCompletedTurnDrain) {
            yield* drainCompletedTurnLateUpdatesBeforePrompt(
              ctx,
              turnId,
              previousNotificationTurnId,
              previousPromptsInFlight,
            );
          }
          ctx.activeTurnId = turnId;
          if (ctx.pendingPromptTurnId === turnId) {
            ctx.pendingPromptTurnId = undefined;
          }
          const localCancelSuppressionActive =
            ctx.dropAcpUpdatesAfterLocalCancel ||
            ctx.localCancelRequestsInFlight > 0 ||
            ctx.locallyCancelledPromptsInFlight > 0;
          if (
            previousPromptsInFlight === 0 &&
            ctx.localCancelRequestsInFlight === 0 &&
            ctx.locallyCancelledPromptsInFlight === 0
          ) {
            ctx.dropAcpUpdatesAfterLocalCancel = false;
            ctx.promptStartedDuringLocalCancel = false;
          } else if (localCancelSuppressionActive) {
            ctx.dropAcpUpdatesAfterLocalCancel = true;
            ctx.promptStartedDuringLocalCancel = true;
          }
          if (steeringTurnId === undefined) {
            ctx.lastPlanFingerprint = undefined;
          }
          ctx.session = {
            ...ctx.session,
            status: "running",
            activeTurnId: turnId,
            updatedAt: yield* nowIso,
          };
          activeStateApplied = true;

          if (steeringTurnId === undefined) {
            yield* offerRuntimeEvent({
              type: "turn.started",
              ...(yield* makeEventStamp()),
              provider: PROVIDER,
              threadId: input.threadId,
              turnId,
              payload: { model: resolvedModel },
            });
          }

          const promptParts: Array<EffectAcpSchema.ContentBlock> = [];
          if (input.input?.trim()) {
            promptParts.push({ type: "text", text: input.input.trim() });
          }
          if (input.attachments && input.attachments.length > 0) {
            for (const attachment of input.attachments) {
              const attachmentPath = resolveAttachmentPath({
                attachmentsDir: serverConfig.attachmentsDir,
                attachment,
              });
              if (!attachmentPath) {
                return yield* new ProviderAdapterRequestError({
                  provider: PROVIDER,
                  method: "session/prompt",
                  detail: `Invalid attachment id '${attachment.id}'.`,
                });
              }
              const bytes = yield* fileSystem.readFile(attachmentPath).pipe(
                Effect.mapError(
                  (cause) =>
                    new ProviderAdapterRequestError({
                      provider: PROVIDER,
                      method: "session/prompt",
                      detail: cause.message,
                      cause,
                    }),
                ),
              );
              promptParts.push({
                type: "image",
                data: Buffer.from(bytes).toString("base64"),
                mimeType: attachment.mimeType,
              });
            }
          }

          if (promptParts.length === 0) {
            return yield* new ProviderAdapterValidationError({
              provider: PROVIDER,
              operation: "sendTurn",
              issue: "Turn requires non-empty text or attachments.",
            });
          }

          ctx.notificationTurnId = turnId;
          yield* releasePromptStartSettled(ctx, promptStartSettled);
          if (
            ctx.stopped ||
            ctx.restartBeforeNextPrompt ||
            ctx.localCancelRequestsInFlight > 0 ||
            ctx.locallyCancelledPromptsInFlight > 0
          ) {
            ctx.locallyCancelledPromptsInFlight = Math.max(
              0,
              ctx.locallyCancelledPromptsInFlight - 1,
            );
            if (
              !ctx.stopped &&
              activeStateApplied &&
              ctx.activeTurnId === turnId &&
              ctx.promptsInFlight === 1
            ) {
              const { activeTurnId: _cancelledActiveTurnId, ...sessionWithoutActiveTurn } =
                ctx.session;
              ctx.activeTurnId = undefined;
              ctx.turnsWithVisibleOutput.delete(String(turnId));
              ctx.session = {
                ...sessionWithoutActiveTurn,
                status: "ready",
                updatedAt: yield* nowIso,
              };
              yield* offerRuntimeEvent({
                type: "turn.completed",
                ...(yield* makeEventStamp()),
                provider: PROVIDER,
                threadId: input.threadId,
                turnId,
                payload: {
                  state: "cancelled",
                  stopReason: "cancelled",
                },
              });
            }
            return {
              threadId: input.threadId,
              turnId,
              resumeCursor: ctx.session.resumeCursor,
            };
          }

          const result = yield* ctx.acp
            .prompt({
              prompt: promptParts,
            })
            .pipe(
              Effect.mapError((error) =>
                mapAcpToAdapterError(PROVIDER, input.threadId, "session/prompt", error),
              ),
            );

          const turnRecord = ctx.turns.find((turn) => turn.id === turnId);
          if (turnRecord) {
            turnRecord.items.push({ prompt: promptParts, result });
          } else {
            ctx.turns.push({ id: turnId, items: [{ prompt: promptParts, result }] });
          }
          // Only the last remaining prompt settles the turn — a steer-
          // superseded prompt resolving (usually cancelled) while another is
          // in flight or pending must leave the merged turn running.
          const locallyCancelledPrompt = ctx.locallyCancelledPromptsInFlight > 0;
          let cancelledResult = result.stopReason === "cancelled" || locallyCancelledPrompt;
          if (locallyCancelledPrompt) {
            yield* drainLocalCancelEventsOrTimeout(ctx);
            ctx.locallyCancelledPromptsInFlight = Math.max(
              0,
              ctx.locallyCancelledPromptsInFlight - 1,
            );
            clearLocalCancelDropForQueuedPrompt(ctx);
          }
          if (ctx.promptsInFlight === 1) {
            if (ctx.stopped) {
              return {
                threadId: input.threadId,
                turnId,
                resumeCursor: ctx.session.resumeCursor,
              };
            }
            if (!cancelledResult) {
              yield* drainEventsOrSessionEnd(ctx);
              if (
                !ctx.stopped &&
                ctx.promptsInFlight === 1 &&
                ctx.activeTurnId === turnId &&
                ctx.session.activeTurnId === turnId &&
                ctx.localCancelRequestsInFlight === 0 &&
                ctx.locallyCancelledPromptsInFlight === 0 &&
                !ctx.turnsWithVisibleOutput.has(String(turnId))
              ) {
                yield* liveDelay(CURSOR_COMPLETED_TURN_LATE_UPDATE_GRACE_MS);
                yield* drainEventsOrSessionEnd(ctx);
              }
            }
            if (!cancelledResult && ctx.locallyCancelledPromptsInFlight > 0) {
              yield* drainLocalCancelEventsOrTimeout(ctx);
              ctx.locallyCancelledPromptsInFlight = Math.max(
                0,
                ctx.locallyCancelledPromptsInFlight - 1,
              );
              clearLocalCancelDropForQueuedPrompt(ctx);
              cancelledResult = true;
            }
            if (ctx.stopped) {
              return {
                threadId: input.threadId,
                turnId,
                resumeCursor: ctx.session.resumeCursor,
              };
            }
            const turnStillOwnsSettlement =
              ctx.promptsInFlight === 1 &&
              ctx.activeTurnId === turnId &&
              ctx.session.activeTurnId === turnId;
            if (!turnStillOwnsSettlement) {
              if (ctx.activeTurnId === turnId && ctx.session.activeTurnId === turnId) {
                ctx.session = {
                  ...ctx.session,
                  status: "running",
                  updatedAt: yield* nowIso,
                  model: resolvedModel,
                };
              }
              return {
                threadId: input.threadId,
                turnId,
                resumeCursor: ctx.session.resumeCursor,
              };
            }
            const { activeTurnId: _completedActiveTurnId, ...sessionWithoutActiveTurn } =
              ctx.session;
            const completionAlreadyEmitted =
              cancelledResult && ctx.preCompletedCancelledTurnIds.has(String(turnId));
            const completedSession = {
              ...sessionWithoutActiveTurn,
              status: "ready",
              updatedAt: yield* nowIso,
              model: resolvedModel,
            } satisfies ProviderSession;
            // Defensive invariant: a non-cancelled turn that never produced
            // any user-visible assistant output must fail loudly. Persisting
            // a clean completion here would render as a silent empty response.
            const silentlyEmptyTurn =
              !cancelledResult && !ctx.turnsWithVisibleOutput.has(String(turnId));
            if (!completionAlreadyEmitted) {
              yield* offerRuntimeEvent({
                type: "turn.completed",
                ...(yield* makeEventStamp()),
                provider: PROVIDER,
                threadId: input.threadId,
                turnId,
                payload: cancelledResult
                  ? {
                      state: "cancelled",
                      stopReason: "cancelled",
                    }
                  : silentlyEmptyTurn
                    ? {
                        state: "failed",
                        stopReason: result.stopReason ?? null,
                        errorMessage:
                          "Cursor completed the turn without returning any assistant output. The response was empty or was not delivered to this session.",
                      }
                    : {
                        state: "completed",
                        stopReason: result.stopReason ?? null,
                      },
              });
            }
            ctx.preCompletedCancelledTurnIds.delete(String(turnId));
            ctx.turnsWithVisibleOutput.delete(String(turnId));
            ctx.activeTurnId = undefined;
            ctx.notificationTurnId = turnId;
            ctx.dropAcpUpdatesAfterLocalCancel = cancelledResult;
            ctx.session = completedSession;
          } else {
            ctx.session = {
              ...ctx.session,
              status: "running",
              activeTurnId: turnId,
              updatedAt: yield* nowIso,
              model: resolvedModel,
            };
          }

          return {
            threadId: input.threadId,
            turnId,
            resumeCursor: ctx.session.resumeCursor,
          };
        }).pipe(
          Effect.onError(() =>
            Effect.sync(() => {
              if (
                activeStateApplied &&
                ctx.promptsInFlight === 1 &&
                ctx.activeTurnId === turnId &&
                ctx.session.activeTurnId === turnId
              ) {
                ctx.activeTurnId = previousActiveTurnId;
                ctx.notificationTurnId = previousNotificationTurnId;
                ctx.dropAcpUpdatesAfterLocalCancel = previousDropAcpUpdatesAfterLocalCancel;
                ctx.localCancelRequestsInFlight = previousLocalCancelRequestsInFlight;
                ctx.locallyCancelledPromptsInFlight = previousLocallyCancelledPromptsInFlight;
                ctx.promptStartedDuringLocalCancel = previousPromptStartedDuringLocalCancel;
                ctx.restartBeforeNextPrompt = previousRestartBeforeNextPrompt;
                ctx.pendingPromptTurnId = previousPendingPromptTurnId;
                ctx.session = previousSession;
                ctx.lastPlanFingerprint = previousLastPlanFingerprint;
                ctx.lastModelSelection = previousLastModelSelection;
              }
            }),
          ),
          Effect.ensuring(
            Effect.gen(function* () {
              if (promptCounted) {
                ctx.promptsInFlight = Math.max(0, ctx.promptsInFlight - 1);
              }
              if (ctx.pendingPromptTurnId === turnId) {
                ctx.pendingPromptTurnId = undefined;
              }
              yield* releasePromptStartSettled(ctx, promptStartSettled);
              clearLocalCancelDropForQueuedPrompt(ctx);
              yield* completeLocalCancelSettled(ctx);
            }),
          ),
        );
      });

    const interruptTurn: CursorAdapterShape["interruptTurn"] = (threadId) =>
      Effect.gen(function* () {
        const ctx = yield* requireSession(threadId);
        const resumedTurnToCancel =
          ctx.promptsInFlight === 0 && ctx.activeTurnId !== undefined
            ? ctx.activeTurnId
            : undefined;
        const hasPendingInteraction =
          ctx.pendingApprovals.size > 0 || ctx.pendingUserInputs.size > 0;
        const hasActiveWorkToCancel =
          ctx.promptsInFlight > 0 || resumedTurnToCancel !== undefined || hasPendingInteraction;
        if (!hasActiveWorkToCancel) {
          return;
        }
        ctx.dropAcpUpdatesAfterLocalCancel = true;
        const turnToSuppress = ctx.activeTurnId ?? ctx.pendingPromptTurnId ?? resumedTurnToCancel;
        if (turnToSuppress !== undefined) {
          ctx.suppressedNotificationTurnIds.add(String(turnToSuppress));
          if (!ctx.turns.some((turn) => String(turn.id) === String(turnToSuppress))) {
            ctx.turns.push({ id: turnToSuppress, items: [] });
          }
        }
        if (ctx.localCancelSettled === undefined) {
          ctx.localCancelSettled = yield* Deferred.make<void>();
        }
        ctx.restartBeforeNextPrompt = true;
        ctx.localCancelRequestsInFlight += 1;
        const promptsToSuppress = ctx.promptsInFlight;
        ctx.locallyCancelledPromptsInFlight = Math.max(
          ctx.locallyCancelledPromptsInFlight,
          promptsToSuppress,
        );
        yield* settlePendingApprovalsAsCancelled(ctx.pendingApprovals);
        yield* settlePendingUserInputsAsEmptyAnswers(ctx.pendingUserInputs);
        const cancelEffect =
          ctx.promptsInFlight > 0 && hasPendingInteraction ? ctx.acp.requestCancel : ctx.acp.cancel;
        const cancelAndDrain = cancelEffect.pipe(
          Effect.mapError((error) =>
            mapAcpToAdapterError(PROVIDER, threadId, "session/cancel", error),
          ),
          Effect.andThen(drainEventsOrSessionEnd(ctx)),
        );
        yield* Effect.ignore(
          Effect.raceFirst(cancelAndDrain, liveDelay(CURSOR_CANCEL_REQUEST_TIMEOUT_MS)),
        ).pipe(
          Effect.ensuring(
            Effect.sync(() => {
              ctx.localCancelRequestsInFlight = Math.max(0, ctx.localCancelRequestsInFlight - 1);
            }),
          ),
        );
        yield* releaseLocalCancelSettledForRestart(ctx);
        if (
          resumedTurnToCancel !== undefined &&
          ctx.promptsInFlight === 0 &&
          ctx.activeTurnId === resumedTurnToCancel
        ) {
          const { activeTurnId: _cancelledActiveTurnId, ...sessionWithoutActiveTurn } = ctx.session;
          ctx.activeTurnId = undefined;
          ctx.notificationTurnId = resumedTurnToCancel;
          ctx.turnsWithVisibleOutput.delete(String(resumedTurnToCancel));
          ctx.session = {
            ...sessionWithoutActiveTurn,
            status: "ready",
            updatedAt: yield* nowIso,
          };
          yield* offerRuntimeEvent({
            type: "turn.completed",
            ...(yield* makeEventStamp()),
            provider: PROVIDER,
            threadId,
            turnId: resumedTurnToCancel,
            payload: {
              state: "cancelled",
              stopReason: "cancelled",
            },
          });
        }
        clearLocalCancelDropForQueuedPrompt(ctx);
        yield* completeLocalCancelSettled(ctx);
      });

    const respondToRequest: CursorAdapterShape["respondToRequest"] = (
      threadId,
      requestId,
      decision,
    ) =>
      Effect.gen(function* () {
        const ctx = yield* requireSession(threadId);
        const pending = ctx.pendingApprovals.get(requestId);
        if (!pending) {
          return yield* new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "session/request_permission",
            detail: `Unknown pending approval request: ${requestId}`,
          });
        }
        yield* Deferred.succeed(pending.decision, decision);
      });

    const respondToUserInput: CursorAdapterShape["respondToUserInput"] = (
      threadId,
      requestId,
      answers,
    ) =>
      Effect.gen(function* () {
        const ctx = yield* requireSession(threadId);
        const pending = ctx.pendingUserInputs.get(requestId);
        if (!pending) {
          return yield* new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "cursor/ask_question",
            detail: `Unknown pending user-input request: ${requestId}`,
          });
        }
        yield* Deferred.succeed(pending.answers, answers);
      });

    const readThread: CursorAdapterShape["readThread"] = (threadId) =>
      Effect.gen(function* () {
        const ctx = yield* requireSession(threadId);
        return { threadId, turns: ctx.turns };
      });

    const rollbackThread: CursorAdapterShape["rollbackThread"] = (threadId, numTurns) =>
      Effect.gen(function* () {
        const ctx = yield* requireSession(threadId);
        if (!Number.isInteger(numTurns) || numTurns < 1) {
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "rollbackThread",
            issue: "numTurns must be an integer >= 1.",
          });
        }
        const nextLength = Math.max(0, ctx.turns.length - numTurns);
        ctx.turns.splice(nextLength);
        return { threadId, turns: ctx.turns };
      });

    const stopSession: CursorAdapterShape["stopSession"] = (threadId) =>
      withThreadLock(
        threadId,
        Effect.gen(function* () {
          const ctx = yield* requireSession(threadId);
          yield* stopSessionInternal(ctx);
        }),
      );

    const listSessions: CursorAdapterShape["listSessions"] = () =>
      Effect.sync(() => Array.from(sessions.values(), (c) => ({ ...c.session })));

    const hasSession: CursorAdapterShape["hasSession"] = (threadId) =>
      Effect.sync(() => {
        const c = sessions.get(threadId);
        return c !== undefined && !c.stopped;
      });

    const stopAll: CursorAdapterShape["stopAll"] = () =>
      Effect.forEach(sessions.values(), (ctx) => stopSessionInternal(ctx), { discard: true });

    yield* Effect.addFinalizer(() =>
      Effect.forEach(sessions.values(), (ctx) => stopSessionInternal(ctx), { discard: true }).pipe(
        Effect.catch((cause) =>
          Effect.logError("Failed to emit Cursor session shutdown event.", { cause }),
        ),
        Effect.tap(() => PubSub.shutdown(runtimeEventPubSub)),
        Effect.tap(() => managedNativeEventLogger?.close() ?? Effect.void),
      ),
    );

    const streamEvents = Stream.fromPubSub(runtimeEventPubSub);

    return {
      provider: PROVIDER,
      capabilities: { sessionModelSwitch: "in-session" },
      startSession,
      sendTurn,
      interruptTurn,
      readThread,
      rollbackThread,
      respondToRequest,
      respondToUserInput,
      stopSession,
      listSessions,
      hasSession,
      stopAll,
      streamEvents,
    } satisfies CursorAdapterShape;
  });
}
