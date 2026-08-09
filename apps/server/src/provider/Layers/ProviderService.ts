/**
 * ProviderServiceLive - Cross-provider orchestration layer.
 *
 * Routes validated transport/API calls to provider adapters through
 * `ProviderAdapterRegistry` and `ProviderSessionDirectory`, and exposes a
 * unified provider event stream for subscribers.
 *
 * It does not implement provider protocol details (adapter concern).
 *
 * @module ProviderServiceLive
 */
import * as NodeCrypto from "node:crypto";
import {
  ModelSelection,
  NonNegativeInt,
  ThreadId,
  TurnId,
  ProviderInterruptTurnInput,
  ProviderRespondToRequestInput,
  ProviderRespondToUserInputInput,
  ProviderSendTurnInput,
  ProviderSessionStartInput,
  ProviderStopSessionInput,
  type ProviderInstanceId,
  type ProviderDriverKind,
  type ProviderRuntimeEvent,
  type ProviderSession,
} from "@t3tools/contracts";
import { causeErrorTag } from "@t3tools/shared/observability";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Ref from "effect/Ref";
import * as Semaphore from "effect/Semaphore";
import * as Schema from "effect/Schema";
import * as SchemaIssue from "effect/SchemaIssue";
import * as Stream from "effect/Stream";
import * as SynchronizedRef from "effect/SynchronizedRef";

import { resolveAttachmentPath } from "../../attachmentStore.ts";
import * as ServerConfig from "../../config.ts";
import {
  increment,
  providerMetricAttributes,
  providerRuntimeEventsTotal,
  providerSessionsTotal,
  providerTurnDuration,
  providerTurnsTotal,
  providerTurnMetricAttributes,
  withMetrics,
} from "../../observability/Metrics.ts";
import {
  type ProviderAdapterError,
  ProviderSessionStartTimeoutError,
  ProviderSendTurnFailedError,
  ProviderValidationError,
} from "../Errors.ts";
import type { ProviderAdapterShape } from "../Services/ProviderAdapter.ts";
import * as ProviderAdapterRegistry from "../Services/ProviderAdapterRegistry.ts";
import * as ProviderService from "../Services/ProviderService.ts";
import * as ProviderSessionDirectory from "../Services/ProviderSessionDirectory.ts";
import {
  captureProviderRuntimeEventBinding,
  type CapturedProviderRuntimeEventBinding,
} from "../runtimeEventBindingRegistry.ts";
import { type EventNdjsonLogger } from "./EventNdjsonLogger.ts";
import * as ProviderEventLoggers from "./ProviderEventLoggers.ts";
import { PROVIDER_EMPTY_RESPONSE_ERROR } from "./providerFailureMessages.ts";
import * as AnalyticsService from "../../telemetry/AnalyticsService.ts";
import * as McpProviderSession from "../../mcp/McpProviderSession.ts";
import * as McpSessionRegistry from "../../mcp/McpSessionRegistry.ts";
const isModelSelection = Schema.is(ModelSelection);
const isTurnId = Schema.is(TurnId);

const supportsActiveTurnResume = (provider: ProviderDriverKind): boolean => provider === "cursor";

// Cursor intentionally keeps the completed turn as the notification owner for
// this 200ms late-update window before a follow-up prompt can start. Mirror that
// existing adapter boundary here so the empty-response decision observes every
// delta that Cursor can still attribute to the completed turn.
const emptyResponseCompletionGraceMs = (provider: ProviderDriverKind): number | undefined =>
  provider === "cursor" ? 200 : undefined;

/**
 * Hook for tests that want to override the canonical event logger pulled
 * from `ProviderEventLoggers`. Production wiring leaves this undefined and
 * reads the logger off the tag.
 */
export interface ProviderServiceLiveOptions {
  readonly canonicalEventLogger?: EventNdjsonLogger;
  /** Maximum time external provider startup may hold the per-thread recovery lock. */
  readonly sessionStartTimeoutMs?: number;
}

type ProviderServiceMethod<Name extends keyof ProviderService.ProviderService["Service"]> =
  ProviderService.ProviderService["Service"][Name];

const ProviderRollbackConversationInput = Schema.Struct({
  threadId: ThreadId,
  numTurns: NonNegativeInt,
});

function toValidationError(
  operation: string,
  issue: string,
  cause?: unknown,
): ProviderValidationError {
  return new ProviderValidationError({
    operation,
    issue,
    ...(cause !== undefined ? { cause } : {}),
  });
}

const decodeInputOrValidationError = <S extends Schema.Top>(input: {
  readonly operation: string;
  readonly schema: S;
  readonly payload: unknown;
}) => {
  const decodeProviderRequestInput = Schema.decodeUnknownEffect(input.schema);
  return decodeProviderRequestInput(input.payload).pipe(
    Effect.mapError(
      (schemaError) =>
        new ProviderValidationError({
          operation: input.operation,
          issue: SchemaIssue.makeFormatterDefault()(schemaError.issue),
          cause: schemaError,
        }),
    ),
  );
};

function toRuntimeStatus(
  session: ProviderSession,
): "starting" | "running" | "waiting" | "stopped" | "error" {
  switch (session.status) {
    case "connecting":
      return "starting";
    case "waiting":
      return "waiting";
    case "error":
      return "error";
    case "closed":
      return "stopped";
    case "ready":
    case "running":
    default:
      return "running";
  }
}

function runtimeStatusFromSessionState(
  state: Extract<ProviderRuntimeEvent, { type: "session.state.changed" }>["payload"]["state"],
): "starting" | "running" | "waiting" | "stopped" | "error" {
  switch (state) {
    case "starting":
      return "starting";
    case "waiting":
      return "waiting";
    case "stopped":
      return "stopped";
    case "error":
      return "error";
    case "ready":
    case "running":
    default:
      return "running";
  }
}

function toRuntimePayloadFromSession(
  session: ProviderSession,
  extra?: {
    readonly modelSelection?: unknown;
    readonly detached?: boolean;
    readonly mcpProviderSessionId?: string;
    readonly lastRuntimeEvent?: string;
    readonly lastRuntimeEventAt?: string;
    readonly sessionOwnershipId?: string;
  },
): Record<string, unknown> {
  return {
    cwd: session.cwd ?? null,
    model: session.model ?? null,
    activeTurnId: session.activeTurnId ?? null,
    lastError: session.lastError ?? null,
    ...(extra?.modelSelection !== undefined ? { modelSelection: extra.modelSelection } : {}),
    ...(extra?.detached !== undefined ? { detached: extra.detached } : {}),
    ...(extra?.mcpProviderSessionId !== undefined
      ? { mcpProviderSessionId: extra.mcpProviderSessionId }
      : {}),
    ...(extra?.lastRuntimeEvent !== undefined ? { lastRuntimeEvent: extra.lastRuntimeEvent } : {}),
    ...(extra?.lastRuntimeEventAt !== undefined
      ? { lastRuntimeEventAt: extra.lastRuntimeEventAt }
      : {}),
    sessionOwnershipId: extra?.sessionOwnershipId ?? NodeCrypto.randomUUID(),
    sendTurnOperationId: null,
    activeTurnSendTurnOperationId: null,
    lastFailedSendTurnOperationId: null,
    failedSendPreservedActiveTurnId: null,
    lastTerminalTurnId: null,
    unconfirmedSessionExit: null,
  };
}

function readPersistedModelSelection(
  runtimePayload: ProviderSessionDirectory.ProviderRuntimeBinding["runtimePayload"],
): ModelSelection | undefined {
  if (!runtimePayload || typeof runtimePayload !== "object" || Array.isArray(runtimePayload)) {
    return undefined;
  }
  const raw = "modelSelection" in runtimePayload ? runtimePayload.modelSelection : undefined;
  return isModelSelection(raw) ? raw : undefined;
}

function readPersistedCwd(
  runtimePayload: ProviderSessionDirectory.ProviderRuntimeBinding["runtimePayload"],
): string | undefined {
  if (!runtimePayload || typeof runtimePayload !== "object" || Array.isArray(runtimePayload)) {
    return undefined;
  }
  const rawCwd = "cwd" in runtimePayload ? runtimePayload.cwd : undefined;
  if (typeof rawCwd !== "string") return undefined;
  const trimmed = rawCwd.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

function readPersistedDetached(
  runtimePayload: ProviderSessionDirectory.ProviderRuntimeBinding["runtimePayload"],
): boolean | undefined {
  if (!runtimePayload || typeof runtimePayload !== "object" || Array.isArray(runtimePayload)) {
    return undefined;
  }
  const raw = "detached" in runtimePayload ? runtimePayload.detached : undefined;
  return typeof raw === "boolean" ? raw : undefined;
}

function readPersistedActiveTurnId(
  runtimePayload: ProviderSessionDirectory.ProviderRuntimeBinding["runtimePayload"],
): TurnId | undefined {
  if (!runtimePayload || typeof runtimePayload !== "object" || Array.isArray(runtimePayload)) {
    return undefined;
  }
  const raw = "activeTurnId" in runtimePayload ? runtimePayload.activeTurnId : undefined;
  if (isTurnId(raw)) {
    return raw;
  }
  if (typeof raw !== "string") {
    return undefined;
  }
  const trimmed = raw.trim();
  return trimmed.length > 0 ? TurnId.make(trimmed) : undefined;
}

function readResumableActiveTurnId(
  provider: ProviderDriverKind,
  runtimePayload: ProviderSessionDirectory.ProviderRuntimeBinding["runtimePayload"],
): TurnId | undefined {
  return supportsActiveTurnResume(provider) ? readPersistedActiveTurnId(runtimePayload) : undefined;
}

function readRecord(value: unknown): Record<string, unknown> | undefined {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined;
}

function encodeStableJson(value: unknown): string | undefined {
  try {
    const seen = new WeakSet<object>();
    return JSON.stringify(value, (_key, nested) => {
      if (!readRecord(nested)) {
        return nested;
      }
      if (seen.has(nested)) {
        throw new TypeError("Cannot encode circular resume cursor.");
      }
      seen.add(nested);
      return Object.fromEntries(
        Object.keys(nested)
          .sort()
          .map((key) => [key, nested[key]]),
      );
    });
  } catch {
    return undefined;
  }
}

function resumeCursorEquals(left: unknown, right: unknown): boolean {
  if (left === right) {
    return true;
  }
  const leftJson = encodeStableJson(left);
  return leftJson !== undefined && leftJson === encodeStableJson(right);
}

function bindingMatchesSnapshot(
  expected: ProviderSessionDirectory.ProviderRuntimeBinding,
  latest: ProviderSessionDirectory.ProviderRuntimeBinding | undefined,
): boolean {
  if (latest === undefined) {
    return false;
  }
  return (
    latest.threadId === expected.threadId &&
    latest.provider === expected.provider &&
    latest.providerInstanceId === expected.providerInstanceId &&
    latest.adapterKey === expected.adapterKey &&
    latest.status === expected.status &&
    latest.runtimeMode === expected.runtimeMode &&
    resumeCursorEquals(latest.resumeCursor, expected.resumeCursor) &&
    resumeCursorEquals(latest.runtimePayload, expected.runtimePayload)
  );
}

function inactiveBindingMatchesSnapshot(
  expected: ProviderSessionDirectory.ProviderRuntimeBindingWithMetadata,
  latest: ProviderSessionDirectory.ProviderRuntimeBinding | undefined,
): boolean {
  if (
    latest === undefined ||
    !bindingMatchesSnapshot(expected, latest) ||
    latest.status === "stopped" ||
    readPersistedActiveTurnId(latest.runtimePayload) !== undefined
  ) {
    return false;
  }
  const latestLastSeenAt =
    "lastSeenAt" in latest && typeof latest.lastSeenAt === "string" ? latest.lastSeenAt : undefined;
  return latestLastSeenAt === expected.lastSeenAt;
}

function terminalBindingMatchesSnapshot(input: {
  readonly expected: ProviderSessionDirectory.ProviderRuntimeBinding;
  readonly latest: ProviderSessionDirectory.ProviderRuntimeBinding | undefined;
  readonly turnId: TurnId;
}): boolean {
  if (
    !bindingMatchesSnapshot(input.expected, input.latest) ||
    (input.latest?.status !== "error" && input.latest?.status !== "stopped")
  ) {
    return false;
  }
  const runtimePayload = readRecord(input.latest.runtimePayload) ?? {};
  const activeTurnId = runtimePayload.activeTurnId;
  const lastTerminalTurnId = runtimePayload.lastTerminalTurnId;
  return (
    (activeTurnId === null || activeTurnId === undefined || activeTurnId === input.turnId) &&
    (lastTerminalTurnId === null ||
      lastTerminalTurnId === undefined ||
      lastTerminalTurnId === input.turnId)
  );
}

function withActiveTurnFallback<T extends ProviderSession>(
  session: T,
  activeTurnId: TurnId | undefined,
): T {
  const effectiveActiveTurnId = session.activeTurnId ?? activeTurnId;
  if (effectiveActiveTurnId === undefined) {
    return session;
  }
  if (session.activeTurnId === effectiveActiveTurnId && session.status === "running") {
    return session;
  }
  return {
    ...session,
    activeTurnId: effectiveActiveTurnId,
    status: session.status === "ready" ? "running" : session.status,
  };
}

function mergeRuntimePayload(
  runtimePayload: ProviderSessionDirectory.ProviderRuntimeBinding["runtimePayload"] | undefined,
  patch: Record<string, unknown>,
): ProviderSessionDirectory.ProviderRuntimeBinding["runtimePayload"] {
  return {
    ...readRecord(runtimePayload),
    ...patch,
  };
}

function readResumeCursorFromRuntimeDetail(detail: unknown): unknown | undefined {
  return readRecord(detail)?.resumeCursor;
}

const dieOnMissingBindingInstanceId = (
  operation: string,
  payload: {
    readonly providerInstanceId?: ProviderInstanceId | undefined;
    readonly provider?: ProviderDriverKind | undefined;
  },
): ProviderInstanceId => {
  if (payload.providerInstanceId !== undefined) {
    return payload.providerInstanceId;
  }
  throw new Error(
    payload.provider
      ? `${operation}: provider instance id is required for provider '${payload.provider}'.`
      : `${operation}: provider instance id is required.`,
  );
};

const correlateRuntimeEventWithInstance = (
  source: {
    readonly instanceId: ProviderInstanceId;
    readonly provider: ProviderDriverKind;
  },
  event: ProviderRuntimeEvent,
): ProviderRuntimeEvent => {
  if (event.provider !== source.provider) {
    throw new Error(
      `ProviderService.streamEvents: provider instance '${source.instanceId}' is backed by driver '${source.provider}' but emitted driver '${event.provider}'.`,
    );
  }
  if (event.providerInstanceId !== undefined && event.providerInstanceId !== source.instanceId) {
    throw new Error(
      `ProviderService.streamEvents: provider instance '${source.instanceId}' emitted event for instance '${event.providerInstanceId}'.`,
    );
  }
  return { ...event, providerInstanceId: source.instanceId };
};

const isProviderSessionEndEvent = (event: ProviderRuntimeEvent): boolean =>
  event.type === "session.exited";

const isTerminalTurnRuntimeEvent = (event: ProviderRuntimeEvent): boolean =>
  event.type === "turn.completed" || event.type === "turn.aborted";

const isStartedTurnRuntimeEvent = (event: ProviderRuntimeEvent): boolean =>
  event.type === "turn.started";

const hasNonWhitespaceText = (value: unknown): value is string =>
  typeof value === "string" && value.trim().length > 0;

const readEmptyResponseFailureTurnIds = (
  payload: Readonly<Record<string, unknown>>,
): ReadonlyArray<string> =>
  Array.isArray(payload.emptyResponseFailureTurnIds)
    ? payload.emptyResponseFailureTurnIds.filter(
        (turnId): turnId is string => typeof turnId === "string",
      )
    : [];

const isMeaningfulCompletedItem = (
  payload: Extract<
    ProviderRuntimeEvent,
    { readonly type: "item.started" | "item.updated" | "item.completed" }
  >["payload"],
): boolean => {
  switch (payload.itemType) {
    case "assistant_message":
    case "reasoning":
    case "plan":
      return hasNonWhitespaceText(payload.detail);
    case "command_execution":
    case "file_change":
    case "mcp_tool_call":
    case "dynamic_tool_call":
    case "collab_agent_tool_call":
    case "web_search":
    case "image_view":
      return true;
    default:
      return false;
  }
};

const isMeaningfulUpdatedItem = (
  payload: Extract<ProviderRuntimeEvent, { readonly type: "item.updated" }>["payload"],
): boolean => {
  switch (payload.itemType) {
    case "assistant_message":
    case "reasoning":
    case "plan":
      return hasNonWhitespaceText(payload.detail);
    case "command_execution":
    case "file_change":
    case "mcp_tool_call":
    case "dynamic_tool_call":
    case "collab_agent_tool_call":
    case "web_search":
    case "image_view":
      // An in-progress structured item shell is lifecycle noise, but adapters
      // may carry already-visible command/tool output in detail before the
      // lifecycle reaches a terminal status.
      return (
        hasNonWhitespaceText(payload.detail) ||
        (payload.status !== undefined && payload.status !== "inProgress")
      );
    default:
      return false;
  }
};

const hasNonEmptyAudioData = (value: unknown): boolean => {
  if (hasNonWhitespaceText(value)) return true;
  if (ArrayBuffer.isView(value)) return value.byteLength > 0;
  if (value instanceof ArrayBuffer) return value.byteLength > 0;
  if (Array.isArray(value)) return value.length > 0;
  if (typeof value !== "object" || value === null || !("data" in value)) return false;
  return hasNonEmptyAudioData(value.data);
};

const isMeaningfulTurnOutputEvent = (event: ProviderRuntimeEvent): boolean => {
  const runtimePayload = (event as { readonly payload?: unknown }).payload;
  if (typeof runtimePayload !== "object" || runtimePayload === null) return false;

  switch (event.type) {
    case "turn.plan.updated":
      return event.payload.plan.length > 0 || hasNonWhitespaceText(event.payload.explanation);
    case "turn.proposed.delta":
      return hasNonWhitespaceText(event.payload.delta);
    case "turn.proposed.completed":
      return hasNonWhitespaceText(event.payload.planMarkdown);
    case "turn.diff.updated":
      return hasNonWhitespaceText(event.payload.unifiedDiff);
    case "content.delta":
      return hasNonWhitespaceText(event.payload.delta);
    case "item.completed":
      // Providers emit empty assistant/reasoning/plan lifecycle shells around
      // their actual deltas. Only detail-bearing shells count; tool and other
      // structured work items are meaningful by their lifecycle alone.
      return isMeaningfulCompletedItem(event.payload);
    case "item.updated":
      return isMeaningfulUpdatedItem(event.payload);
    case "thread.realtime.audio.delta":
      return hasNonEmptyAudioData(event.payload.audio);
    case "task.progress":
      return (
        hasNonWhitespaceText(event.payload.description) ||
        hasNonWhitespaceText(event.payload.summary)
      );
    case "hook.progress":
      return (
        hasNonWhitespaceText(event.payload.output) ||
        hasNonWhitespaceText(event.payload.stdout) ||
        hasNonWhitespaceText(event.payload.stderr)
      );
    case "tool.progress":
      return hasNonWhitespaceText(event.payload.summary);
    case "task.completed":
    case "hook.completed":
    case "tool.summary":
    case "tool.denied":
    case "request.opened":
    case "user-input.requested":
      return true;
    case "files.persisted":
      return event.payload.files.length > 0;
    default:
      return false;
  }
};

interface AdapterGenerationRecord {
  readonly currentAdapter: ProviderAdapterShape<ProviderAdapterError>;
  readonly currentGeneration: number;
}

interface TrackedMcpSessionRecord {
  readonly adapterGeneration: number;
  readonly providerSessionId: string;
}

interface ProviderRuntimeEventSource {
  readonly instanceId: ProviderInstanceId;
  readonly provider: ProviderDriverKind;
  readonly adapterGeneration: number;
  readonly adapter: ProviderAdapterShape<ProviderAdapterError>;
}

interface ProviderSessionEndContext {
  readonly providerSessionId?: string;
  readonly duringPendingStart: boolean;
}

interface BufferedProviderRuntimeEvent {
  readonly event: ProviderRuntimeEvent;
  readonly sessionEndContext?: ProviderSessionEndContext;
  readonly terminalTurnOwnedAtIngress?: true;
}

interface PendingEmptyResponseCompletion {
  readonly source: ProviderRuntimeEventSource;
  readonly threadId: ThreadId;
  readonly turnId: TurnId | string;
  readonly events: Array<BufferedProviderRuntimeEvent>;
  readonly settled: Deferred.Deferred<void>;
  ownershipOpen: boolean;
  draining: boolean;
  closed: boolean;
}

const makeProviderService = Effect.fn("makeProviderService")(function* (
  options?: ProviderServiceLiveOptions,
) {
  const providerServiceScope = yield* Effect.scope;
  const analytics = yield* Effect.service(AnalyticsService.AnalyticsService);
  const serverConfig = yield* ServerConfig.ServerConfig;
  const eventLoggers = yield* ProviderEventLoggers.ProviderEventLoggers;
  // Options-provided logger wins (test overrides); otherwise we take whatever
  // the `ProviderEventLoggers` tag exposes — `undefined` means "no canonical
  // log writer is attached", which downstream code already handles as a
  // no-op.
  const canonicalEventLogger = options?.canonicalEventLogger ?? eventLoggers.canonical;

  const registry = yield* ProviderAdapterRegistry.ProviderAdapterRegistry;
  const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
  const runtimeEventPubSub = yield* PubSub.unbounded<ProviderRuntimeEvent>();
  const turnsWithMeaningfulOutputByAdapter = new WeakMap<
    ProviderAdapterShape<ProviderAdapterError>,
    Map<ProviderInstanceId, Map<ThreadId, Set<string>>>
  >();
  const pendingEmptyResponseCompletionsByAdapter = new WeakMap<
    ProviderAdapterShape<ProviderAdapterError>,
    Map<ProviderInstanceId, Map<ThreadId, PendingEmptyResponseCompletion>>
  >();

  const clearTrackedSessionOutput = (
    adapter: ProviderAdapterShape<ProviderAdapterError>,
    instanceId: ProviderInstanceId,
    threadId: ThreadId,
  ) => {
    const instances = turnsWithMeaningfulOutputByAdapter.get(adapter);
    const sessions = instances?.get(instanceId);
    if (sessions === undefined) return;
    sessions.delete(threadId);
    if (sessions.size === 0) instances?.delete(instanceId);
    if (instances?.size === 0) turnsWithMeaningfulOutputByAdapter.delete(adapter);
  };

  const clearTrackedTurnOutput = (
    adapter: ProviderAdapterShape<ProviderAdapterError>,
    instanceId: ProviderInstanceId,
    threadId: ThreadId,
    turnId: TurnId | string,
  ): boolean => {
    const sessions = turnsWithMeaningfulOutputByAdapter.get(adapter)?.get(instanceId);
    const turns = sessions?.get(threadId);
    if (turns === undefined) return false;
    const hadOutput = turns.delete(String(turnId));
    if (turns.size === 0) clearTrackedSessionOutput(adapter, instanceId, threadId);
    return hadOutput;
  };

  const hasTrackedTurnOutput = (
    adapter: ProviderAdapterShape<ProviderAdapterError>,
    instanceId: ProviderInstanceId,
    threadId: ThreadId,
    turnId: TurnId | string,
  ): boolean =>
    turnsWithMeaningfulOutputByAdapter
      .get(adapter)
      ?.get(instanceId)
      ?.get(threadId)
      ?.has(String(turnId)) === true;

  const trackTurnOutput = (
    adapter: ProviderAdapterShape<ProviderAdapterError>,
    instanceId: ProviderInstanceId,
    threadId: ThreadId,
    turnId: TurnId | string,
  ) => {
    const instances =
      turnsWithMeaningfulOutputByAdapter.get(adapter) ??
      new Map<ProviderInstanceId, Map<ThreadId, Set<string>>>();
    const sessions = instances.get(instanceId) ?? new Map<ThreadId, Set<string>>();
    const turns = sessions.get(threadId) ?? new Set<string>();
    turns.add(String(turnId));
    sessions.set(threadId, turns);
    instances.set(instanceId, sessions);
    turnsWithMeaningfulOutputByAdapter.set(adapter, instances);
  };

  const preserveRecoveredActiveTurnOutput = (
    adapter: ProviderAdapterShape<ProviderAdapterError>,
    instanceId: ProviderInstanceId,
    threadId: ThreadId,
    activeTurnId: TurnId,
  ) => {
    // A rebuilt ProviderService cannot reconstruct whether this already-active
    // turn emitted output before the rebuild. Treat that absence as
    // inconclusive so a terminal-only resume cannot fabricate an empty turn.
    trackTurnOutput(adapter, instanceId, threadId, activeTurnId);
  };

  const getPendingEmptyResponseCompletion = (
    source: ProviderRuntimeEventSource,
    threadId: ThreadId,
  ): PendingEmptyResponseCompletion | undefined =>
    pendingEmptyResponseCompletionsByAdapter
      .get(source.adapter)
      ?.get(source.instanceId)
      ?.get(threadId);

  const setPendingEmptyResponseCompletion = (pending: PendingEmptyResponseCompletion) => {
    const instances =
      pendingEmptyResponseCompletionsByAdapter.get(pending.source.adapter) ??
      new Map<ProviderInstanceId, Map<ThreadId, PendingEmptyResponseCompletion>>();
    const sessions =
      instances.get(pending.source.instanceId) ??
      new Map<ThreadId, PendingEmptyResponseCompletion>();
    sessions.set(pending.threadId, pending);
    instances.set(pending.source.instanceId, sessions);
    pendingEmptyResponseCompletionsByAdapter.set(pending.source.adapter, instances);
  };

  const deletePendingEmptyResponseCompletion = (pending: PendingEmptyResponseCompletion) => {
    const instances = pendingEmptyResponseCompletionsByAdapter.get(pending.source.adapter);
    const sessions = instances?.get(pending.source.instanceId);
    if (sessions?.get(pending.threadId) !== pending) return;
    sessions.delete(pending.threadId);
    if (sessions.size === 0) instances?.delete(pending.source.instanceId);
    if (instances?.size === 0) {
      pendingEmptyResponseCompletionsByAdapter.delete(pending.source.adapter);
    }
  };

  const clearTrackedSessionOutputExceptTurn = (
    adapter: ProviderAdapterShape<ProviderAdapterError>,
    instanceId: ProviderInstanceId,
    threadId: ThreadId,
    turnId: TurnId | string,
  ) => {
    const turns = turnsWithMeaningfulOutputByAdapter.get(adapter)?.get(instanceId)?.get(threadId);
    if (turns === undefined) return;
    const incomingTurnId = String(turnId);
    for (const trackedTurnId of turns) {
      if (trackedTurnId !== incomingTurnId) turns.delete(trackedTurnId);
    }
    if (turns.size === 0) clearTrackedSessionOutput(adapter, instanceId, threadId);
  };

  const guardEmptyAssistantResponse = (
    source: {
      readonly adapter: ProviderAdapterShape<ProviderAdapterError>;
      readonly instanceId: ProviderInstanceId;
    },
    event: ProviderRuntimeEvent,
    startedTurnAccepted: boolean,
    terminalTurnOwned: boolean,
    terminalTurnReplaysEmptyFailure: boolean,
  ): ProviderRuntimeEvent => {
    const turnId = event.turnId;
    if (event.type === "session.exited") {
      clearTrackedSessionOutput(source.adapter, source.instanceId, event.threadId);
      return event;
    }
    if (turnId === undefined) return event;

    const payload =
      "payload" in event && typeof event.payload === "object" && event.payload !== null
        ? event.payload
        : undefined;
    if (event.type === "turn.started") {
      if (startedTurnAccepted) {
        // The persisted active turn is authoritative. Prune orphaned output
        // from older turns while preserving this id so duplicate starts and
        // output that raced ahead of the start remain idempotent.
        clearTrackedSessionOutputExceptTurn(
          source.adapter,
          source.instanceId,
          event.threadId,
          turnId,
        );
      }
      return event;
    }
    if (isMeaningfulTurnOutputEvent(event)) {
      trackTurnOutput(source.adapter, source.instanceId, event.threadId, turnId);
      return event;
    }
    if (event.type === "turn.aborted") {
      if (terminalTurnOwned) {
        clearTrackedTurnOutput(source.adapter, source.instanceId, event.threadId, turnId);
      }
      return event;
    }
    if (event.type !== "turn.completed") return event;

    if (!terminalTurnOwned) return event;

    const hasMeaningfulOutput = clearTrackedTurnOutput(
      source.adapter,
      source.instanceId,
      event.threadId,
      turnId,
    );
    if (
      payload === undefined ||
      !("state" in payload) ||
      payload.state !== "completed" ||
      (hasMeaningfulOutput && !terminalTurnReplaysEmptyFailure)
    ) {
      return event;
    }
    return {
      ...event,
      payload: {
        ...event.payload,
        state: "failed",
        errorMessage: PROVIDER_EMPTY_RESPONSE_ERROR,
      },
    };
  };
  const nowIso = Effect.map(DateTime.now, DateTime.formatIso);
  const mcpEndCleanupRetryDelay = "250 millis";
  const sessionExitLivenessPollDelay = "50 millis";
  const sessionExitLivenessProbeTimeout = "50 millis";
  const sessionExitLivenessPollAttempts = 5;
  const sessionStartTimeoutMs = Math.max(1, options?.sessionStartTimeoutMs ?? 10 * 60 * 1_000);
  const adapterGenerations = yield* Ref.make(
    new Map<ProviderInstanceId, AdapterGenerationRecord>(),
  );
  const trackedMcpSessions = yield* Ref.make(
    new Map<ProviderInstanceId, Map<ThreadId, TrackedMcpSessionRecord>>(),
  );
  const pendingMcpSessionStarts = yield* Ref.make(new Map<ThreadId, number>());
  const activeSendTurnOperations = yield* Ref.make(new Map<ThreadId, string>());
  const startAdapterSessionWithTimeout = Effect.fn(
    "ProviderService.startAdapterSessionWithTimeout",
  )(function* (input: {
    readonly adapter: ProviderAdapterShape<ProviderAdapterError>;
    readonly request: ProviderSessionStartInput;
    readonly sessionOwnershipId?: string;
  }) {
    const startedSession = yield* input.adapter
      .startSession(input.request)
      .pipe(Effect.timeoutOption(sessionStartTimeoutMs));
    if (Option.isNone(startedSession)) {
      const detail = `Provider startup timed out after ${sessionStartTimeoutMs}ms.`;
      return yield* new ProviderSessionStartTimeoutError({
        provider: input.adapter.provider,
        threadId: input.request.threadId,
        detail,
        timeoutMs: sessionStartTimeoutMs,
        ...(input.sessionOwnershipId !== undefined
          ? { sessionOwnershipId: input.sessionOwnershipId }
          : {}),
      });
    }
    return startedSession.value;
  });
  const threadSessionLocks = yield* SynchronizedRef.make(
    new Map<ThreadId, { readonly semaphore: Semaphore.Semaphore; readonly users: number }>(),
  );
  const getThreadSessionLock = (threadId: ThreadId) =>
    SynchronizedRef.modifyEffect(threadSessionLocks, (current) => {
      const existing = current.get(threadId);
      if (existing !== undefined) {
        const next = new Map(current);
        const acquired = { ...existing, users: existing.users + 1 };
        next.set(threadId, acquired);
        return Effect.succeed([acquired, next] as const);
      }
      return Semaphore.make(1).pipe(
        Effect.map((semaphore) => {
          const next = new Map(current);
          const acquired = { semaphore, users: 1 };
          next.set(threadId, acquired);
          return [acquired, next] as const;
        }),
      );
    });
  const releaseThreadSessionLock = (threadId: ThreadId, semaphore: Semaphore.Semaphore) =>
    SynchronizedRef.update(threadSessionLocks, (current) => {
      const existing = current.get(threadId);
      if (existing === undefined || existing.semaphore !== semaphore) return current;
      const next = new Map(current);
      if (existing.users <= 1) {
        next.delete(threadId);
      } else {
        next.set(threadId, { ...existing, users: existing.users - 1 });
      }
      return next;
    });
  const withThreadSessionLock = <A, E, R>(threadId: ThreadId, effect: Effect.Effect<A, E, R>) =>
    Effect.acquireUseRelease(
      getThreadSessionLock(threadId),
      ({ semaphore }) => semaphore.withPermit(effect),
      ({ semaphore }) => releaseThreadSessionLock(threadId, semaphore),
    );
  const releaseSendTurnOperation = (threadId: ThreadId, operationId: string) =>
    withThreadSessionLock(
      threadId,
      Effect.gen(function* () {
        yield* Ref.update(activeSendTurnOperations, (current) => {
          if (current.get(threadId) !== operationId) return current;
          const next = new Map(current);
          next.delete(threadId);
          return next;
        });
        const binding = Option.getOrUndefined(yield* directory.getBinding(threadId));
        const runtimePayload = readRecord(binding?.runtimePayload) ?? {};
        if (binding === undefined || runtimePayload.sendTurnOperationId !== operationId) {
          return;
        }
        yield* directory.upsert({
          threadId,
          provider: binding.provider,
          ...(binding.providerInstanceId !== undefined
            ? { providerInstanceId: binding.providerInstanceId }
            : {}),
          runtimeMode: binding.runtimeMode ?? "full-access",
          status: binding.status ?? "running",
          ...(binding.resumeCursor !== undefined ? { resumeCursor: binding.resumeCursor } : {}),
          runtimePayload: {
            ...runtimePayload,
            sendTurnOperationId: null,
          },
        });
      }),
    ).pipe(
      Effect.catchCause((cause) =>
        Effect.logWarning("provider.session.send-turn-operation-release-failed", {
          threadId,
          operationId,
          cause,
        }),
      ),
    );
  const beginMcpSessionStart = (threadId: ThreadId) =>
    Ref.update(pendingMcpSessionStarts, (current) => {
      const next = new Map(current);
      next.set(threadId, (current.get(threadId) ?? 0) + 1);
      return next;
    });
  const endMcpSessionStart = (threadId: ThreadId) =>
    Ref.update(pendingMcpSessionStarts, (current) => {
      const count = current.get(threadId) ?? 0;
      if (count <= 0) return current;
      const next = new Map(current);
      if (count === 1) {
        next.delete(threadId);
      } else {
        next.set(threadId, count - 1);
      }
      return next;
    });
  const hasPendingMcpSessionStart = (threadId: ThreadId) =>
    Ref.get(pendingMcpSessionStarts).pipe(Effect.map((current) => current.has(threadId)));
  const waitForPendingMcpSessionStart = Effect.fn("ProviderService.waitForPendingMcpSessionStart")(
    function* (threadId: ThreadId) {
      while (yield* hasPendingMcpSessionStart(threadId)) {
        yield* Effect.sleep(mcpEndCleanupRetryDelay);
      }
    },
  );
  const observeAdapterGeneration = (
    providerInstanceId: ProviderInstanceId,
    adapter: ProviderAdapterShape<ProviderAdapterError>,
  ) =>
    Ref.modify(adapterGenerations, (current) => {
      const existing = current.get(providerInstanceId);
      const generation =
        existing?.currentAdapter === adapter
          ? existing.currentGeneration
          : (existing?.currentGeneration ?? 0) + 1;
      const next = new Map(current);
      next.set(providerInstanceId, {
        currentAdapter: adapter,
        currentGeneration: generation,
      });
      return [generation, next] as const;
    });
  const getAdapterGenerationForStart = Effect.fn("ProviderService.getAdapterGenerationForStart")(
    function* (
      providerInstanceId: ProviderInstanceId,
      adapter: ProviderAdapterShape<ProviderAdapterError>,
    ) {
      const existing = (yield* Ref.get(adapterGenerations)).get(providerInstanceId);
      if (!existing || existing.currentAdapter === adapter) {
        return yield* observeAdapterGeneration(providerInstanceId, adapter);
      }

      const currentAdapter = Option.getOrUndefined(
        yield* registry.getByInstance(providerInstanceId).pipe(Effect.option),
      );
      if (currentAdapter === adapter) {
        return yield* observeAdapterGeneration(providerInstanceId, adapter);
      }
      return 0;
    },
  );
  const getCurrentAdapterGeneration = (providerInstanceId: ProviderInstanceId) =>
    Ref.get(adapterGenerations).pipe(
      Effect.map((current) => current.get(providerInstanceId)?.currentGeneration),
    );
  const isCurrentAdapterGeneration = (
    providerInstanceId: ProviderInstanceId,
    adapterGeneration: number,
  ) =>
    Ref.get(adapterGenerations).pipe(
      Effect.map(
        (current) => current.get(providerInstanceId)?.currentGeneration === adapterGeneration,
      ),
    );
  const trackMcpSession = (
    threadId: ThreadId,
    providerInstanceId: ProviderInstanceId,
    adapterGeneration: number,
  ) =>
    Ref.update(trackedMcpSessions, (current) => {
      const mcpSession = McpProviderSession.readMcpProviderSession(threadId);
      if (!mcpSession || mcpSession.providerInstanceId !== providerInstanceId) {
        return current;
      }
      const next = new Map(current);
      const threads = new Map(next.get(providerInstanceId) ?? []);
      threads.set(threadId, {
        adapterGeneration,
        providerSessionId: mcpSession.providerSessionId,
      });
      next.set(providerInstanceId, threads);
      return next;
    });
  const forgetMcpSession = (
    threadId: ThreadId,
    providerInstanceId?: ProviderInstanceId,
    providerSessionId?: string,
  ) =>
    Ref.update(trackedMcpSessions, (current) => {
      let changed = false;
      const next = new Map<ProviderInstanceId, Map<ThreadId, TrackedMcpSessionRecord>>();
      for (const [trackedProviderInstanceId, threads] of current) {
        const tracked = threads.get(threadId);
        if (
          (providerInstanceId !== undefined && trackedProviderInstanceId !== providerInstanceId) ||
          tracked === undefined ||
          (providerSessionId !== undefined && tracked.providerSessionId !== providerSessionId)
        ) {
          next.set(trackedProviderInstanceId, threads);
          continue;
        }
        changed = true;
        const remaining = new Map(threads);
        remaining.delete(threadId);
        if (remaining.size > 0) {
          next.set(trackedProviderInstanceId, remaining);
        }
      }
      return changed ? next : current;
    });
  const prepareMcpSession = (threadId: ThreadId, providerInstanceId: ProviderInstanceId) =>
    McpSessionRegistry.issueActiveMcpCredential({ threadId, providerInstanceId }).pipe(
      Effect.map((credential) => {
        if (credential) {
          McpProviderSession.setMcpProviderSession(credential.config);
          return credential.config;
        }
        const currentMcpSession = McpProviderSession.readMcpProviderSession(threadId);
        return currentMcpSession?.providerInstanceId === providerInstanceId
          ? currentMcpSession
          : undefined;
      }),
    );
  const clearMcpSession = (threadId: ThreadId) =>
    McpSessionRegistry.revokeActiveMcpThread(threadId).pipe(
      Effect.tap(() => Effect.sync(() => McpProviderSession.clearMcpProviderSession(threadId))),
      Effect.andThen(forgetMcpSession(threadId)),
    );
  const clearPreparedMcpSession = (
    threadId: ThreadId,
    providerInstanceId: ProviderInstanceId,
    preparedMcpSession: McpProviderSession.McpProviderSessionConfig | undefined,
  ) =>
    preparedMcpSession
      ? clearMcpProviderSession(threadId, providerInstanceId, preparedMcpSession.providerSessionId)
      : Effect.void;
  const stopSupersededAdapterSession = (
    adapter: ProviderAdapterShape<ProviderAdapterError>,
    threadId: ThreadId,
    providerInstanceId: ProviderInstanceId,
  ) =>
    adapter.stopSession(threadId).pipe(
      Effect.catchCause((cause) =>
        Effect.logWarning("provider.session.superseded-stop-failed", {
          threadId,
          providerInstanceId,
          cause,
        }),
      ),
    );
  const clearMcpProviderSession = (
    threadId: ThreadId,
    providerInstanceId: ProviderInstanceId,
    providerSessionId: string,
  ) =>
    McpSessionRegistry.revokeActiveMcpProviderSession(providerSessionId).pipe(
      Effect.tap(() =>
        Effect.sync(() =>
          McpProviderSession.clearMcpProviderSessionIfProviderSessionId(
            threadId,
            providerSessionId,
          ),
        ),
      ),
      Effect.andThen(forgetMcpSession(threadId, providerInstanceId, providerSessionId)),
    );

  const clearMcpSessionAfterProviderSessionEnds = Effect.fn(
    "ProviderService.clearMcpSessionAfterProviderSessionEnds",
  )(function* (
    source: {
      readonly instanceId: ProviderInstanceId;
      readonly provider: ProviderDriverKind;
      readonly adapterGeneration: number;
    },
    event: ProviderRuntimeEvent,
    observed: {
      readonly providerSessionId?: string;
      readonly duringPendingStart: boolean;
    },
  ) {
    if (!isProviderSessionEndEvent(event)) return;
    const eventProviderSessionId =
      event.type === "session.exited" ? event.payload.mcpProviderSessionId : undefined;
    if (event.type === "session.exited" && eventProviderSessionId === undefined) {
      return;
    }
    if (eventProviderSessionId === undefined && observed.duringPendingStart) {
      return;
    }
    const endedProviderSessionId = eventProviderSessionId ?? observed.providerSessionId;
    if (endedProviderSessionId === undefined) {
      return;
    }

    yield* Effect.sleep(mcpEndCleanupRetryDelay);
    yield* waitForPendingMcpSessionStart(event.threadId);

    const currentMcpSession = McpProviderSession.readMcpProviderSession(event.threadId);
    if (!currentMcpSession || currentMcpSession.providerInstanceId !== source.instanceId) {
      return;
    }
    if (currentMcpSession.providerSessionId !== endedProviderSessionId) {
      return;
    }

    const tracked = (yield* Ref.get(trackedMcpSessions))
      .get(source.instanceId)
      ?.get(event.threadId);
    if (
      tracked &&
      (tracked.adapterGeneration !== source.adapterGeneration ||
        tracked.providerSessionId !== endedProviderSessionId)
    ) {
      return;
    }

    yield* clearMcpProviderSession(event.threadId, source.instanceId, endedProviderSessionId).pipe(
      Effect.catchCause((cause) =>
        Effect.logWarning("provider.session.mcp-clear-failed", {
          threadId: event.threadId,
          provider: source.provider,
          cause,
        }),
      ),
    );
  });

  const clearMcpSessionsForProviderInstance = Effect.fn(
    "ProviderService.clearMcpSessionsForProviderInstance",
  )(function* (
    providerInstanceId: ProviderInstanceId,
    options?: {
      readonly retainAdapterGeneration?: number;
    },
  ) {
    const trackedEntries = Array.from(
      (yield* Ref.get(trackedMcpSessions)).get(providerInstanceId) ?? [],
    ).filter(
      ([, tracked]) =>
        options?.retainAdapterGeneration === undefined ||
        tracked.adapterGeneration !== options.retainAdapterGeneration,
    );
    yield* Effect.forEach(
      trackedEntries,
      ([threadId, tracked]) =>
        Effect.gen(function* () {
          const currentMcpSession = McpProviderSession.readMcpProviderSession(threadId);
          if (!currentMcpSession || currentMcpSession.providerInstanceId !== providerInstanceId) {
            yield* forgetMcpSession(threadId, providerInstanceId, tracked.providerSessionId);
            return;
          }
          if (yield* hasPendingMcpSessionStart(threadId)) {
            return;
          }
          const latestTracked = (yield* Ref.get(trackedMcpSessions))
            .get(providerInstanceId)
            ?.get(threadId);
          if (
            options?.retainAdapterGeneration !== undefined &&
            latestTracked?.adapterGeneration === options.retainAdapterGeneration
          ) {
            return;
          }
          yield* clearMcpProviderSession(threadId, providerInstanceId, tracked.providerSessionId);
        }).pipe(
          Effect.catchCause((cause) =>
            Effect.logWarning("provider.session.mcp-clear-instance-failed", {
              threadId,
              providerInstanceId,
              cause,
            }),
          ),
        ),
      { discard: true },
    );
  });

  const publishRuntimeEvent = (
    event: ProviderRuntimeEvent,
    binding: CapturedProviderRuntimeEventBinding | undefined,
  ): Effect.Effect<void> =>
    Effect.succeed(event).pipe(
      Effect.tap(() => Effect.sync(() => captureProviderRuntimeEventBinding(event, binding))),
      Effect.tap((canonicalEvent) =>
        canonicalEventLogger
          ? canonicalEventLogger.write(canonicalEvent, canonicalEvent.threadId)
          : Effect.void,
      ),
      Effect.flatMap((canonicalEvent) => PubSub.publish(runtimeEventPubSub, canonicalEvent)),
      Effect.asVoid,
    );

  const requireBindingInstanceId = (
    operation: string,
    payload: {
      readonly providerInstanceId?: ProviderInstanceId | undefined;
      readonly provider?: ProviderDriverKind | undefined;
    },
  ): Effect.Effect<ProviderInstanceId, ProviderValidationError> =>
    payload.providerInstanceId !== undefined
      ? Effect.succeed(payload.providerInstanceId)
      : Effect.fail(
          toValidationError(
            operation,
            payload.provider
              ? `Provider instance id is required for provider '${payload.provider}'.`
              : "Provider instance id is required.",
          ),
        );

  const upsertSessionBinding = (
    session: ProviderSession,
    threadId: ThreadId,
    extra?: {
      readonly modelSelection?: unknown;
      readonly detached?: boolean;
      readonly mcpProviderSessionId?: string;
      readonly lastRuntimeEvent?: string;
      readonly lastRuntimeEventAt?: string;
      readonly sessionOwnershipId?: string;
    },
  ) =>
    Effect.gen(function* () {
      const providerInstanceId = yield* requireBindingInstanceId(
        "ProviderService.upsertSessionBinding",
        session,
      );
      const binding = Option.getOrUndefined(
        yield* directory
          .getBinding(threadId)
          .pipe(
            Effect.orElseSucceed(() =>
              Option.none<ProviderSessionDirectory.ProviderRuntimeBinding>(),
            ),
          ),
      );
      const reusableRuntimePayload =
        binding?.provider === session.provider && binding.providerInstanceId === providerInstanceId
          ? binding.runtimePayload
          : undefined;
      yield* directory.upsert({
        threadId,
        provider: session.provider,
        providerInstanceId,
        runtimeMode: session.runtimeMode,
        status: toRuntimeStatus(session),
        ...(session.resumeCursor !== undefined ? { resumeCursor: session.resumeCursor } : {}),
        runtimePayload: mergeRuntimePayload(
          reusableRuntimePayload,
          toRuntimePayloadFromSession(session, extra),
        ),
      });
    });

  const providerSessionIsProvablyGone = Effect.fn("ProviderService.providerSessionIsProvablyGone")(
    function* (input: {
      readonly adapter: ProviderAdapterShape<ProviderAdapterError>;
      readonly provider: ProviderDriverKind;
      readonly threadId: ThreadId;
    }) {
      let adapterStillOwnsSession = Option.some(true);
      for (
        let attempt = 0;
        attempt < sessionExitLivenessPollAttempts &&
        Option.isSome(adapterStillOwnsSession) &&
        adapterStillOwnsSession.value;
        attempt += 1
      ) {
        yield* Effect.sleep(sessionExitLivenessPollDelay);
        adapterStillOwnsSession = yield* input.adapter.hasSession(input.threadId).pipe(
          Effect.timeoutOption(sessionExitLivenessProbeTimeout),
          Effect.catchCause((cause) =>
            Effect.logWarning("provider.session.exit-liveness-check-failed", {
              threadId: input.threadId,
              provider: input.provider,
              cause,
            }).pipe(Effect.as(Option.none<boolean>())),
          ),
        );
        if (Option.isNone(adapterStillOwnsSession)) {
          yield* Effect.logWarning("provider.session.exit-liveness-check-unavailable", {
            threadId: input.threadId,
            provider: input.provider,
          });
        }
      }
      return Option.isSome(adapterStillOwnsSession) && !adapterStillOwnsSession.value;
    },
  );

  const persistSessionStateRuntimeState = (
    source: {
      readonly instanceId: ProviderInstanceId;
      readonly provider: ProviderDriverKind;
      readonly adapterGeneration: number;
      readonly adapter: ProviderAdapterShape<ProviderAdapterError>;
    },
    event: ProviderRuntimeEvent,
  ) => {
    if (event.type !== "session.state.changed") {
      return Effect.succeed(true);
    }
    const resumeCursor = readResumeCursorFromRuntimeDetail(event.payload.detail);
    const terminalSessionState =
      event.payload.state === "error" || event.payload.state === "stopped";
    const requiresProviderAbsence = event.payload.state === "stopped";
    if (!terminalSessionState && event.payload.state !== "waiting" && resumeCursor === undefined) {
      return Effect.succeed(true);
    }

    const persistState = Effect.gen(function* () {
      if (!(yield* isCurrentAdapterGeneration(source.instanceId, source.adapterGeneration))) {
        return false;
      }
      const binding = Option.getOrUndefined(yield* directory.getBinding(event.threadId));
      if (
        binding !== undefined &&
        (binding.provider !== source.provider || binding.providerInstanceId !== source.instanceId)
      ) {
        return false;
      }
      const previousPayload = readRecord(binding?.runtimePayload) ?? {};
      const bindingActiveTurnId = readPersistedActiveTurnId(previousPayload);
      const lastTerminalTurnId =
        typeof previousPayload.lastTerminalTurnId === "string"
          ? previousPayload.lastTerminalTurnId
          : undefined;
      if (
        terminalSessionState &&
        event.turnId !== undefined &&
        ((bindingActiveTurnId !== undefined && event.turnId !== bindingActiveTurnId) ||
          (bindingActiveTurnId === undefined &&
            lastTerminalTurnId !== undefined &&
            event.turnId !== lastTerminalTurnId))
      ) {
        return false;
      }
      if (
        requiresProviderAbsence &&
        !(yield* providerSessionIsProvablyGone({
          adapter: source.adapter,
          provider: source.provider,
          threadId: event.threadId,
        }))
      ) {
        return false;
      }
      const nextActiveTurnId =
        event.payload.state === "waiting" || event.payload.state === "running"
          ? (event.turnId ?? null)
          : null;
      yield* directory.upsert({
        threadId: event.threadId,
        provider: source.provider,
        providerInstanceId: source.instanceId,
        runtimeMode: binding?.runtimeMode ?? "full-access",
        status: runtimeStatusFromSessionState(event.payload.state),
        ...(resumeCursor !== undefined
          ? { resumeCursor }
          : binding?.resumeCursor !== undefined
            ? { resumeCursor: binding.resumeCursor }
            : {}),
        runtimePayload: {
          ...previousPayload,
          activeTurnId: nextActiveTurnId,
          ...(terminalSessionState
            ? {
                sendTurnOperationId: null,
                activeTurnSendTurnOperationId: null,
                ...(bindingActiveTurnId !== undefined || event.turnId !== undefined
                  ? { lastTerminalTurnId: bindingActiveTurnId ?? event.turnId }
                  : {}),
              }
            : {}),
          ...(event.payload.state === "error" && event.payload.reason !== undefined
            ? { lastError: event.payload.reason }
            : {}),
          lastRuntimeEvent: event.type,
          lastRuntimeEventAt: event.createdAt,
        },
      });
      return true;
    });
    return terminalSessionState
      ? withThreadSessionLock(event.threadId, persistState)
      : persistState;
  };

  const persistSessionExitRuntimeState = (
    source: {
      readonly instanceId: ProviderInstanceId;
      readonly provider: ProviderDriverKind;
      readonly adapterGeneration: number;
      readonly adapter: ProviderAdapterShape<ProviderAdapterError>;
    },
    event: ProviderRuntimeEvent,
    onAccepted: (event: ProviderRuntimeEvent) => Effect.Effect<void>,
  ) => {
    if (event.type !== "session.exited") {
      return Effect.succeed(Option.some(event));
    }

    return withThreadSessionLock(
      event.threadId,
      Effect.gen(function* () {
        if (!(yield* isCurrentAdapterGeneration(source.instanceId, source.adapterGeneration))) {
          return Option.none<ProviderRuntimeEvent>();
        }
        // Ownership checks must fail closed: treating a transient read error
        // as an absent binding could let an old exit overwrite a replacement.
        const binding = Option.getOrUndefined(yield* directory.getBinding(event.threadId));
        if (
          binding !== undefined &&
          (binding.provider !== source.provider || binding.providerInstanceId !== source.instanceId)
        ) {
          return Option.none<ProviderRuntimeEvent>();
        }
        const previousPayload = readRecord(binding?.runtimePayload) ?? {};
        const boundMcpProviderSessionId =
          typeof previousPayload.mcpProviderSessionId === "string"
            ? previousPayload.mcpProviderSessionId
            : undefined;
        const currentMcpSession = McpProviderSession.readMcpProviderSession(event.threadId);
        const currentMcpProviderSessionId =
          currentMcpSession?.providerInstanceId === source.instanceId
            ? currentMcpSession.providerSessionId
            : undefined;
        const authoritativeMcpProviderSessionId =
          currentMcpProviderSessionId ?? boundMcpProviderSessionId;
        const eventMcpProviderSessionId = event.payload.mcpProviderSessionId;
        const bindingActiveTurnId = readPersistedActiveTurnId(previousPayload);
        const lastTerminalTurnId =
          typeof previousPayload.lastTerminalTurnId === "string"
            ? previousPayload.lastTerminalTurnId
            : undefined;
        if (
          event.turnId !== undefined &&
          ((bindingActiveTurnId !== undefined && event.turnId !== bindingActiveTurnId) ||
            (bindingActiveTurnId === undefined &&
              lastTerminalTurnId !== undefined &&
              event.turnId !== lastTerminalTurnId))
        ) {
          return Option.none<ProviderRuntimeEvent>();
        }
        if (eventMcpProviderSessionId === undefined) {
          // Adapter cleanup can lag event emission under load. Poll for a
          // definitive disappearance instead of relying on one timing sample.
          // An exact turn id is insufficient because a replacement may resume
          // that same provider turn; a still-live session therefore makes any
          // session-id-less exit ambiguous.
          // If ownership stays ambiguous, persist the observation so the
          // watchdog can reconcile it from later live-session snapshots.
          if (
            !(yield* providerSessionIsProvablyGone({
              adapter: source.adapter,
              provider: source.provider,
              threadId: event.threadId,
            }))
          ) {
            if (binding !== undefined) {
              yield* directory.upsert({
                threadId: event.threadId,
                provider: binding.provider,
                providerInstanceId: source.instanceId,
                runtimeMode: binding.runtimeMode ?? "full-access",
                status: binding.status ?? "running",
                ...(binding.resumeCursor !== undefined
                  ? { resumeCursor: binding.resumeCursor }
                  : {}),
                runtimePayload: {
                  ...previousPayload,
                  unconfirmedSessionExit: {
                    eventId: event.eventId,
                    observedAt: event.createdAt,
                    ...(event.payload.reason !== undefined ? { reason: event.payload.reason } : {}),
                  },
                },
              });
            }
            return Option.none<ProviderRuntimeEvent>();
          }
        }
        if (
          eventMcpProviderSessionId !== undefined &&
          authoritativeMcpProviderSessionId !== undefined &&
          eventMcpProviderSessionId !== authoritativeMcpProviderSessionId
        ) {
          return Option.none<ProviderRuntimeEvent>();
        }
        const activeTurnId = previousPayload.activeTurnId;
        const correlatedTurnId = event.turnId ?? bindingActiveTurnId;
        const providerExitIsError = event.payload.exitKind !== "graceful";
        yield* directory.upsert({
          threadId: event.threadId,
          provider: source.provider,
          providerInstanceId: source.instanceId,
          runtimeMode: binding?.runtimeMode ?? "full-access",
          status: providerExitIsError ? "error" : "stopped",
          ...(binding?.resumeCursor !== undefined ? { resumeCursor: binding.resumeCursor } : {}),
          runtimePayload: {
            ...previousPayload,
            activeTurnId: null,
            sendTurnOperationId: null,
            activeTurnSendTurnOperationId: null,
            ...(activeTurnId !== null && activeTurnId !== undefined
              ? { lastTerminalTurnId: activeTurnId }
              : {}),
            ...(event.payload.reason !== undefined ? { lastError: event.payload.reason } : {}),
            lastRuntimeEvent: event.type,
            lastRuntimeEventAt: event.createdAt,
            unconfirmedSessionExit: null,
          },
        });
        const canonicalEvent =
          correlatedTurnId === undefined ? event : { ...event, turnId: correlatedTurnId };
        yield* onAccepted(canonicalEvent);
        return Option.some<ProviderRuntimeEvent>(canonicalEvent);
      }),
    );
  };

  const persistTerminalTurnRuntimeState = (
    source: {
      readonly instanceId: ProviderInstanceId;
      readonly provider: ProviderDriverKind;
      readonly adapterGeneration: number;
    },
    event: ProviderRuntimeEvent,
  ) => {
    if (!isTerminalTurnRuntimeEvent(event) || event.turnId === undefined) {
      return Effect.void;
    }

    return Effect.gen(function* () {
      const binding = Option.getOrUndefined(
        yield* directory
          .getBinding(event.threadId)
          .pipe(
            Effect.orElseSucceed(() =>
              Option.none<ProviderSessionDirectory.ProviderRuntimeBinding>(),
            ),
          ),
      );
      if (!binding) {
        return;
      }
      if (
        binding.provider !== source.provider ||
        binding.providerInstanceId !== source.instanceId
      ) {
        return;
      }
      if (!(yield* isCurrentAdapterGeneration(source.instanceId, source.adapterGeneration))) {
        return;
      }
      const previousPayload = readRecord(binding.runtimePayload) ?? {};
      const persistedActiveTurnId = previousPayload.activeTurnId;
      const emptyResponseFailureTurnIds = readEmptyResponseFailureTurnIds(previousPayload);
      const isEmptyResponseFailure =
        event.type === "turn.completed" &&
        event.payload.state === "failed" &&
        event.payload.errorMessage === PROVIDER_EMPTY_RESPONSE_ERROR;
      if (
        persistedActiveTurnId !== null &&
        persistedActiveTurnId !== undefined &&
        String(persistedActiveTurnId) !== String(event.turnId)
      ) {
        if (isEmptyResponseFailure && !emptyResponseFailureTurnIds.includes(String(event.turnId))) {
          // Grace may prove an ingress-owned empty failure after a replacement
          // turn becomes active. Persist only the historical replay marker;
          // the newer owner's status, active id, and last-error state remain
          // authoritative for the session.
          yield* directory.upsert({
            threadId: event.threadId,
            provider: source.provider,
            providerInstanceId: source.instanceId,
            runtimeMode: binding.runtimeMode ?? "full-access",
            status: binding.status ?? "running",
            ...(binding.resumeCursor !== undefined ? { resumeCursor: binding.resumeCursor } : {}),
            runtimePayload: {
              ...previousPayload,
              emptyResponseFailureTurnIds: [...emptyResponseFailureTurnIds, String(event.turnId)],
            },
          });
        }
        return;
      }
      const activeTurnMatches =
        persistedActiveTurnId !== null &&
        persistedActiveTurnId !== undefined &&
        String(persistedActiveTurnId) === String(event.turnId);
      if (
        !activeTurnMatches &&
        isEmptyResponseFailure &&
        emptyResponseFailureTurnIds.includes(String(event.turnId))
      ) {
        // Historical duplicate: publish the preserved failure classification
        // without letting it replace the current session's runtime status.
        return;
      }

      yield* directory.upsert({
        threadId: event.threadId,
        provider: source.provider,
        providerInstanceId: source.instanceId,
        runtimeMode: binding.runtimeMode ?? "full-access",
        status: binding.status ?? "running",
        ...(binding.resumeCursor !== undefined ? { resumeCursor: binding.resumeCursor } : {}),
        runtimePayload: {
          ...previousPayload,
          ...(activeTurnMatches
            ? {
                activeTurnId: null,
                activeTurnSendTurnOperationId: null,
              }
            : {}),
          ...(activeTurnMatches &&
          event.type === "turn.completed" &&
          event.payload.state === "completed" &&
          previousPayload.lastError === PROVIDER_EMPTY_RESPONSE_ERROR
            ? { lastError: null }
            : {}),
          ...(isEmptyResponseFailure
            ? {
                lastError: PROVIDER_EMPTY_RESPONSE_ERROR,
                emptyResponseFailureTurnIds: emptyResponseFailureTurnIds.includes(
                  String(event.turnId),
                )
                  ? emptyResponseFailureTurnIds
                  : [...emptyResponseFailureTurnIds, String(event.turnId)],
              }
            : {}),
          lastTerminalTurnId: event.turnId,
          lastRuntimeEvent: event.type,
          lastRuntimeEventAt: event.createdAt,
        },
      });
    });
  };

  const persistStartedTurnRuntimeState = (
    source: {
      readonly instanceId: ProviderInstanceId;
      readonly provider: ProviderDriverKind;
      readonly adapterGeneration: number;
    },
    event: ProviderRuntimeEvent,
  ) => {
    if (!isStartedTurnRuntimeEvent(event) || event.turnId === undefined) {
      return Effect.succeed(false);
    }

    return Effect.gen(function* () {
      const binding = Option.getOrUndefined(
        yield* directory
          .getBinding(event.threadId)
          .pipe(
            Effect.orElseSucceed(() =>
              Option.none<ProviderSessionDirectory.ProviderRuntimeBinding>(),
            ),
          ),
      );
      if (!binding) {
        return false;
      }
      if (
        binding.provider !== source.provider ||
        binding.providerInstanceId !== source.instanceId
      ) {
        return false;
      }
      if (!(yield* isCurrentAdapterGeneration(source.instanceId, source.adapterGeneration))) {
        return false;
      }
      const previousPayload = readRecord(binding.runtimePayload) ?? {};
      const lastTerminalTurnId = previousPayload.lastTerminalTurnId;
      if (
        lastTerminalTurnId !== null &&
        lastTerminalTurnId !== undefined &&
        String(lastTerminalTurnId) === String(event.turnId)
      ) {
        return false;
      }
      const persistedActiveTurnId = previousPayload.activeTurnId;
      if (
        persistedActiveTurnId !== null &&
        persistedActiveTurnId !== undefined &&
        String(persistedActiveTurnId) !== String(event.turnId)
      ) {
        return false;
      }

      yield* directory.upsert({
        threadId: event.threadId,
        provider: source.provider,
        providerInstanceId: source.instanceId,
        runtimeMode: binding.runtimeMode ?? "full-access",
        status: "running",
        ...(binding.resumeCursor !== undefined ? { resumeCursor: binding.resumeCursor } : {}),
        runtimePayload: {
          ...previousPayload,
          activeTurnId: event.turnId,
          ...(previousPayload.lastError === PROVIDER_EMPTY_RESPONSE_ERROR
            ? { lastError: null }
            : {}),
          activeTurnSendTurnOperationId:
            typeof previousPayload.sendTurnOperationId === "string"
              ? previousPayload.sendTurnOperationId
              : null,
          lastRuntimeEvent: event.type,
          lastRuntimeEventAt: event.createdAt,
        },
      });
      return true;
    });
  };

  const registerSendTurnOperation = Effect.fn("ProviderService.registerSendTurnOperation")(
    function* (input: {
      readonly source: {
        readonly instanceId: ProviderInstanceId;
        readonly provider: ProviderDriverKind;
      };
      readonly threadId: ThreadId;
    }) {
      return yield* withThreadSessionLock(
        input.threadId,
        Effect.gen(function* () {
          const binding = Option.getOrUndefined(yield* directory.getBinding(input.threadId));
          if (
            binding === undefined ||
            binding.provider !== input.source.provider ||
            binding.providerInstanceId !== input.source.instanceId
          ) {
            return yield* toValidationError(
              "ProviderService.sendTurn",
              `Provider binding changed before send-turn registration for thread '${input.threadId}'.`,
            );
          }
          const currentPayload = readRecord(binding.runtimePayload) ?? {};
          const sessionOwnershipId =
            typeof currentPayload.sessionOwnershipId === "string"
              ? currentPayload.sessionOwnershipId
              : NodeCrypto.randomUUID();
          const activeOperationId = (yield* Ref.get(activeSendTurnOperations)).get(input.threadId);
          if (
            activeOperationId !== undefined &&
            currentPayload.sendTurnOperationId === activeOperationId
          ) {
            return yield* new ProviderSendTurnFailedError({
              provider: input.source.provider,
              threadId: input.threadId,
              detail: "Another session/prompt request is already in flight for this thread.",
              sessionOwnershipId,
              superseded: true,
              overlapping: true,
            });
          }
          const operationId = NodeCrypto.randomUUID();
          yield* directory.upsert({
            threadId: input.threadId,
            provider: binding.provider,
            providerInstanceId: input.source.instanceId,
            runtimeMode: binding.runtimeMode ?? "full-access",
            status: binding.status ?? "running",
            ...(binding.resumeCursor !== undefined ? { resumeCursor: binding.resumeCursor } : {}),
            runtimePayload: {
              ...currentPayload,
              sessionOwnershipId,
              sendTurnOperationId: operationId,
              lastFailedSendTurnOperationId: null,
              failedSendPreservedActiveTurnId: null,
            },
          });
          yield* Ref.update(activeSendTurnOperations, (current) => {
            const next = new Map(current);
            next.set(input.threadId, operationId);
            return next;
          });
          return { sessionOwnershipId, operationId } as const;
        }),
      );
    },
  );

  const clearFailedSendTurnRuntimeState = (
    source: {
      readonly instanceId: ProviderInstanceId;
      readonly provider: ProviderDriverKind;
    },
    threadId: ThreadId,
    ownership: {
      readonly sessionOwnershipId: string;
      readonly operationId: string;
    },
    errorMessage: string,
  ) =>
    withThreadSessionLock(
      threadId,
      Effect.gen(function* () {
        const binding = Option.getOrUndefined(yield* directory.getBinding(threadId));
        if (
          binding === undefined ||
          binding.provider !== source.provider ||
          binding.providerInstanceId !== source.instanceId
        ) {
          return { superseded: true } as const;
        }

        const currentPayload = readRecord(binding.runtimePayload) ?? {};
        if (
          currentPayload.sessionOwnershipId !== ownership.sessionOwnershipId ||
          currentPayload.sendTurnOperationId !== ownership.operationId
        ) {
          return { superseded: true } as const;
        }
        const currentActiveTurnId =
          typeof currentPayload.activeTurnId === "string"
            ? TurnId.make(currentPayload.activeTurnId)
            : undefined;
        const activeTurnStartedByThisOperation =
          currentActiveTurnId !== undefined &&
          currentPayload.activeTurnSendTurnOperationId === ownership.operationId;
        if (currentActiveTurnId !== undefined && !activeTurnStartedByThisOperation) {
          // A steer can fail while an older provider turn is still running.
          // Retire only this request token: the pre-existing turn did not start
          // inside this operation and must remain available to runtime events.
          yield* directory.upsert({
            threadId,
            provider: source.provider,
            providerInstanceId: source.instanceId,
            runtimeMode: binding.runtimeMode ?? "full-access",
            status: binding.status ?? "running",
            ...(binding.resumeCursor !== undefined ? { resumeCursor: binding.resumeCursor } : {}),
            runtimePayload: {
              ...currentPayload,
              sendTurnOperationId: null,
              lastFailedSendTurnOperationId: ownership.operationId,
              failedSendPreservedActiveTurnId: currentActiveTurnId,
            },
          });
          return {
            superseded: false,
            preservedActiveTurnId: currentActiveTurnId,
          } as const;
        }
        const failedAt = yield* nowIso;
        yield* directory.upsert({
          threadId,
          provider: source.provider,
          providerInstanceId: source.instanceId,
          runtimeMode: binding.runtimeMode ?? "full-access",
          status: currentActiveTurnId === undefined ? (binding.status ?? "running") : "error",
          ...(binding.resumeCursor !== undefined ? { resumeCursor: binding.resumeCursor } : {}),
          runtimePayload: {
            ...currentPayload,
            activeTurnId: null,
            sendTurnOperationId: null,
            activeTurnSendTurnOperationId: null,
            lastFailedSendTurnOperationId: ownership.operationId,
            failedSendPreservedActiveTurnId: null,
            lastError: errorMessage,
            ...(currentActiveTurnId !== undefined
              ? { lastTerminalTurnId: currentActiveTurnId }
              : {}),
            lastRuntimeEvent: "provider.sendTurn.failed",
            lastRuntimeEventAt: failedAt,
          },
        });
        return {
          superseded: false,
          ...(currentActiveTurnId !== undefined ? { turnId: currentActiveTurnId } : {}),
        } as const;
      }),
    );

  const shouldFenceRuntimeEventAfterSendFailure = Effect.fn(
    "ProviderService.shouldFenceRuntimeEventAfterSendFailure",
  )(function* (
    source: {
      readonly instanceId: ProviderInstanceId;
      readonly provider: ProviderDriverKind;
    },
    event: ProviderRuntimeEvent,
  ) {
    const canReviveFailedSend =
      event.type === "turn.started" ||
      (event.type === "session.state.changed" &&
        (event.payload.state === "starting" ||
          event.payload.state === "running" ||
          event.payload.state === "waiting"));
    if (!canReviveFailedSend) return false;

    const binding = Option.getOrUndefined(yield* directory.getBinding(event.threadId));
    if (
      binding === undefined ||
      binding.provider !== source.provider ||
      binding.providerInstanceId !== source.instanceId
    ) {
      return false;
    }
    const runtimePayload = readRecord(binding.runtimePayload) ?? {};
    const failedOperationId = runtimePayload.lastFailedSendTurnOperationId;
    if (typeof failedOperationId !== "string" || runtimePayload.sendTurnOperationId !== null) {
      return false;
    }
    const preservedActiveTurnId = runtimePayload.failedSendPreservedActiveTurnId;
    if (typeof preservedActiveTurnId === "string") {
      return event.turnId !== undefined && String(event.turnId) !== preservedActiveTurnId;
    }
    return true;
  });

  const handleAcceptedRuntimeEventNow = Effect.fn("ProviderService.handleAcceptedRuntimeEventNow")(
    function* (
      source: {
        readonly instanceId: ProviderInstanceId;
        readonly provider: ProviderDriverKind;
        readonly adapterGeneration: number;
        readonly adapter: ProviderAdapterShape<ProviderAdapterError>;
      },
      canonicalEvent: ProviderRuntimeEvent,
      sessionEndContext?: ProviderSessionEndContext,
      terminalTurnOwnedAtIngress?: boolean,
      threadSessionLockHeld = false,
    ) {
      if (!(yield* isCurrentAdapterGeneration(source.instanceId, source.adapterGeneration))) {
        yield* Effect.logWarning("provider.runtime-event.stale-adapter-generation-dropped", {
          outcome: "stale-adapter-generation",
          eventId: canonicalEvent.eventId,
          eventType: canonicalEvent.type,
          threadId: canonicalEvent.threadId,
          provider: canonicalEvent.provider,
          providerInstanceId: source.instanceId,
          adapterGeneration: source.adapterGeneration,
        });
        return;
      }

      const liveSessions = yield* source.adapter.listSessions().pipe(
        Effect.catchCause((cause) =>
          Effect.logWarning("provider.runtime-event.live-binding-read-failed", {
            threadId: canonicalEvent.threadId,
            provider: canonicalEvent.provider,
            cause,
          }).pipe(Effect.as([] as ReadonlyArray<ProviderSession>)),
        ),
      );
      const liveBinding = liveSessions.find(
        (session) => session.threadId === canonicalEvent.threadId,
      );
      const persistedBinding =
        liveBinding === undefined
          ? Option.getOrUndefined(
              yield* directory.getBinding(canonicalEvent.threadId).pipe(
                Effect.catchCause((cause) =>
                  Effect.logWarning("provider.runtime-event.persisted-binding-read-failed", {
                    threadId: canonicalEvent.threadId,
                    provider: canonicalEvent.provider,
                    cause,
                  }).pipe(
                    Effect.as(Option.none<ProviderSessionDirectory.ProviderRuntimeBinding>()),
                  ),
                ),
              ),
            )
          : undefined;
      const persistedBindingIsEligible =
        persistedBinding !== undefined &&
        (canonicalEvent.type === "session.exited" ||
          persistedBinding.status === "starting" ||
          persistedBinding.status === "running" ||
          persistedBinding.status === "waiting");
      const binding: CapturedProviderRuntimeEventBinding | undefined =
        liveBinding !== undefined
          ? {
              threadId: liveBinding.threadId,
              provider: liveBinding.provider,
              providerInstanceId: source.instanceId,
              runtimeMode: liveBinding.runtimeMode,
              cwd: liveBinding.cwd,
            }
          : persistedBindingIsEligible
            ? {
                threadId: persistedBinding.threadId,
                provider: persistedBinding.provider,
                providerInstanceId: persistedBinding.providerInstanceId,
                runtimeMode: persistedBinding.runtimeMode,
                cwd: undefined,
              }
            : undefined;

      const handleEvent = Effect.gen(function* () {
        const shouldFence = yield* shouldFenceRuntimeEventAfterSendFailure(
          source,
          canonicalEvent,
        ).pipe(
          Effect.catchCause((cause) =>
            Effect.logWarning("provider.session.failed-send-fence-read-failed", {
              threadId: canonicalEvent.threadId,
              provider: canonicalEvent.provider,
              cause,
            }).pipe(Effect.as(true)),
          ),
        );
        if (shouldFence) {
          yield* Effect.logDebug("provider.session.failed-send-runtime-event-ignored", {
            threadId: canonicalEvent.threadId,
            provider: canonicalEvent.provider,
            eventId: canonicalEvent.eventId,
            eventType: canonicalEvent.type,
          });
          return;
        }
        const sessionStateAccepted = yield* persistSessionStateRuntimeState(
          source,
          canonicalEvent,
        ).pipe(
          Effect.catchCause((cause) =>
            Effect.logWarning("provider.session.state-persist-failed", {
              threadId: canonicalEvent.threadId,
              provider: canonicalEvent.provider,
              cause,
            }).pipe(Effect.as(false)),
          ),
        );
        if (!sessionStateAccepted) return;
        const startedTurnAccepted = yield* persistStartedTurnRuntimeState(
          source,
          canonicalEvent,
        ).pipe(
          Effect.catchCause((cause) =>
            Effect.logWarning("provider.session.turn-start-persist-failed", {
              threadId: canonicalEvent.threadId,
              provider: canonicalEvent.provider,
              cause,
            }).pipe(Effect.as(false)),
          ),
        );
        const terminalTurnOwnershipOption =
          terminalTurnOwnedAtIngress === undefined
            ? yield* readTerminalTurnOwnership(source, canonicalEvent)
            : Option.some({
                ownsTurn: terminalTurnOwnedAtIngress,
                replaysEmptyFailure: false,
              } as const);
        if (
          Option.isNone(terminalTurnOwnershipOption) &&
          canonicalEvent.type === "turn.completed" &&
          canonicalEvent.payload.state === "completed"
        ) {
          return;
        }
        const terminalTurnOwnership = Option.getOrElse(terminalTurnOwnershipOption, () => ({
          // Provider-declared failures and aborts remain authoritative even
          // when ownership is unreadable. False only prevents uncertain
          // tracker cleanup; it must not suppress the terminal notification.
          ownsTurn: false,
          replaysEmptyFailure: false,
        }));
        const acceptedEvent = guardEmptyAssistantResponse(
          source,
          canonicalEvent,
          startedTurnAccepted,
          terminalTurnOwnership.ownsTurn,
          terminalTurnOwnership.replaysEmptyFailure,
        );
        yield* persistTerminalTurnRuntimeState(source, acceptedEvent).pipe(
          Effect.catchCause((cause) =>
            Effect.logWarning("provider.session.terminal-turn-persist-failed", {
              threadId: acceptedEvent.threadId,
              provider: acceptedEvent.provider,
              cause,
            }),
          ),
        );
        yield* increment(providerRuntimeEventsTotal, {
          provider: acceptedEvent.provider,
          eventType: acceptedEvent.type,
        });
        if (sessionEndContext !== undefined) {
          yield* clearMcpSessionAfterProviderSessionEnds(source, acceptedEvent, {
            ...(sessionEndContext.providerSessionId !== undefined
              ? { providerSessionId: sessionEndContext.providerSessionId }
              : {}),
            duringPendingStart: sessionEndContext.duringPendingStart,
          }).pipe(Effect.forkDetach, Effect.asVoid);
        }
        yield* publishRuntimeEvent(acceptedEvent, binding);
      });

      const mustOrderAgainstSendFailure =
        canonicalEvent.type === "turn.started" ||
        isTerminalTurnRuntimeEvent(canonicalEvent) ||
        (canonicalEvent.type === "session.state.changed" &&
          canonicalEvent.payload.state !== "error" &&
          canonicalEvent.payload.state !== "stopped");
      yield* mustOrderAgainstSendFailure && !threadSessionLockHeld
        ? withThreadSessionLock(canonicalEvent.threadId, handleEvent)
        : handleEvent;
    },
  );

  const processCorrelatedRuntimeEvent = Effect.fn("ProviderService.processCorrelatedRuntimeEvent")(
    function* (
      source: ProviderRuntimeEventSource,
      correlatedEvent: ProviderRuntimeEvent,
      capturedSessionEndContext?: ProviderSessionEndContext,
      terminalTurnOwnedAtIngress?: boolean,
      threadSessionLockHeld = false,
    ) {
      const isSessionEnd = isProviderSessionEndEvent(correlatedEvent);
      if (isSessionEnd) {
        let sessionEndContext = capturedSessionEndContext;
        if (sessionEndContext === undefined) {
          const observedMcpSession = McpProviderSession.readMcpProviderSession(
            correlatedEvent.threadId,
          );
          sessionEndContext = {
            ...(observedMcpSession !== undefined
              ? { providerSessionId: observedMcpSession.providerSessionId }
              : {}),
            duringPendingStart: yield* hasPendingMcpSessionStart(correlatedEvent.threadId),
          };
        }
        const correlatedSessionExit = yield* persistSessionExitRuntimeState(
          source,
          correlatedEvent,
          (canonicalEvent) =>
            handleAcceptedRuntimeEventNow(source, canonicalEvent, {
              ...(sessionEndContext.providerSessionId !== undefined
                ? { providerSessionId: sessionEndContext.providerSessionId }
                : {}),
              duringPendingStart: sessionEndContext.duringPendingStart,
            }),
        ).pipe(
          Effect.catchCause((cause) =>
            Effect.logWarning("provider.session.exit-persist-failed", {
              threadId: correlatedEvent.threadId,
              provider: correlatedEvent.provider,
              cause,
            }).pipe(Effect.as(Option.none<ProviderRuntimeEvent>())),
          ),
        );
        if (Option.isNone(correlatedSessionExit)) {
          yield* Effect.logDebug("provider.session.stale-exit-ignored", {
            threadId: correlatedEvent.threadId,
            provider: correlatedEvent.provider,
            eventId: correlatedEvent.eventId,
          });
        }
        return;
      }
      yield* handleAcceptedRuntimeEventNow(
        source,
        correlatedEvent,
        undefined,
        terminalTurnOwnedAtIngress,
        threadSessionLockHeld,
      );
    },
  );

  let processRuntimeEvent: (
    source: ProviderRuntimeEventSource,
    event: ProviderRuntimeEvent,
    capturedSessionEndContext?: ProviderSessionEndContext,
  ) => Effect.Effect<void>;

  const drainPendingEmptyResponseCompletion = Effect.fn(
    "ProviderService.drainPendingEmptyResponseCompletion",
  )(function* (pending: PendingEmptyResponseCompletion) {
    if (pending.draining) {
      yield* Deferred.await(pending.settled);
      return;
    }
    pending.draining = true;
    const closePending = () => {
      if (pending.closed) return;
      pending.closed = true;
      clearTrackedTurnOutput(
        pending.source.adapter,
        pending.source.instanceId,
        pending.threadId,
        pending.turnId,
      );
      deletePendingEmptyResponseCompletion(pending);
    };

    // Freeze the empty-response decision at the grace boundary, then replay
    // every buffered event in source order.
    const outputBoundaryIndex = pending.events.findIndex(
      ({ event }, index) =>
        index > 0 &&
        (isProviderSessionEndEvent(event) ||
          (event.type === "turn.started" &&
            event.turnId !== undefined &&
            String(event.turnId) !== String(pending.turnId)) ||
          ((event.type === "turn.aborted" ||
            (event.type === "turn.completed" && event.payload.state !== "completed")) &&
            event.turnId !== undefined &&
            String(event.turnId) === String(pending.turnId)) ||
          (event.type === "session.state.changed" &&
            (event.payload.state === "error" || event.payload.state === "stopped"))),
    );
    const outputBoundary = outputBoundaryIndex === -1 ? pending.events.length : outputBoundaryIndex;
    const observedLateOutput = pending.events
      .slice(0, outputBoundary)
      .some(
        ({ event }) =>
          event.turnId !== undefined &&
          String(event.turnId) === String(pending.turnId) &&
          isMeaningfulTurnOutputEvent(event),
      );
    yield* Effect.gen(function* () {
      let eventIndex = 0;
      let replayThroughGraceAwarePath = false;
      while (true) {
        if (eventIndex >= pending.events.length) {
          // Closing and removing the buffer is synchronous with the final
          // length check. An ingress fiber that resumes afterward will fail
          // its identity recheck and process the event normally.
          closePending();
          break;
        }
        const bufferedIndex = eventIndex;
        const buffered = pending.events[bufferedIndex];
        eventIndex += 1;
        if (buffered !== undefined) {
          const opensReplacementTurnBoundary =
            buffered.event.turnId !== undefined &&
            String(buffered.event.turnId) !== String(pending.turnId) &&
            (buffered.event.type === "turn.started" ||
              (buffered.event.type === "turn.completed" &&
                buffered.event.payload.state === "completed"));
          if (!replayThroughGraceAwarePath && opensReplacementTurnBoundary) {
            // The prior turn's grace decision ends at replacement ownership.
            // Detect this while consuming the live array because ingress can
            // append a replacement after draining starts, including a turn
            // owned through sendTurn with no turn.started event. Remove this
            // buffer before replaying the replacement lifecycle so a valid
            // owner establishes an independent Cursor grace window.
            closePending();
            replayThroughGraceAwarePath = true;
          }
          if (replayThroughGraceAwarePath) {
            yield* processRuntimeEvent(pending.source, buffered.event, buffered.sessionEndContext);
          } else {
            if (
              observedLateOutput &&
              bufferedIndex < outputBoundary &&
              buffered.event.type === "turn.completed" &&
              buffered.event.payload.state === "completed" &&
              buffered.event.turnId !== undefined &&
              String(buffered.event.turnId) === String(pending.turnId)
            ) {
              // A grace-boundary output decision belongs to the turn, not to
              // one completion notification. Re-seed it for every buffered
              // successful duplicate because the guard consumes the bit.
              trackTurnOutput(
                pending.source.adapter,
                pending.source.instanceId,
                pending.threadId,
                pending.turnId,
              );
            }
            yield* processCorrelatedRuntimeEvent(
              pending.source,
              buffered.event,
              buffered.sessionEndContext,
              buffered.terminalTurnOwnedAtIngress,
            );
          }
        }
      }
    }).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          closePending();
        }).pipe(Effect.andThen(Deferred.succeed(pending.settled, undefined)), Effect.asVoid),
      ),
    );
  });

  const flushPendingEmptyResponseCompletionsForAdapter = (
    adapter: ProviderAdapterShape<ProviderAdapterError>,
    instanceId: ProviderInstanceId,
  ): Effect.Effect<void> =>
    Effect.gen(function* () {
      while (true) {
        const sessions = pendingEmptyResponseCompletionsByAdapter.get(adapter)?.get(instanceId);
        if (sessions === undefined || sessions.size === 0) return;

        // Draining can replay a buffered replacement turn through the grace-aware
        // path and create another pending completion. Re-fetch after every pass
        // so adapter replacement/removal cannot advance its generation while any
        // completion created by the old generation is still pending.
        yield* Effect.forEach(Array.from(sessions.values()), drainPendingEmptyResponseCompletion, {
          concurrency: "unbounded",
          discard: true,
        });
      }
    });

  const terminalEventOwnsPersistedActiveTurn = Effect.fn(
    "ProviderService.terminalEventOwnsPersistedActiveTurn",
  )(function* (source: ProviderRuntimeEventSource, event: ProviderRuntimeEvent) {
    if (!isTerminalTurnRuntimeEvent(event) || event.turnId === undefined) {
      return { ownsTurn: false, replaysEmptyFailure: false } as const;
    }
    const binding = Option.getOrUndefined(yield* directory.getBinding(event.threadId));
    if (
      binding === undefined ||
      binding.provider !== source.provider ||
      binding.providerInstanceId !== source.instanceId
    ) {
      return { ownsTurn: false, replaysEmptyFailure: false } as const;
    }
    const runtimePayload = readRecord(binding.runtimePayload) ?? {};
    const activeTurnId = runtimePayload.activeTurnId;
    const activeTurnMatches =
      activeTurnId !== null &&
      activeTurnId !== undefined &&
      String(activeTurnId) === String(event.turnId);
    const replaysEmptyFailure =
      readEmptyResponseFailureTurnIds(runtimePayload).includes(String(event.turnId)) ||
      (runtimePayload.lastError === PROVIDER_EMPTY_RESPONSE_ERROR &&
        runtimePayload.lastTerminalTurnId !== null &&
        runtimePayload.lastTerminalTurnId !== undefined &&
        String(runtimePayload.lastTerminalTurnId) === String(event.turnId));
    return {
      ownsTurn: activeTurnMatches || replaysEmptyFailure,
      replaysEmptyFailure,
    } as const;
  });

  const readTerminalTurnOwnership = Effect.fn("ProviderService.readTerminalTurnOwnership")(
    function* (source: ProviderRuntimeEventSource, event: ProviderRuntimeEvent) {
      return yield* terminalEventOwnsPersistedActiveTurn(source, event).pipe(
        Effect.retry({ times: 2 }),
        Effect.map(Option.some),
        Effect.catchCause((cause) =>
          Effect.logWarning("provider.session.terminal-turn-ownership-read-failed", {
            threadId: event.threadId,
            provider: event.provider,
            cause,
          }).pipe(
            // Ownership is a prerequisite for accepting a successful terminal
            // event. If every bounded retry fails, suppress this notification
            // instead of treating unreadable state as proof that it is unowned.
            Effect.as(
              Option.none<{
                readonly ownsTurn: boolean;
                readonly replaysEmptyFailure: boolean;
              }>(),
            ),
          ),
        ),
      );
    },
  );

  const captureBufferedRuntimeEvent = Effect.fn("ProviderService.captureBufferedRuntimeEvent")(
    function* (event: ProviderRuntimeEvent): Effect.fn.Return<BufferedProviderRuntimeEvent> {
      if (!isProviderSessionEndEvent(event)) return { event };
      const observedMcpSession = McpProviderSession.readMcpProviderSession(event.threadId);
      const duringPendingStart = yield* hasPendingMcpSessionStart(event.threadId);
      return {
        event,
        sessionEndContext: {
          ...(observedMcpSession !== undefined
            ? { providerSessionId: observedMcpSession.providerSessionId }
            : {}),
          duringPendingStart,
        },
      };
    },
  );

  processRuntimeEvent = (
    source: ProviderRuntimeEventSource,
    event: ProviderRuntimeEvent,
    capturedSessionEndContext?: ProviderSessionEndContext,
  ): Effect.Effect<void> =>
    Effect.sync(() => correlateRuntimeEventWithInstance(source, event)).pipe(
      Effect.flatMap((correlatedEvent) =>
        Effect.gen(function* () {
          const pending = getPendingEmptyResponseCompletion(source, correlatedEvent.threadId);
          if (pending !== undefined) {
            const capturedEvent =
              capturedSessionEndContext === undefined
                ? yield* captureBufferedRuntimeEvent(correlatedEvent)
                : {
                    event: correlatedEvent,
                    sessionEndContext: capturedSessionEndContext,
                  };
            const bufferedTerminalOwnsPendingTurn =
              pending.ownershipOpen &&
              isTerminalTurnRuntimeEvent(correlatedEvent) &&
              correlatedEvent.turnId !== undefined &&
              String(correlatedEvent.turnId) === String(pending.turnId);
            const bufferedEvent: BufferedProviderRuntimeEvent = {
              ...capturedEvent,
              ...(bufferedTerminalOwnsPendingTurn ? { terminalTurnOwnedAtIngress: true } : {}),
            };
            // Capturing session-end context can yield. If the grace drain
            // settled in the meantime, process this event normally instead
            // of appending it to a buffer that is no longer reachable.
            if (getPendingEmptyResponseCompletion(source, correlatedEvent.threadId) !== pending) {
              yield* processRuntimeEvent(
                source,
                bufferedEvent.event,
                bufferedEvent.sessionEndContext,
              );
              return;
            }
            pending.events.push(bufferedEvent);
            if (
              isProviderSessionEndEvent(correlatedEvent) ||
              (correlatedEvent.type === "session.state.changed" &&
                (correlatedEvent.payload.state === "error" ||
                  correlatedEvent.payload.state === "stopped")) ||
              (correlatedEvent.type === "turn.started" &&
                correlatedEvent.turnId !== undefined &&
                String(correlatedEvent.turnId) !== String(pending.turnId)) ||
              ((correlatedEvent.type === "turn.aborted" ||
                (correlatedEvent.type === "turn.completed" &&
                  correlatedEvent.payload.state !== "completed")) &&
                correlatedEvent.turnId !== undefined &&
                String(correlatedEvent.turnId) === String(pending.turnId))
            ) {
              pending.ownershipOpen = false;
            }
            return;
          }

          const completionGraceMs = emptyResponseCompletionGraceMs(source.provider);
          if (
            completionGraceMs !== undefined &&
            correlatedEvent.type === "turn.completed" &&
            correlatedEvent.payload.state === "completed" &&
            correlatedEvent.turnId !== undefined
          ) {
            const completionTurnId = correlatedEvent.turnId;
            yield* withThreadSessionLock(
              correlatedEvent.threadId,
              Effect.gen(function* () {
                const pendingAfterBarrier = getPendingEmptyResponseCompletion(
                  source,
                  correlatedEvent.threadId,
                );
                if (pendingAfterBarrier !== undefined) {
                  const bufferedTerminalOwnsPendingTurn =
                    pendingAfterBarrier.ownershipOpen &&
                    String(completionTurnId) === String(pendingAfterBarrier.turnId);
                  pendingAfterBarrier.events.push({
                    event: correlatedEvent,
                    ...(bufferedTerminalOwnsPendingTurn
                      ? { terminalTurnOwnedAtIngress: true }
                      : {}),
                  });
                  return;
                }

                if (
                  !hasTrackedTurnOutput(
                    source.adapter,
                    source.instanceId,
                    correlatedEvent.threadId,
                    completionTurnId,
                  ) &&
                  (yield* isCurrentAdapterGeneration(source.instanceId, source.adapterGeneration))
                ) {
                  const terminalTurnOwnershipOption = yield* readTerminalTurnOwnership(
                    source,
                    correlatedEvent,
                  );
                  if (Option.isNone(terminalTurnOwnershipOption)) return;
                  const terminalTurnOwnership = terminalTurnOwnershipOption.value;
                  if (
                    terminalTurnOwnership.ownsTurn &&
                    !terminalTurnOwnership.replaysEmptyFailure
                  ) {
                    const settlement = yield* Deferred.make<void>();
                    const pendingCompletion: PendingEmptyResponseCompletion = {
                      source,
                      threadId: correlatedEvent.threadId,
                      turnId: completionTurnId,
                      events: [{ event: correlatedEvent, terminalTurnOwnedAtIngress: true }],
                      settled: settlement,
                      ownershipOpen: true,
                      draining: false,
                      closed: false,
                    };
                    setPendingEmptyResponseCompletion(pendingCompletion);
                    yield* Effect.sleep(`${completionGraceMs} millis`).pipe(
                      Effect.andThen(drainPendingEmptyResponseCompletion(pendingCompletion)),
                      Effect.forkIn(providerServiceScope),
                    );
                    return;
                  }
                }

                yield* processCorrelatedRuntimeEvent(
                  source,
                  correlatedEvent,
                  capturedSessionEndContext,
                  undefined,
                  true,
                );
              }),
            );
            return;
          }

          yield* processCorrelatedRuntimeEvent(source, correlatedEvent, capturedSessionEndContext);
        }),
      ),
    );

  // `subscribedAdapters` is our source-of-truth for "which instance adapters
  // are currently wired into the runtime event bus". It both tracks the set
  // of live subscriptions (so `reconcileInstanceSubscriptions` can diff and
  // fork only the *new* or *rebuilt* ones) and serves as the dynamic adapter
  // list consumed by `stopStaleSessionsForThread`, `listSessions`, and
  // `runStopAll` — replacing the pre-Slice-D startup snapshot so hot-added
  // instances become visible to those call sites as soon as settings edits
  // land.
  const subscribedAdapters = yield* Ref.make(
    new Map<ProviderInstanceId, ProviderAdapterShape<ProviderAdapterError>>(),
  );

  const getAdapterEntries = Ref.get(subscribedAdapters).pipe(
    Effect.map((map) => Array.from(map.entries())),
  );

  // Rebuild the map of id → adapter from the registry and fork a new event
  // subscription for every instance that is either brand new or whose adapter
  // identity changed (indicating the underlying `ProviderInstance` was torn
  // down and rebuilt by `ProviderInstanceRegistry.reconcile`). Orphaned
  // fibers for removed/replaced instances exit on their own because their
  // adapter's `streamEvents` source terminates when the old scope closes.
  const reconcileInstanceSubscriptions = Effect.gen(function* () {
    const previous = yield* Ref.get(subscribedAdapters);
    const currentIds = yield* registry.listInstances();
    const next = new Map<ProviderInstanceId, ProviderAdapterShape<ProviderAdapterError>>();
    for (const id of currentIds) {
      const adapterOption = yield* registry
        .getByInstance(id)
        .pipe(Effect.tapError(Effect.logWarning), Effect.option);
      if (Option.isNone(adapterOption)) continue;
      const adapter = adapterOption.value;
      const previousAdapter = previous.get(id);
      if (previousAdapter !== undefined && previousAdapter !== adapter) {
        yield* flushPendingEmptyResponseCompletionsForAdapter(previousAdapter, id);
      }
      const adapterGeneration = yield* observeAdapterGeneration(id, adapter);
      next.set(id, adapter);
      if (previousAdapter !== adapter) {
        yield* Stream.runForEach(adapter.streamEvents, (event) =>
          processRuntimeEvent(
            {
              instanceId: id,
              provider: adapter.provider,
              adapterGeneration,
              adapter,
            },
            event,
          ),
        ).pipe(Effect.forkScoped);
      }
    }
    yield* Effect.forEach(
      previous,
      ([instanceId, adapter]) => {
        if (next.get(instanceId) === adapter) return Effect.void;
        return flushPendingEmptyResponseCompletionsForAdapter(adapter, instanceId).pipe(
          Effect.andThen(getCurrentAdapterGeneration(instanceId)),
          Effect.flatMap((retainAdapterGeneration) =>
            clearMcpSessionsForProviderInstance(
              instanceId,
              next.has(instanceId) && retainAdapterGeneration !== undefined
                ? { retainAdapterGeneration }
                : {},
            ),
          ),
        );
      },
      { discard: true },
    );
    yield* Ref.update(adapterGenerations, (current) => {
      let changed = false;
      const pruned = new Map(current);
      for (const instanceId of previous.keys()) {
        if (next.has(instanceId)) continue;
        changed = pruned.delete(instanceId) || changed;
      }
      return changed ? pruned : current;
    });
    yield* Ref.set(subscribedAdapters, next);
  });

  const instanceChanges = yield* registry.subscribeChanges;
  yield* reconcileInstanceSubscriptions;
  yield* Stream.runForEach(
    Stream.fromSubscription(instanceChanges),
    () => reconcileInstanceSubscriptions,
  ).pipe(Effect.forkScoped);

  const recoverSessionForThread = Effect.fn("recoverSessionForThread")(function* (input: {
    readonly binding: ProviderSessionDirectory.ProviderRuntimeBinding;
    readonly operation: string;
  }) {
    const bindingInstanceId = yield* requireBindingInstanceId(input.operation, input.binding);
    yield* Effect.annotateCurrentSpan({
      "provider.operation": "recover-session",
      "provider.kind": input.binding.provider,
      "provider.instance_id": bindingInstanceId,
      "provider.thread_id": input.binding.threadId,
    });
    return yield* Effect.gen(function* () {
      const adapter = yield* registry.getByInstance(bindingInstanceId);
      const adapterGeneration = yield* getAdapterGenerationForStart(bindingInstanceId, adapter);
      if (adapterGeneration === 0) {
        return yield* toValidationError(
          input.operation,
          `Provider instance '${bindingInstanceId}' was replaced before recovering thread '${input.binding.threadId}'. Retry the operation.`,
        );
      }
      const hasResumeCursor =
        input.binding.resumeCursor !== null && input.binding.resumeCursor !== undefined;
      const hasActiveSession = yield* adapter.hasSession(input.binding.threadId);
      if (hasActiveSession) {
        const activeSessions = yield* adapter.listSessions();
        const existing = activeSessions.find(
          (session) => session.threadId === input.binding.threadId,
        );
        if (existing) {
          const existingWithInstance = withActiveTurnFallback(
            { ...existing, providerInstanceId: bindingInstanceId },
            readResumableActiveTurnId(input.binding.provider, input.binding.runtimePayload),
          );
          yield* upsertSessionBinding(existingWithInstance, input.binding.threadId);
          if (existingWithInstance.activeTurnId !== undefined) {
            preserveRecoveredActiveTurnOutput(
              adapter,
              bindingInstanceId,
              input.binding.threadId,
              existingWithInstance.activeTurnId,
            );
          }
          yield* analytics.record("provider.session.recovered", {
            provider: existingWithInstance.provider,
            strategy: "adopt-existing",
            hasResumeCursor: existingWithInstance.resumeCursor !== undefined,
          });
          return { adapter, session: existingWithInstance } as const;
        }
      }

      if (!hasResumeCursor) {
        return yield* toValidationError(
          input.operation,
          `Cannot recover thread '${input.binding.threadId}' because no provider resume state is persisted.`,
        );
      }

      const persistedCwd = readPersistedCwd(input.binding.runtimePayload);
      const persistedModelSelection = readPersistedModelSelection(input.binding.runtimePayload);
      const persistedDetached = readPersistedDetached(input.binding.runtimePayload);
      const persistedRuntimePayload = readRecord(input.binding.runtimePayload) ?? {};
      const persistedSessionOwnershipId =
        typeof persistedRuntimePayload.sessionOwnershipId === "string"
          ? persistedRuntimePayload.sessionOwnershipId
          : undefined;
      const persistedActiveTurnId = readResumableActiveTurnId(
        input.binding.provider,
        input.binding.runtimePayload,
      );

      yield* beginMcpSessionStart(input.binding.threadId);
      let preparedMcpSession: McpProviderSession.McpProviderSessionConfig | undefined;
      const resumed = yield* Effect.gen(function* () {
        preparedMcpSession = yield* prepareMcpSession(input.binding.threadId, bindingInstanceId);
        return yield* startAdapterSessionWithTimeout({
          adapter,
          request: {
            threadId: input.binding.threadId,
            provider: input.binding.provider,
            providerInstanceId: bindingInstanceId,
            ...(persistedCwd ? { cwd: persistedCwd } : {}),
            ...(persistedModelSelection ? { modelSelection: persistedModelSelection } : {}),
            ...(hasResumeCursor ? { resumeCursor: input.binding.resumeCursor } : {}),
            ...(persistedDetached !== undefined ? { detached: persistedDetached } : {}),
            ...(persistedActiveTurnId !== undefined ? { activeTurnId: persistedActiveTurnId } : {}),
            runtimeMode: input.binding.runtimeMode ?? "full-access",
          },
          ...(persistedSessionOwnershipId !== undefined
            ? { sessionOwnershipId: persistedSessionOwnershipId }
            : {}),
        });
      }).pipe(
        Effect.onError(() =>
          clearPreparedMcpSession(input.binding.threadId, bindingInstanceId, preparedMcpSession),
        ),
        Effect.ensuring(endMcpSessionStart(input.binding.threadId)),
      );
      if (resumed.provider !== adapter.provider) {
        yield* clearPreparedMcpSession(
          input.binding.threadId,
          bindingInstanceId,
          preparedMcpSession,
        );
        return yield* toValidationError(
          input.operation,
          `Adapter/provider mismatch while recovering thread '${input.binding.threadId}'. Expected '${adapter.provider}', received '${resumed.provider}'.`,
        );
      }
      if (!(yield* isCurrentAdapterGeneration(bindingInstanceId, adapterGeneration))) {
        yield* stopSupersededAdapterSession(adapter, input.binding.threadId, bindingInstanceId);
        yield* clearPreparedMcpSession(
          input.binding.threadId,
          bindingInstanceId,
          preparedMcpSession,
        );
        return yield* toValidationError(
          input.operation,
          `Provider instance '${bindingInstanceId}' was replaced while recovering thread '${input.binding.threadId}'. Retry the operation.`,
        );
      }
      yield* trackMcpSession(input.binding.threadId, bindingInstanceId, adapterGeneration);

      const resumedWithInstance = withActiveTurnFallback(
        { ...resumed, providerInstanceId: bindingInstanceId },
        persistedActiveTurnId,
      );
      yield* upsertSessionBinding(resumedWithInstance, input.binding.threadId);
      if (resumedWithInstance.activeTurnId !== undefined) {
        preserveRecoveredActiveTurnOutput(
          adapter,
          bindingInstanceId,
          input.binding.threadId,
          resumedWithInstance.activeTurnId,
        );
      }
      yield* analytics.record("provider.session.recovered", {
        provider: resumedWithInstance.provider,
        strategy: "resume-thread",
        hasResumeCursor: resumedWithInstance.resumeCursor !== undefined,
      });
      return { adapter, session: resumedWithInstance } as const;
    }).pipe(
      withMetrics({
        counter: providerSessionsTotal,
        attributes: providerMetricAttributes(input.binding.provider, {
          operation: "recover",
        }),
      }),
    );
  });

  const resolveRoutableSession = Effect.fn("resolveRoutableSession")(function* (input: {
    readonly threadId: ThreadId;
    readonly operation: string;
    readonly allowRecovery: boolean;
  }) {
    const bindingOption = yield* directory.getBinding(input.threadId);
    const binding = Option.getOrUndefined(bindingOption);
    if (!binding) {
      return yield* toValidationError(
        input.operation,
        `Cannot route thread '${input.threadId}' because no persisted provider binding exists.`,
      );
    }
    const instanceId = yield* requireBindingInstanceId(input.operation, binding);
    const adapter = yield* registry.getByInstance(instanceId);

    const hasRequestedSession = yield* adapter.hasSession(input.threadId);
    if (hasRequestedSession) {
      return {
        adapter,
        instanceId,
        threadId: input.threadId,
        runtimeMode: binding.runtimeMode,
        isActive: true,
      } as const;
    }

    if (!input.allowRecovery) {
      return {
        adapter,
        instanceId,
        threadId: input.threadId,
        runtimeMode: binding.runtimeMode,
        isActive: false,
      } as const;
    }

    // Recovery may synchronously expose a resumed/adopted adapter before its
    // persisted active-turn output marker is restored. Successful Cursor
    // completions use the same lock, so keep recovery serialized through the
    // marker write rather than letting the grace window race adapter startup.
    const recovered = yield* withThreadSessionLock(
      input.threadId,
      recoverSessionForThread({
        binding,
        operation: input.operation,
      }),
    );
    return {
      adapter: recovered.adapter,
      instanceId,
      threadId: input.threadId,
      runtimeMode: recovered.session.runtimeMode,
      isActive: true,
    } as const;
  });

  const stopStaleSessionsForThread = Effect.fn("stopStaleSessionsForThread")(function* (input: {
    readonly threadId: ThreadId;
    readonly currentInstanceId: ProviderInstanceId;
  }) {
    const currentAdapters = yield* getAdapterEntries;
    yield* Effect.forEach(
      currentAdapters,
      ([instanceId, adapter]) =>
        instanceId === input.currentInstanceId
          ? Effect.void
          : Effect.gen(function* () {
              const hasSession = yield* adapter.hasSession(input.threadId);
              if (!hasSession) {
                return;
              }

              yield* adapter.stopSession(input.threadId).pipe(
                Effect.tap(() =>
                  analytics.record("provider.session.stopped", {
                    provider: adapter.provider,
                  }),
                ),
                Effect.catchCause((cause) =>
                  Effect.logWarning("provider.session.stop-stale-failed", {
                    threadId: input.threadId,
                    provider: adapter.provider,
                    cause,
                  }),
                ),
              );
            }),
      { discard: true },
    );
  });

  const startSession: ProviderServiceMethod<"startSession"> = Effect.fn("startSession")(
    function* (threadId, rawInput) {
      const parsed = yield* decodeInputOrValidationError({
        operation: "ProviderService.startSession",
        schema: ProviderSessionStartInput,
        payload: rawInput,
      });

      const resolvedInstanceId = yield* requireBindingInstanceId(
        "ProviderService.startSession",
        parsed,
      );
      let metricProvider = parsed.provider ?? String(resolvedInstanceId);
      yield* Effect.annotateCurrentSpan({
        "provider.operation": "start-session",
        "provider.instance_id": resolvedInstanceId,
        "provider.thread_id": threadId,
        "provider.runtime_mode": parsed.runtimeMode,
      });
      return yield* withThreadSessionLock(
        threadId,
        Effect.gen(function* () {
          const instanceInfo = yield* registry.getInstanceInfo(resolvedInstanceId);
          const resolvedProvider = instanceInfo.driverKind;
          metricProvider = resolvedProvider;
          if (parsed.provider !== undefined && parsed.provider !== resolvedProvider) {
            return yield* toValidationError(
              "ProviderService.startSession",
              `Provider instance '${resolvedInstanceId}' belongs to driver '${resolvedProvider}', not '${parsed.provider}'.`,
            );
          }
          const input = {
            ...parsed,
            threadId,
            provider: resolvedProvider,
          };
          if (!instanceInfo.enabled) {
            return yield* toValidationError(
              "ProviderService.startSession",
              `Provider instance '${resolvedInstanceId}' is disabled in T3 Code settings.`,
            );
          }
          const persistedBinding = Option.getOrUndefined(yield* directory.getBinding(threadId));
          let persistedRuntimePayload = readRecord(persistedBinding?.runtimePayload) ?? {};
          let persistedBindingNeedsUpdate = false;
          if (
            typeof persistedRuntimePayload.sendTurnOperationId === "string" &&
            persistedRuntimePayload.sendTurnOperationId.length > 0 &&
            persistedBinding !== undefined
          ) {
            const activeOperationId = (yield* Ref.get(activeSendTurnOperations)).get(threadId);
            if (
              persistedBinding.status !== "error" &&
              persistedBinding.status !== "stopped" &&
              activeOperationId === persistedRuntimePayload.sendTurnOperationId
            ) {
              return yield* toValidationError(
                "ProviderService.startSession",
                `Cannot replace provider session for thread '${threadId}' while session/prompt is in flight. Retry after the prompt settles.`,
              );
            }
            // A persisted token without the matching in-process operation is
            // crash residue, not liveness evidence. Reconcile it before the
            // replacement starts so a restart cannot wedge recovery forever.
            persistedRuntimePayload = {
              ...persistedRuntimePayload,
              sendTurnOperationId: null,
            };
            persistedBindingNeedsUpdate = true;
          }
          let persistedSessionOwnershipId =
            typeof persistedRuntimePayload.sessionOwnershipId === "string"
              ? persistedRuntimePayload.sessionOwnershipId
              : undefined;
          if (persistedBinding !== undefined && persistedSessionOwnershipId === undefined) {
            persistedSessionOwnershipId = NodeCrypto.randomUUID();
            persistedRuntimePayload = {
              ...persistedRuntimePayload,
              sessionOwnershipId: persistedSessionOwnershipId,
            };
            persistedBindingNeedsUpdate = true;
          }
          if (persistedBinding !== undefined && persistedBindingNeedsUpdate) {
            yield* directory.upsert({
              threadId,
              provider: persistedBinding.provider,
              ...(persistedBinding.providerInstanceId !== undefined
                ? { providerInstanceId: persistedBinding.providerInstanceId }
                : {}),
              runtimeMode: persistedBinding.runtimeMode ?? "full-access",
              status: persistedBinding.status ?? "running",
              ...(persistedBinding.resumeCursor !== undefined
                ? { resumeCursor: persistedBinding.resumeCursor }
                : {}),
              runtimePayload: persistedRuntimePayload,
            });
          }
          const effectiveResumeCursor =
            input.resumeCursor ??
            (persistedBinding?.providerInstanceId === resolvedInstanceId
              ? persistedBinding.resumeCursor
              : undefined);
          const effectiveCwd =
            input.cwd ??
            (persistedBinding?.providerInstanceId === resolvedInstanceId
              ? readPersistedCwd(persistedBinding.runtimePayload)
              : undefined);
          const effectiveDetached =
            input.detached ??
            (persistedBinding?.providerInstanceId === resolvedInstanceId
              ? readPersistedDetached(persistedBinding.runtimePayload)
              : undefined);
          const usesPersistedResumeCursor =
            effectiveResumeCursor !== undefined &&
            persistedBinding?.providerInstanceId === resolvedInstanceId &&
            persistedBinding.resumeCursor !== null &&
            persistedBinding.resumeCursor !== undefined &&
            resumeCursorEquals(effectiveResumeCursor, persistedBinding.resumeCursor);
          const persistedActiveTurnId =
            supportsActiveTurnResume(resolvedProvider) &&
            usesPersistedResumeCursor &&
            persistedBinding !== undefined
              ? readPersistedActiveTurnId(persistedBinding.runtimePayload)
              : undefined;
          const effectiveActiveTurnId = supportsActiveTurnResume(resolvedProvider)
            ? (input.activeTurnId ?? persistedActiveTurnId)
            : undefined;
          yield* Effect.annotateCurrentSpan({
            "provider.kind": resolvedProvider,
            "provider.resume_cursor.source":
              input.resumeCursor !== undefined
                ? "request"
                : effectiveResumeCursor !== undefined &&
                    persistedBinding?.providerInstanceId === resolvedInstanceId
                  ? "persisted"
                  : "none",
            "provider.resume_cursor.present": effectiveResumeCursor !== undefined,
            "provider.cwd.source":
              input.cwd !== undefined
                ? "request"
                : effectiveCwd !== undefined &&
                    persistedBinding?.providerInstanceId === resolvedInstanceId
                  ? "persisted"
                  : "none",
            "provider.cwd.effective": effectiveCwd ?? "",
            "provider.session.detached": effectiveDetached === true,
          });
          const adapter = yield* registry.getByInstance(resolvedInstanceId);
          const adapterGeneration = yield* getAdapterGenerationForStart(
            resolvedInstanceId,
            adapter,
          );
          if (adapterGeneration === 0) {
            return yield* toValidationError(
              "ProviderService.startSession",
              `Provider instance '${resolvedInstanceId}' was replaced before starting thread '${threadId}'. Retry the start.`,
            );
          }
          const replacesLiveAdapterSession = yield* adapter.hasSession(threadId);
          yield* beginMcpSessionStart(threadId);
          let preparedMcpSession: McpProviderSession.McpProviderSessionConfig | undefined;
          const session = yield* Effect.gen(function* () {
            preparedMcpSession = yield* prepareMcpSession(threadId, resolvedInstanceId);
            return yield* startAdapterSessionWithTimeout({
              adapter,
              request: {
                ...input,
                providerInstanceId: resolvedInstanceId,
                ...(effectiveCwd !== undefined ? { cwd: effectiveCwd } : {}),
                ...(effectiveResumeCursor !== undefined
                  ? { resumeCursor: effectiveResumeCursor }
                  : {}),
                ...(effectiveDetached !== undefined ? { detached: effectiveDetached } : {}),
                ...(effectiveActiveTurnId !== undefined
                  ? { activeTurnId: effectiveActiveTurnId }
                  : {}),
              },
              ...(persistedSessionOwnershipId !== undefined
                ? { sessionOwnershipId: persistedSessionOwnershipId }
                : {}),
            });
          }).pipe(
            Effect.onError(() =>
              clearPreparedMcpSession(threadId, resolvedInstanceId, preparedMcpSession),
            ),
            Effect.ensuring(endMcpSessionStart(threadId)),
          );

          if (session.provider !== adapter.provider) {
            yield* clearPreparedMcpSession(threadId, resolvedInstanceId, preparedMcpSession);
            return yield* toValidationError(
              "ProviderService.startSession",
              `Adapter/provider mismatch: requested '${adapter.provider}', received '${session.provider}'.`,
            );
          }
          if (!(yield* isCurrentAdapterGeneration(resolvedInstanceId, adapterGeneration))) {
            yield* stopSupersededAdapterSession(adapter, threadId, resolvedInstanceId);
            yield* clearPreparedMcpSession(threadId, resolvedInstanceId, preparedMcpSession);
            return yield* toValidationError(
              "ProviderService.startSession",
              `Provider instance '${resolvedInstanceId}' was replaced while starting thread '${threadId}'. Retry the start.`,
            );
          }
          yield* trackMcpSession(threadId, resolvedInstanceId, adapterGeneration);
          const sessionWithInstance = withActiveTurnFallback(
            {
              ...session,
              providerInstanceId: resolvedInstanceId,
            },
            effectiveActiveTurnId,
          );

          yield* stopStaleSessionsForThread({
            threadId,
            currentInstanceId: resolvedInstanceId,
          });
          yield* upsertSessionBinding(sessionWithInstance, threadId, {
            modelSelection: input.modelSelection,
            ...(effectiveDetached !== undefined ? { detached: effectiveDetached } : {}),
            ...(preparedMcpSession !== undefined
              ? { mcpProviderSessionId: preparedMcpSession.providerSessionId }
              : {}),
          });
          if (effectiveActiveTurnId !== undefined && !replacesLiveAdapterSession) {
            preserveRecoveredActiveTurnOutput(
              adapter,
              resolvedInstanceId,
              threadId,
              effectiveActiveTurnId,
            );
          }
          yield* analytics.record("provider.session.started", {
            provider: sessionWithInstance.provider,
            runtimeMode: input.runtimeMode,
            hasResumeCursor: sessionWithInstance.resumeCursor !== undefined,
            hasCwd: typeof effectiveCwd === "string" && effectiveCwd.trim().length > 0,
            hasModel:
              typeof input.modelSelection?.model === "string" &&
              input.modelSelection.model.trim().length > 0,
          });

          // Changing runtime mode restarts the session, so the transition is only
          // observable here, by diffing against the mode the previous session for
          // this thread was bound to. Recording it separately is what makes the
          // "started supervised, switched to full access" funnel answerable.
          const previousRuntimeMode = persistedBinding?.runtimeMode;
          if (previousRuntimeMode !== undefined && previousRuntimeMode !== input.runtimeMode) {
            yield* analytics.record("provider.runtime_mode.changed", {
              provider: sessionWithInstance.provider,
              from: previousRuntimeMode,
              to: input.runtimeMode,
            });
          }

          return sessionWithInstance;
        }).pipe(
          withMetrics({
            counter: providerSessionsTotal,
            attributes: () =>
              providerMetricAttributes(metricProvider, {
                operation: "start",
              }),
          }),
        ),
      );
    },
  );

  const sendTurn: ProviderServiceMethod<"sendTurn"> = Effect.fn("sendTurn")(function* (rawInput) {
    const parsed = yield* decodeInputOrValidationError({
      operation: "ProviderService.sendTurn",
      schema: ProviderSendTurnInput,
      payload: rawInput,
    });

    const attachments = parsed.attachments ?? [];
    if (!parsed.input && attachments.length === 0) {
      return yield* toValidationError(
        "ProviderService.sendTurn",
        "Either input text or at least one attachment is required",
      );
    }

    // Adapters inline attachment pixels into the model prompt, but the model's
    // tools cannot dereference pixels. Appending the on-disk path is what lets
    // a turn like "include this screenshot in the PR" copy the actual file.
    // This runs after schema decode, so the appended lines are exempt from the
    // PROVIDER_SEND_TURN_MAX_INPUT_CHARS check; attachment count is capped, so
    // the overhead is bounded. Unresolvable ids are skipped here and surface
    // as adapter errors when the file is read for inlining.
    const attachmentPathLines = attachments.flatMap((attachment) => {
      const attachmentPath = resolveAttachmentPath({
        attachmentsDir: serverConfig.attachmentsDir,
        attachment,
      });
      return attachmentPath === null
        ? []
        : [`[Attached ${attachment.type} "${attachment.name}" is saved at: ${attachmentPath}]`];
    });
    const inputTextWithAttachmentPaths =
      attachmentPathLines.length === 0
        ? parsed.input
        : [parsed.input, attachmentPathLines.join("\n")]
            .filter((part): part is string => typeof part === "string" && part.length > 0)
            .join("\n\n");

    const input = {
      ...parsed,
      ...(inputTextWithAttachmentPaths !== undefined
        ? { input: inputTextWithAttachmentPaths }
        : {}),
      attachments,
    };
    yield* Effect.annotateCurrentSpan({
      "provider.operation": "send-turn",
      "provider.thread_id": input.threadId,
      "provider.interaction_mode": input.interactionMode,
      "provider.attachment_count": input.attachments.length,
    });
    let metricProvider = "unknown";
    let metricModel = input.modelSelection?.model;
    return yield* Effect.gen(function* () {
      const routed = yield* resolveRoutableSession({
        threadId: input.threadId,
        operation: "ProviderService.sendTurn",
        allowRecovery: true,
      });
      metricProvider = routed.adapter.provider;
      metricModel = input.modelSelection?.model;
      yield* Effect.annotateCurrentSpan({
        "provider.kind": routed.adapter.provider,
        ...(input.modelSelection?.model ? { "provider.model": input.modelSelection.model } : {}),
      });
      // A turn is the clearest sign a session is still alive. The MCP
      // credential is minted once at session start and cannot be rotated into
      // an already-spawned agent process, so we keep the existing token valid
      // rather than issuing a new one: sessions that go a long time between
      // browser tool calls used to lose the toolkit outright.
      yield* McpSessionRegistry.touchActiveMcpThread(input.threadId);
      const sendTurnOwnership = yield* registerSendTurnOperation({
        source: {
          provider: routed.adapter.provider,
          instanceId: routed.instanceId,
        },
        threadId: input.threadId,
      });
      const turn = yield* Effect.gen(function* () {
        const acceptedTurn = yield* routed.adapter.sendTurn(input).pipe(
          Effect.catch((error) =>
            clearFailedSendTurnRuntimeState(
              {
                provider: routed.adapter.provider,
                instanceId: routed.instanceId,
              },
              input.threadId,
              sendTurnOwnership,
              error.message,
            ).pipe(
              Effect.catchCause((cause) =>
                Effect.logWarning("provider.session.send-turn-failure-cleanup-failed", {
                  threadId: input.threadId,
                  provider: routed.adapter.provider,
                  cause,
                }).pipe(Effect.as({ superseded: false } as const)),
              ),
              Effect.flatMap((failure) =>
                Effect.fail(
                  new ProviderSendTurnFailedError({
                    provider: routed.adapter.provider,
                    threadId: input.threadId,
                    detail: error.message,
                    sessionOwnershipId: sendTurnOwnership.sessionOwnershipId,
                    sendTurnOperationId: sendTurnOwnership.operationId,
                    ...("turnId" in failure && failure.turnId !== undefined
                      ? { turnId: failure.turnId }
                      : {}),
                    ...("preservedActiveTurnId" in failure &&
                    failure.preservedActiveTurnId !== undefined
                      ? { preservedActiveTurnId: failure.preservedActiveTurnId }
                      : {}),
                    superseded: failure.superseded,
                    cause: error,
                  }),
                ),
              ),
            ),
          ),
        );
        const sendTurnResultPersisted = yield* withThreadSessionLock(
          input.threadId,
          Effect.gen(function* () {
            const binding = Option.getOrUndefined(yield* directory.getBinding(input.threadId));
            const persistedPayload = readRecord(binding?.runtimePayload) ?? {};
            if (
              binding === undefined ||
              binding.provider !== routed.adapter.provider ||
              binding.providerInstanceId !== routed.instanceId ||
              persistedPayload.sessionOwnershipId !== sendTurnOwnership.sessionOwnershipId
            ) {
              return { _tag: "superseded" } as const;
            }
            if (binding.status === "error" || binding.status === "stopped") {
              // A correlated session exit or failed cleanup won while the
              // adapter request was in flight. Retire only this operation token;
              // never resurrect the dead session as running on late success.
              yield* directory.upsert({
                threadId: input.threadId,
                provider: binding.provider,
                providerInstanceId: routed.instanceId,
                runtimeMode: binding.runtimeMode ?? "full-access",
                status: binding.status,
                ...(binding.resumeCursor !== undefined
                  ? { resumeCursor: binding.resumeCursor }
                  : {}),
                runtimePayload: {
                  ...persistedPayload,
                  sendTurnOperationId: null,
                  lastFailedSendTurnOperationId: sendTurnOwnership.operationId,
                },
              });
              return {
                _tag: "terminal",
                detail:
                  typeof persistedPayload.lastError === "string"
                    ? persistedPayload.lastError
                    : "Provider session became terminal before session/prompt returned.",
              } as const;
            }
            if (persistedPayload.sendTurnOperationId !== sendTurnOwnership.operationId) {
              return { _tag: "superseded" } as const;
            }
            const lastTerminalTurnId = persistedPayload.lastTerminalTurnId;
            const turnAlreadyTerminated =
              lastTerminalTurnId !== null &&
              lastTerminalTurnId !== undefined &&
              String(lastTerminalTurnId) === String(acceptedTurn.turnId);
            const runtimePayloadPatch: Record<string, unknown> = {
              ...(input.modelSelection !== undefined
                ? { modelSelection: input.modelSelection }
                : {}),
              activeTurnId: turnAlreadyTerminated ? null : acceptedTurn.turnId,
              sendTurnOperationId: null,
            };
            if (!turnAlreadyTerminated) {
              runtimePayloadPatch.lastRuntimeEvent = "provider.sendTurn";
              runtimePayloadPatch.lastRuntimeEventAt = yield* nowIso;
            }
            yield* directory.upsert({
              threadId: input.threadId,
              provider: routed.adapter.provider,
              providerInstanceId: routed.instanceId,
              status: binding.status === "waiting" ? "waiting" : "running",
              ...(acceptedTurn.resumeCursor !== undefined
                ? { resumeCursor: acceptedTurn.resumeCursor }
                : {}),
              runtimePayload: mergeRuntimePayload(binding.runtimePayload, runtimePayloadPatch),
            });
            return { _tag: "persisted" } as const;
          }),
        );
        if (sendTurnResultPersisted._tag === "terminal") {
          return yield* new ProviderSendTurnFailedError({
            provider: routed.adapter.provider,
            threadId: input.threadId,
            detail: sendTurnResultPersisted.detail,
            sessionOwnershipId: sendTurnOwnership.sessionOwnershipId,
            sendTurnOperationId: sendTurnOwnership.operationId,
            turnId: acceptedTurn.turnId,
            superseded: false,
          });
        }
        if (sendTurnResultPersisted._tag === "superseded") {
          return yield* new ProviderSendTurnFailedError({
            provider: routed.adapter.provider,
            threadId: input.threadId,
            detail: "Provider session ownership changed after session/prompt was accepted.",
            sessionOwnershipId: sendTurnOwnership.sessionOwnershipId,
            sendTurnOperationId: sendTurnOwnership.operationId,
            turnId: acceptedTurn.turnId,
            superseded: true,
          });
        }
        return acceptedTurn;
      }).pipe(
        Effect.ensuring(releaseSendTurnOperation(input.threadId, sendTurnOwnership.operationId)),
      );
      yield* analytics.record("provider.turn.sent", {
        provider: routed.adapter.provider,
        model: input.modelSelection?.model,
        interactionMode: input.interactionMode,
        // Session-start events alone skew runtime mode toward users who toggle
        // often, since every toggle restarts the session. Recording it per turn
        // gives a usage-weighted view and lets it cross with interactionMode.
        runtimeMode: routed.runtimeMode,
        attachmentCount: input.attachments.length,
        hasInput: typeof input.input === "string" && input.input.trim().length > 0,
      });
      return turn;
    }).pipe(
      withMetrics({
        counter: providerTurnsTotal,
        timer: providerTurnDuration,
        attributes: () =>
          providerTurnMetricAttributes({
            provider: metricProvider,
            model: metricModel,
            extra: {
              operation: "send",
            },
          }),
      }),
    );
  });

  const interruptTurn: ProviderServiceMethod<"interruptTurn"> = Effect.fn("interruptTurn")(
    function* (rawInput) {
      const input = yield* decodeInputOrValidationError({
        operation: "ProviderService.interruptTurn",
        schema: ProviderInterruptTurnInput,
        payload: rawInput,
      });
      let metricProvider = "unknown";
      return yield* Effect.gen(function* () {
        const routed = yield* resolveRoutableSession({
          threadId: input.threadId,
          operation: "ProviderService.interruptTurn",
          allowRecovery: true,
        });
        metricProvider = routed.adapter.provider;
        yield* Effect.annotateCurrentSpan({
          "provider.operation": "interrupt-turn",
          "provider.kind": routed.adapter.provider,
          "provider.thread_id": input.threadId,
          "provider.turn_id": input.turnId,
        });
        yield* routed.adapter.interruptTurn(routed.threadId, input.turnId);
        yield* analytics.record("provider.turn.interrupted", {
          provider: routed.adapter.provider,
        });
      }).pipe(
        withMetrics({
          counter: providerTurnsTotal,
          outcomeAttributes: () =>
            providerMetricAttributes(metricProvider, {
              operation: "interrupt",
            }),
        }),
      );
    },
  );

  const respondToRequest: ProviderServiceMethod<"respondToRequest"> = Effect.fn("respondToRequest")(
    function* (rawInput) {
      const input = yield* decodeInputOrValidationError({
        operation: "ProviderService.respondToRequest",
        schema: ProviderRespondToRequestInput,
        payload: rawInput,
      });
      let metricProvider = "unknown";
      return yield* Effect.gen(function* () {
        const routed = yield* resolveRoutableSession({
          threadId: input.threadId,
          operation: "ProviderService.respondToRequest",
          allowRecovery: true,
        });
        metricProvider = routed.adapter.provider;
        yield* Effect.annotateCurrentSpan({
          "provider.operation": "respond-to-request",
          "provider.kind": routed.adapter.provider,
          "provider.thread_id": input.threadId,
          "provider.request_id": input.requestId,
        });
        yield* routed.adapter.respondToRequest(routed.threadId, input.requestId, input.decision);
        yield* analytics.record("provider.request.responded", {
          provider: routed.adapter.provider,
          decision: input.decision,
        });
      }).pipe(
        withMetrics({
          counter: providerTurnsTotal,
          outcomeAttributes: () =>
            providerMetricAttributes(metricProvider, {
              operation: "approval-response",
            }),
        }),
      );
    },
  );

  const respondToUserInput: ProviderServiceMethod<"respondToUserInput"> = Effect.fn(
    "respondToUserInput",
  )(function* (rawInput) {
    const input = yield* decodeInputOrValidationError({
      operation: "ProviderService.respondToUserInput",
      schema: ProviderRespondToUserInputInput,
      payload: rawInput,
    });
    let metricProvider = "unknown";
    return yield* Effect.gen(function* () {
      const routed = yield* resolveRoutableSession({
        threadId: input.threadId,
        operation: "ProviderService.respondToUserInput",
        allowRecovery: true,
      });
      metricProvider = routed.adapter.provider;
      yield* Effect.annotateCurrentSpan({
        "provider.operation": "respond-to-user-input",
        "provider.kind": routed.adapter.provider,
        "provider.thread_id": input.threadId,
        "provider.request_id": input.requestId,
      });
      yield* routed.adapter.respondToUserInput(routed.threadId, input.requestId, input.answers);
    }).pipe(
      withMetrics({
        counter: providerTurnsTotal,
        outcomeAttributes: () =>
          providerMetricAttributes(metricProvider, {
            operation: "user-input-response",
          }),
      }),
    );
  });

  const stopSession: ProviderServiceMethod<"stopSession"> = Effect.fn("stopSession")(
    function* (rawInput) {
      const input = yield* decodeInputOrValidationError({
        operation: "ProviderService.stopSession",
        schema: ProviderStopSessionInput,
        payload: rawInput,
      });
      let metricProvider = "unknown";
      return yield* Effect.gen(function* () {
        const routed = yield* resolveRoutableSession({
          threadId: input.threadId,
          operation: "ProviderService.stopSession",
          allowRecovery: false,
        });
        metricProvider = routed.adapter.provider;
        yield* Effect.annotateCurrentSpan({
          "provider.operation": "stop-session",
          "provider.kind": routed.adapter.provider,
          "provider.thread_id": input.threadId,
        });
        if (routed.isActive) {
          yield* routed.adapter.stopSession(routed.threadId);
        }
        yield* clearMcpSession(input.threadId);
        const binding = Option.getOrUndefined(
          yield* directory
            .getBinding(input.threadId)
            .pipe(
              Effect.orElseSucceed(() =>
                Option.none<ProviderSessionDirectory.ProviderRuntimeBinding>(),
              ),
            ),
        );
        yield* directory.upsert({
          threadId: input.threadId,
          provider: routed.adapter.provider,
          providerInstanceId: routed.instanceId,
          status: "stopped",
          runtimePayload: mergeRuntimePayload(binding?.runtimePayload, {
            activeTurnId: null,
            sendTurnOperationId: null,
            activeTurnSendTurnOperationId: null,
          }),
        });
        yield* analytics.record("provider.session.stopped", {
          provider: routed.adapter.provider,
        });
      }).pipe(
        withMetrics({
          counter: providerSessionsTotal,
          outcomeAttributes: () =>
            providerMetricAttributes(metricProvider, {
              operation: "stop",
            }),
        }),
      );
    },
  );

  const stopInactiveSession: NonNullable<
    ProviderService.ProviderServiceShape["stopInactiveSession"]
  > = Effect.fn("ProviderService.stopInactiveSession")(function* (input) {
    return yield* withThreadSessionLock(
      input.threadId,
      Effect.gen(function* () {
        const latestBinding = Option.getOrUndefined(yield* directory.getBinding(input.threadId));
        if (!inactiveBindingMatchesSnapshot(input.expectedBinding, latestBinding)) {
          return false;
        }
        yield* stopSession({ threadId: input.threadId });
        return true;
      }),
    );
  });

  const forceFailStaleSession: NonNullable<
    ProviderService.ProviderServiceShape["forceFailStaleSession"]
  > = Effect.fn("ProviderService.forceFailStaleSession")(function* (input) {
    return yield* withThreadSessionLock(
      input.threadId,
      Effect.gen(function* () {
        const latestBinding = Option.getOrUndefined(yield* directory.getBinding(input.threadId));
        if (
          !terminalBindingMatchesSnapshot({
            expected: input.expectedBinding,
            latest: latestBinding,
            turnId: input.turnId,
          })
        ) {
          return false;
        }
        yield* input.onOwned;
        yield* input.onSettled;
        return true;
      }),
    );
  });

  const stopFailedSession: ProviderService.ProviderServiceShape["stopFailedSession"] = Effect.fn(
    "ProviderService.stopFailedSession",
  )(function* (input) {
    return yield* withThreadSessionLock(
      input.threadId,
      Effect.gen(function* () {
        const binding = Option.getOrUndefined(yield* directory.getBinding(input.threadId));
        if (binding === undefined) {
          if (input.sessionOwnershipId !== undefined || input.requireSessionAbsent !== true) {
            return false;
          }
          // There is no runtime owner to clean up. Keep the replacement-start
          // lock until the terminal projection is committed so a new session
          // cannot be overwritten by this stale failure handler.
          yield* input.onOwned;
          yield* input.onStopped;
          return true;
        }
        const runtimePayload = readRecord(binding.runtimePayload) ?? {};
        const activeTurnId = readPersistedActiveTurnId(runtimePayload);
        const lastTerminalTurnId = runtimePayload.lastTerminalTurnId;
        const sessionOwnershipId = runtimePayload.sessionOwnershipId;
        const currentSendTurnOperationId = runtimePayload.sendTurnOperationId;
        const lastFailedSendTurnOperationId = runtimePayload.lastFailedSendTurnOperationId;
        const operationStillOwnsFailure =
          input.sendTurnOperationId !== undefined &&
          input.sessionOwnershipId !== undefined &&
          sessionOwnershipId === input.sessionOwnershipId &&
          lastFailedSendTurnOperationId === input.sendTurnOperationId &&
          (currentSendTurnOperationId === null || currentSendTurnOperationId === undefined) &&
          (activeTurnId === undefined || activeTurnId === input.turnId);
        const turnStillBelongsToFailure =
          activeTurnId === input.turnId ||
          (lastTerminalTurnId !== null &&
            lastTerminalTurnId !== undefined &&
            String(lastTerminalTurnId) === String(input.turnId));
        const identityStillOwnsFailure =
          input.sessionOwnershipId !== undefined &&
          sessionOwnershipId === input.sessionOwnershipId &&
          turnStillBelongsToFailure;
        const legacyActiveTurnStillOwnsFailure =
          input.allowLegacyActiveTurnMatch === true &&
          input.sessionOwnershipId === undefined &&
          typeof sessionOwnershipId !== "string" &&
          activeTurnId === input.turnId;
        const sessionAbsence =
          input.requireSessionAbsent === true && binding.providerInstanceId !== undefined
            ? yield* registry.getByInstance(binding.providerInstanceId).pipe(
                Effect.flatMap((adapter) =>
                  providerSessionIsProvablyGone({
                    adapter,
                    provider: binding.provider,
                    threadId: input.threadId,
                  }).pipe(
                    Effect.map((sessionIsProvablyAbsent) => ({
                      providerInstanceIsAbsent: false,
                      sessionIsProvablyAbsent,
                    })),
                  ),
                ),
                Effect.catchTag("ProviderUnsupportedError", () =>
                  Effect.succeed({
                    providerInstanceIsAbsent: true,
                    sessionIsProvablyAbsent: true,
                  }),
                ),
              )
            : {
                providerInstanceIsAbsent: false,
                sessionIsProvablyAbsent: false,
              };
        const bindingStillBelongsToFailedTurn =
          operationStillOwnsFailure ||
          (input.sendTurnOperationId === undefined &&
            (identityStillOwnsFailure ||
              legacyActiveTurnStillOwnsFailure ||
              (sessionAbsence.sessionIsProvablyAbsent && turnStillBelongsToFailure)));
        if (!bindingStillBelongsToFailedTurn) {
          return false;
        }
        if (binding.status === "stopped") {
          yield* input.onOwned;
          yield* input.onStopped;
          return true;
        }
        const failedAt = yield* nowIso;
        yield* directory.upsert({
          threadId: input.threadId,
          provider: binding.provider,
          ...(binding.providerInstanceId !== undefined
            ? { providerInstanceId: binding.providerInstanceId }
            : {}),
          runtimeMode: binding.runtimeMode ?? "full-access",
          status: sessionAbsence.providerInstanceIsAbsent ? "stopped" : "error",
          ...(binding.resumeCursor !== undefined ? { resumeCursor: binding.resumeCursor } : {}),
          runtimePayload: mergeRuntimePayload(binding.runtimePayload, {
            activeTurnId: null,
            sendTurnOperationId: null,
            activeTurnSendTurnOperationId: null,
            lastTerminalTurnId: input.turnId,
            lastError: input.reason,
            lastRuntimeEvent: sessionAbsence.providerInstanceIsAbsent
              ? "provider.turn.watchdog.instance-absent"
              : "provider.turn.watchdog.stop-pending",
            lastRuntimeEventAt: failedAt,
          }),
        });
        // Projection and lease settlement must happen under the same lock as
        // the binding ownership check. Otherwise a replacement can start in
        // the gap and be overwritten by this terminal event.
        yield* input.onOwned;
        if (sessionAbsence.providerInstanceIsAbsent) {
          yield* clearMcpSession(input.threadId);
        } else {
          yield* stopSession({ threadId: input.threadId });
        }
        yield* input.onStopped;
        return true;
      }),
    );
  });

  const listSessions: ProviderServiceMethod<"listSessions"> = Effect.fn("listSessions")(
    function* () {
      const currentAdapters = yield* getAdapterEntries;
      const sessionsByProvider = yield* Effect.forEach(currentAdapters, ([instanceId, adapter]) =>
        adapter.listSessions().pipe(
          Effect.map((sessions) =>
            sessions.map((session) => ({
              ...session,
              providerInstanceId: instanceId,
            })),
          ),
        ),
      );
      const activeSessions = sessionsByProvider.flatMap((sessions) => sessions);
      const persistedBindings = yield* directory.listThreadIds().pipe(
        Effect.flatMap((threadIds) =>
          Effect.forEach(
            threadIds,
            (threadId) =>
              directory
                .getBinding(threadId)
                .pipe(
                  Effect.orElseSucceed(() =>
                    Option.none<ProviderSessionDirectory.ProviderRuntimeBinding>(),
                  ),
                ),
            { concurrency: "unbounded" },
          ),
        ),
        Effect.orElseSucceed(
          () => [] as Array<Option.Option<ProviderSessionDirectory.ProviderRuntimeBinding>>,
        ),
      );
      const bindingsByThreadId = new Map<
        ThreadId,
        ProviderSessionDirectory.ProviderRuntimeBinding
      >();
      for (const bindingOption of persistedBindings) {
        const binding = Option.getOrUndefined(bindingOption);
        if (binding) {
          bindingsByThreadId.set(binding.threadId, binding);
        }
      }

      const sessions: ProviderSession[] = [];
      for (const session of activeSessions) {
        const binding = bindingsByThreadId.get(session.threadId);
        if (!binding) {
          sessions.push(session);
          continue;
        }

        const overrides: {
          resumeCursor?: ProviderSession["resumeCursor"];
          runtimeMode?: ProviderSession["runtimeMode"];
          providerInstanceId?: ProviderSession["providerInstanceId"];
          activeTurnId?: ProviderSession["activeTurnId"];
        } = {};
        overrides.providerInstanceId = dieOnMissingBindingInstanceId(
          "ProviderService.listSessions",
          binding,
        );
        if (binding.provider !== session.provider) {
          return yield* Effect.die(
            new Error(
              `ProviderService.listSessions: thread '${session.threadId}' is active on provider '${session.provider}' but persisted binding names provider '${binding.provider}'.`,
            ),
          );
        }
        if (overrides.providerInstanceId !== session.providerInstanceId) {
          return yield* Effect.die(
            new Error(
              `ProviderService.listSessions: thread '${session.threadId}' is active on provider instance '${session.providerInstanceId}' but persisted binding names '${overrides.providerInstanceId}'.`,
            ),
          );
        }
        if (session.resumeCursor === undefined && binding.resumeCursor !== undefined) {
          overrides.resumeCursor = binding.resumeCursor;
        }
        if (binding.runtimeMode !== undefined) {
          overrides.runtimeMode = binding.runtimeMode;
        }
        sessions.push(withActiveTurnFallback(Object.assign({}, session, overrides), undefined));
      }
      return sessions;
    },
  );

  const getCapabilities: ProviderServiceMethod<"getCapabilities"> = (instanceId) =>
    registry.getByInstance(instanceId).pipe(Effect.map((adapter) => adapter.capabilities));

  const getInstanceInfo: ProviderServiceMethod<"getInstanceInfo"> = (instanceId) =>
    registry.getInstanceInfo(instanceId);

  const rollbackConversation: ProviderServiceMethod<"rollbackConversation"> = Effect.fn(
    "rollbackConversation",
  )(function* (rawInput) {
    const input = yield* decodeInputOrValidationError({
      operation: "ProviderService.rollbackConversation",
      schema: ProviderRollbackConversationInput,
      payload: rawInput,
    });
    if (input.numTurns === 0) {
      return;
    }
    let metricProvider = "unknown";
    return yield* Effect.gen(function* () {
      const routed = yield* resolveRoutableSession({
        threadId: input.threadId,
        operation: "ProviderService.rollbackConversation",
        allowRecovery: true,
      });
      metricProvider = routed.adapter.provider;
      yield* Effect.annotateCurrentSpan({
        "provider.operation": "rollback-conversation",
        "provider.kind": routed.adapter.provider,
        "provider.thread_id": input.threadId,
        "provider.rollback_turns": input.numTurns,
      });
      yield* routed.adapter.rollbackThread(routed.threadId, input.numTurns);
      yield* analytics.record("provider.conversation.rolled_back", {
        provider: routed.adapter.provider,
        turns: input.numTurns,
      });
    }).pipe(
      withMetrics({
        counter: providerTurnsTotal,
        outcomeAttributes: () =>
          providerMetricAttributes(metricProvider, {
            operation: "rollback",
          }),
      }),
    );
  });

  const runStopAll = Effect.fn("runStopAll")(function* () {
    const threadIds = yield* directory.listThreadIds();
    const currentAdapters = yield* getAdapterEntries;
    const activeSessions = yield* Effect.forEach(currentAdapters, ([instanceId, adapter]) =>
      adapter.listSessions().pipe(
        Effect.map((sessions) =>
          sessions.map((session) => ({
            ...session,
            providerInstanceId: instanceId,
          })),
        ),
      ),
    ).pipe(Effect.map((sessionsByAdapter) => sessionsByAdapter.flatMap((sessions) => sessions)));
    yield* Effect.forEach(activeSessions, (session) =>
      Effect.flatMap(nowIso, (lastRuntimeEventAt) =>
        upsertSessionBinding(session, session.threadId, {
          lastRuntimeEvent: "provider.stopAll",
          lastRuntimeEventAt,
        }),
      ),
    ).pipe(Effect.asVoid);
    yield* Effect.forEach(currentAdapters, ([, adapter]) => adapter.stopAll()).pipe(Effect.asVoid);
    yield* McpSessionRegistry.revokeAllActiveMcpProviderCredentials();
    McpProviderSession.clearAllMcpProviderSessions();
    yield* Ref.set(trackedMcpSessions, new Map());
    const bindings = yield* directory.listBindings().pipe(Effect.orElseSucceed(() => []));
    yield* Effect.forEach(bindings, (binding) =>
      Effect.gen(function* () {
        const providerInstanceId = dieOnMissingBindingInstanceId(
          "ProviderService.stopAll",
          binding,
        );
        return yield* directory.upsert({
          threadId: binding.threadId,
          provider: binding.provider,
          providerInstanceId,
          status: "stopped",
          runtimePayload: mergeRuntimePayload(binding.runtimePayload, {
            activeTurnId: null,
            activeTurnSendTurnOperationId: null,
            lastRuntimeEvent: "provider.stopAll",
            lastRuntimeEventAt: yield* nowIso,
          }),
        });
      }),
    ).pipe(Effect.asVoid);
    yield* analytics.record("provider.sessions.stopped_all", {
      sessionCount: threadIds.length,
    });
    yield* analytics.flush;
  });

  yield* Effect.addFinalizer(() =>
    runStopAll().pipe(
      Effect.catchCause((cause) =>
        Effect.logWarning("failed to stop provider service", {
          errorTag: causeErrorTag(cause),
        }),
      ),
    ),
  );

  return {
    startSession,
    sendTurn,
    interruptTurn,
    respondToRequest,
    respondToUserInput,
    stopSession,
    stopInactiveSession,
    forceFailStaleSession,
    stopFailedSession,
    listSessions,
    getCapabilities,
    getInstanceInfo,
    rollbackConversation,
    // Each access creates a fresh PubSub subscription so that multiple
    // consumers (ProviderRuntimeIngestion, CheckpointReactor, etc.) each
    // independently receive all runtime events.
    get streamEvents(): ProviderServiceMethod<"streamEvents"> {
      return Stream.fromPubSub(runtimeEventPubSub);
    },
  } satisfies ProviderService.ProviderService["Service"];
});

export const ProviderServiceLive = Layer.effect(
  ProviderService.ProviderService,
  makeProviderService(),
);

export function makeProviderServiceLive(options?: ProviderServiceLiveOptions) {
  return Layer.effect(ProviderService.ProviderService, makeProviderService(options));
}
