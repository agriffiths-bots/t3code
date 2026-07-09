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
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as SchemaIssue from "effect/SchemaIssue";
import * as Stream from "effect/Stream";

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
import { type ProviderAdapterError, ProviderValidationError } from "../Errors.ts";
import type { ProviderAdapterShape } from "../Services/ProviderAdapter.ts";
import * as ProviderAdapterRegistry from "../Services/ProviderAdapterRegistry.ts";
import * as ProviderService from "../Services/ProviderService.ts";
import * as ProviderSessionDirectory from "../Services/ProviderSessionDirectory.ts";
import { type EventNdjsonLogger } from "./EventNdjsonLogger.ts";
import * as ProviderEventLoggers from "./ProviderEventLoggers.ts";
import * as AnalyticsService from "../../telemetry/AnalyticsService.ts";
import * as McpProviderSession from "../../mcp/McpProviderSession.ts";
import * as McpSessionRegistry from "../../mcp/McpSessionRegistry.ts";
const isModelSelection = Schema.is(ModelSelection);
const isTurnId = Schema.is(TurnId);

const supportsActiveTurnResume = (provider: ProviderDriverKind): boolean => provider === "cursor";

/**
 * Hook for tests that want to override the canonical event logger pulled
 * from `ProviderEventLoggers`. Production wiring leaves this undefined and
 * reads the logger off the tag.
 */
export interface ProviderServiceLiveOptions {
  readonly canonicalEventLogger?: EventNdjsonLogger;
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
    readonly lastRuntimeEvent?: string;
    readonly lastRuntimeEventAt?: string;
  },
): Record<string, unknown> {
  return {
    cwd: session.cwd ?? null,
    model: session.model ?? null,
    activeTurnId: session.activeTurnId ?? null,
    lastError: session.lastError ?? null,
    ...(extra?.modelSelection !== undefined ? { modelSelection: extra.modelSelection } : {}),
    ...(extra?.detached !== undefined ? { detached: extra.detached } : {}),
    ...(extra?.lastRuntimeEvent !== undefined ? { lastRuntimeEvent: extra.lastRuntimeEvent } : {}),
    ...(extra?.lastRuntimeEventAt !== undefined
      ? { lastRuntimeEventAt: extra.lastRuntimeEventAt }
      : {}),
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

interface AdapterGenerationRecord {
  readonly currentAdapter: ProviderAdapterShape<ProviderAdapterError>;
  readonly currentGeneration: number;
}

interface TrackedMcpSessionRecord {
  readonly adapterGeneration: number;
  readonly providerSessionId: string;
}

const makeProviderService = Effect.fn("makeProviderService")(function* (
  options?: ProviderServiceLiveOptions,
) {
  const analytics = yield* Effect.service(AnalyticsService.AnalyticsService);
  const eventLoggers = yield* ProviderEventLoggers.ProviderEventLoggers;
  // Options-provided logger wins (test overrides); otherwise we take whatever
  // the `ProviderEventLoggers` tag exposes — `undefined` means "no canonical
  // log writer is attached", which downstream code already handles as a
  // no-op.
  const canonicalEventLogger = options?.canonicalEventLogger ?? eventLoggers.canonical;

  const registry = yield* ProviderAdapterRegistry.ProviderAdapterRegistry;
  const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
  const runtimeEventPubSub = yield* PubSub.unbounded<ProviderRuntimeEvent>();
  const nowIso = Effect.map(DateTime.now, DateTime.formatIso);
  const mcpEndCleanupRetryDelay = "250 millis";
  const adapterGenerations = yield* Ref.make(
    new Map<ProviderInstanceId, AdapterGenerationRecord>(),
  );
  const trackedMcpSessions = yield* Ref.make(
    new Map<ProviderInstanceId, Map<ThreadId, TrackedMcpSessionRecord>>(),
  );
  const pendingMcpSessionStarts = yield* Ref.make(new Map<ThreadId, number>());
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

  const publishRuntimeEvent = (event: ProviderRuntimeEvent): Effect.Effect<void> =>
    Effect.succeed(event).pipe(
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
      readonly lastRuntimeEvent?: string;
      readonly lastRuntimeEventAt?: string;
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

  const persistSessionStateRuntimeState = (
    source: {
      readonly instanceId: ProviderInstanceId;
      readonly provider: ProviderDriverKind;
      readonly adapterGeneration: number;
    },
    event: ProviderRuntimeEvent,
  ) => {
    if (event.type !== "session.state.changed") {
      return Effect.void;
    }
    const resumeCursor = readResumeCursorFromRuntimeDetail(event.payload.detail);
    if (event.payload.state !== "waiting" && resumeCursor === undefined) {
      return Effect.void;
    }

    return Effect.gen(function* () {
      if (!(yield* isCurrentAdapterGeneration(source.instanceId, source.adapterGeneration))) {
        return;
      }
      const binding = Option.getOrUndefined(
        yield* directory
          .getBinding(event.threadId)
          .pipe(
            Effect.orElseSucceed(() =>
              Option.none<ProviderSessionDirectory.ProviderRuntimeBinding>(),
            ),
          ),
      );
      if (
        binding !== undefined &&
        (binding.provider !== source.provider || binding.providerInstanceId !== source.instanceId)
      ) {
        return;
      }
      const previousPayload = readRecord(binding?.runtimePayload) ?? {};
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
          lastRuntimeEvent: event.type,
          lastRuntimeEventAt: event.createdAt,
        },
      });
    });
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
      if (
        persistedActiveTurnId !== null &&
        persistedActiveTurnId !== undefined &&
        String(persistedActiveTurnId) !== String(event.turnId)
      ) {
        return;
      }
      const activeTurnMatches =
        persistedActiveTurnId !== null &&
        persistedActiveTurnId !== undefined &&
        String(persistedActiveTurnId) === String(event.turnId);

      yield* directory.upsert({
        threadId: event.threadId,
        provider: source.provider,
        providerInstanceId: source.instanceId,
        runtimeMode: binding.runtimeMode ?? "full-access",
        status: binding.status ?? "running",
        ...(binding.resumeCursor !== undefined ? { resumeCursor: binding.resumeCursor } : {}),
        runtimePayload: {
          ...previousPayload,
          ...(activeTurnMatches ? { activeTurnId: null } : {}),
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
      const lastTerminalTurnId = previousPayload.lastTerminalTurnId;
      if (
        lastTerminalTurnId !== null &&
        lastTerminalTurnId !== undefined &&
        String(lastTerminalTurnId) === String(event.turnId)
      ) {
        return;
      }
      const persistedActiveTurnId = previousPayload.activeTurnId;
      if (
        persistedActiveTurnId !== null &&
        persistedActiveTurnId !== undefined &&
        String(persistedActiveTurnId) !== String(event.turnId)
      ) {
        return;
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
          lastRuntimeEvent: event.type,
          lastRuntimeEventAt: event.createdAt,
        },
      });
    });
  };

  const clearFailedSendTurnRuntimeState = (
    source: {
      readonly instanceId: ProviderInstanceId;
      readonly provider: ProviderDriverKind;
      readonly adapter: ProviderAdapterShape<ProviderAdapterError>;
    },
    threadId: ThreadId,
    previousBinding: ProviderSessionDirectory.ProviderRuntimeBinding | undefined,
  ) =>
    Effect.gen(function* () {
      const binding = Option.getOrUndefined(
        yield* directory
          .getBinding(threadId)
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

      const currentPayload = readRecord(binding.runtimePayload) ?? {};
      if (currentPayload.lastRuntimeEvent !== "turn.started") {
        return;
      }
      const currentActiveTurnId = currentPayload.activeTurnId;
      if (currentActiveTurnId === null || currentActiveTurnId === undefined) {
        return;
      }
      const liveActiveTurnId = yield* Effect.gen(function* () {
        if (!(yield* source.adapter.hasSession(threadId))) {
          return undefined;
        }
        const sessions = yield* source.adapter.listSessions();
        return sessions.find((session) => session.threadId === threadId)?.activeTurnId;
      });
      if (
        liveActiveTurnId !== undefined &&
        String(liveActiveTurnId) === String(currentActiveTurnId)
      ) {
        return;
      }

      const previousPayload = readRecord(previousBinding?.runtimePayload) ?? {};
      const previousActiveTurnId = previousPayload.activeTurnId;
      const restoredPayload: Record<string, unknown> = {
        ...currentPayload,
        activeTurnId: previousActiveTurnId ?? null,
        lastRuntimeEvent: previousPayload.lastRuntimeEvent ?? "provider.sendTurn.failed",
      };
      if (previousPayload.lastRuntimeEventAt !== undefined) {
        restoredPayload.lastRuntimeEventAt = previousPayload.lastRuntimeEventAt;
      } else {
        restoredPayload.lastRuntimeEventAt = yield* nowIso;
      }

      yield* directory.upsert({
        threadId,
        provider: source.provider,
        providerInstanceId: source.instanceId,
        runtimeMode: binding.runtimeMode ?? "full-access",
        status: previousBinding?.status ?? binding.status ?? "running",
        ...(binding.resumeCursor !== undefined ? { resumeCursor: binding.resumeCursor } : {}),
        runtimePayload: restoredPayload,
      });
    });

  const processRuntimeEvent = (
    source: {
      readonly instanceId: ProviderInstanceId;
      readonly provider: ProviderDriverKind;
      readonly adapterGeneration: number;
    },
    event: ProviderRuntimeEvent,
  ): Effect.Effect<void> =>
    Effect.sync(() => correlateRuntimeEventWithInstance(source, event)).pipe(
      Effect.flatMap((canonicalEvent) =>
        Effect.gen(function* () {
          const isSessionEnd = isProviderSessionEndEvent(canonicalEvent);
          const observedMcpSession = isSessionEnd
            ? McpProviderSession.readMcpProviderSession(canonicalEvent.threadId)
            : undefined;
          const observedDuringPendingStart = isSessionEnd
            ? yield* hasPendingMcpSessionStart(canonicalEvent.threadId)
            : false;
          yield* persistSessionStateRuntimeState(source, canonicalEvent).pipe(
            Effect.catchCause((cause) =>
              Effect.logWarning("provider.session.state-persist-failed", {
                threadId: canonicalEvent.threadId,
                provider: canonicalEvent.provider,
                cause,
              }),
            ),
          );
          yield* persistStartedTurnRuntimeState(source, canonicalEvent).pipe(
            Effect.catchCause((cause) =>
              Effect.logWarning("provider.session.turn-start-persist-failed", {
                threadId: canonicalEvent.threadId,
                provider: canonicalEvent.provider,
                cause,
              }),
            ),
          );
          yield* persistTerminalTurnRuntimeState(source, canonicalEvent).pipe(
            Effect.catchCause((cause) =>
              Effect.logWarning("provider.session.terminal-turn-persist-failed", {
                threadId: canonicalEvent.threadId,
                provider: canonicalEvent.provider,
                cause,
              }),
            ),
          );
          yield* increment(providerRuntimeEventsTotal, {
            provider: canonicalEvent.provider,
            eventType: canonicalEvent.type,
          });
          if (isSessionEnd) {
            yield* clearMcpSessionAfterProviderSessionEnds(source, canonicalEvent, {
              ...(observedMcpSession
                ? { providerSessionId: observedMcpSession.providerSessionId }
                : {}),
              duringPendingStart: observedDuringPendingStart,
            }).pipe(Effect.forkDetach, Effect.asVoid);
          }
          yield* publishRuntimeEvent(canonicalEvent);
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
      const adapterGeneration = yield* observeAdapterGeneration(id, adapter);
      next.set(id, adapter);
      if (previous.get(id) !== adapter) {
        yield* Stream.runForEach(adapter.streamEvents, (event) =>
          processRuntimeEvent(
            {
              instanceId: id,
              provider: adapter.provider,
              adapterGeneration,
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
        return getCurrentAdapterGeneration(instanceId).pipe(
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
      const persistedActiveTurnId = readResumableActiveTurnId(
        input.binding.provider,
        input.binding.runtimePayload,
      );

      yield* beginMcpSessionStart(input.binding.threadId);
      let preparedMcpSession: McpProviderSession.McpProviderSessionConfig | undefined;
      const resumed = yield* Effect.gen(function* () {
        preparedMcpSession = yield* prepareMcpSession(input.binding.threadId, bindingInstanceId);
        return yield* adapter.startSession({
          threadId: input.binding.threadId,
          provider: input.binding.provider,
          providerInstanceId: bindingInstanceId,
          ...(persistedCwd ? { cwd: persistedCwd } : {}),
          ...(persistedModelSelection ? { modelSelection: persistedModelSelection } : {}),
          ...(hasResumeCursor ? { resumeCursor: input.binding.resumeCursor } : {}),
          ...(persistedDetached !== undefined ? { detached: persistedDetached } : {}),
          ...(persistedActiveTurnId !== undefined ? { activeTurnId: persistedActiveTurnId } : {}),
          runtimeMode: input.binding.runtimeMode ?? "full-access",
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
        isActive: true,
      } as const;
    }

    if (!input.allowRecovery) {
      return {
        adapter,
        instanceId,
        threadId: input.threadId,
        isActive: false,
      } as const;
    }

    const recovered = yield* recoverSessionForThread({
      binding,
      operation: input.operation,
    });
    return {
      adapter: recovered.adapter,
      instanceId,
      threadId: input.threadId,
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
      return yield* Effect.gen(function* () {
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
        const adapterGeneration = yield* getAdapterGenerationForStart(resolvedInstanceId, adapter);
        if (adapterGeneration === 0) {
          return yield* toValidationError(
            "ProviderService.startSession",
            `Provider instance '${resolvedInstanceId}' was replaced before starting thread '${threadId}'. Retry the start.`,
          );
        }
        yield* beginMcpSessionStart(threadId);
        let preparedMcpSession: McpProviderSession.McpProviderSessionConfig | undefined;
        const session = yield* Effect.gen(function* () {
          preparedMcpSession = yield* prepareMcpSession(threadId, resolvedInstanceId);
          return yield* adapter.startSession({
            ...input,
            providerInstanceId: resolvedInstanceId,
            ...(effectiveCwd !== undefined ? { cwd: effectiveCwd } : {}),
            ...(effectiveResumeCursor !== undefined ? { resumeCursor: effectiveResumeCursor } : {}),
            ...(effectiveDetached !== undefined ? { detached: effectiveDetached } : {}),
            ...(effectiveActiveTurnId !== undefined ? { activeTurnId: effectiveActiveTurnId } : {}),
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
        });
        yield* analytics.record("provider.session.started", {
          provider: sessionWithInstance.provider,
          runtimeMode: input.runtimeMode,
          hasResumeCursor: sessionWithInstance.resumeCursor !== undefined,
          hasCwd: typeof effectiveCwd === "string" && effectiveCwd.trim().length > 0,
          hasModel:
            typeof input.modelSelection?.model === "string" &&
            input.modelSelection.model.trim().length > 0,
        });

        return sessionWithInstance;
      }).pipe(
        withMetrics({
          counter: providerSessionsTotal,
          attributes: () =>
            providerMetricAttributes(metricProvider, {
              operation: "start",
            }),
        }),
      );
    },
  );

  const sendTurn: ProviderServiceMethod<"sendTurn"> = Effect.fn("sendTurn")(function* (rawInput) {
    const parsed = yield* decodeInputOrValidationError({
      operation: "ProviderService.sendTurn",
      schema: ProviderSendTurnInput,
      payload: rawInput,
    });

    const input = {
      ...parsed,
      attachments: parsed.attachments ?? [],
    };
    if (!input.input && input.attachments.length === 0) {
      return yield* toValidationError(
        "ProviderService.sendTurn",
        "Either input text or at least one attachment is required",
      );
    }
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
      const bindingBeforeSend = Option.getOrUndefined(
        yield* directory
          .getBinding(input.threadId)
          .pipe(
            Effect.orElseSucceed(() =>
              Option.none<ProviderSessionDirectory.ProviderRuntimeBinding>(),
            ),
          ),
      );
      const turn = yield* routed.adapter.sendTurn(input).pipe(
        Effect.catch((error) =>
          clearFailedSendTurnRuntimeState(
            {
              provider: routed.adapter.provider,
              instanceId: routed.instanceId,
              adapter: routed.adapter,
            },
            input.threadId,
            bindingBeforeSend,
          ).pipe(
            Effect.catchCause((cause) =>
              Effect.logWarning("provider.session.send-turn-failure-cleanup-failed", {
                threadId: input.threadId,
                provider: routed.adapter.provider,
                cause,
              }),
            ),
            Effect.andThen(Effect.fail(error)),
          ),
        ),
      );
      const binding = Option.getOrUndefined(
        yield* directory
          .getBinding(input.threadId)
          .pipe(
            Effect.orElseSucceed(() =>
              Option.none<ProviderSessionDirectory.ProviderRuntimeBinding>(),
            ),
          ),
      );
      const persistedPayload = readRecord(binding?.runtimePayload) ?? {};
      const lastTerminalTurnId = persistedPayload.lastTerminalTurnId;
      const turnAlreadyTerminated =
        lastTerminalTurnId !== null &&
        lastTerminalTurnId !== undefined &&
        String(lastTerminalTurnId) === String(turn.turnId);
      const runtimePayloadPatch: Record<string, unknown> = {
        ...(input.modelSelection !== undefined ? { modelSelection: input.modelSelection } : {}),
        activeTurnId: turnAlreadyTerminated ? null : turn.turnId,
      };
      if (!turnAlreadyTerminated) {
        runtimePayloadPatch.lastRuntimeEvent = "provider.sendTurn";
        runtimePayloadPatch.lastRuntimeEventAt = yield* nowIso;
      }
      yield* directory.upsert({
        threadId: input.threadId,
        provider: routed.adapter.provider,
        providerInstanceId: routed.instanceId,
        status: "running",
        ...(turn.resumeCursor !== undefined ? { resumeCursor: turn.resumeCursor } : {}),
        runtimePayload: mergeRuntimePayload(binding?.runtimePayload, runtimePayloadPatch),
      });
      yield* analytics.record("provider.turn.sent", {
        provider: routed.adapter.provider,
        model: input.modelSelection?.model,
        interactionMode: input.interactionMode,
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
