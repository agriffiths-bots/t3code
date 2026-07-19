import {
  CommandId,
  EventId,
  type OrchestrationThreadShell,
  type ProviderSession,
  type ThreadId,
  type TurnId,
} from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schedule from "effect/Schedule";

import { ProjectionPendingApprovalRepositoryLive } from "../../persistence/Layers/ProjectionPendingApprovals.ts";
import { ProjectionPendingApprovalRepository } from "../../persistence/Services/ProjectionPendingApprovals.ts";
import { OrchestrationEngineService } from "../../orchestration/Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "../../orchestration/Services/ProjectionSnapshotQuery.ts";
import {
  ProviderSessionDirectory,
  type ProviderRuntimeBindingWithMetadata,
} from "../Services/ProviderSessionDirectory.ts";
import {
  ProviderSessionReaper,
  type ProviderSessionReaperShape,
} from "../Services/ProviderSessionReaper.ts";
import { ProviderService } from "../Services/ProviderService.ts";

import { threadAudienceSystemDispatchAuthority } from "../../orchestration/commandAudienceGuard.ts";
const DEFAULT_INACTIVITY_THRESHOLD_MS = 30 * 60 * 1000;
const DEFAULT_SWEEP_INTERVAL_MS = 5 * 60 * 1000;
const DEFAULT_PERMISSION_REQUEST_TIMEOUT_MS = 4 * 60 * 60 * 1000;
const DEFAULT_STOP_TIMEOUT_MS = 10 * 1000;

export interface ProviderSessionReaperLiveOptions {
  readonly inactivityThresholdMs?: number;
  readonly sweepIntervalMs?: number;
  readonly permissionRequestTimeoutMs?: number;
  readonly stopTimeoutMs?: number;
}

function providerSessionForActiveTurn(
  sessions: ReadonlyArray<ProviderSession>,
  threadId: ThreadId,
  turnId: TurnId,
): ProviderSession | undefined {
  const threadSessions = sessions.filter((session) => session.threadId === threadId);
  return (
    threadSessions.find((session) => liveSessionStillRunsTurn(session, turnId)) ??
    threadSessions.find((session) => session.activeTurnId === turnId)
  );
}

function liveSessionStillRunsTurn(session: ProviderSession | undefined, turnId: TurnId): boolean {
  return (
    session !== undefined &&
    (session.status === "connecting" ||
      session.status === "running" ||
      session.status === "waiting") &&
    session.activeTurnId === turnId
  );
}

function nonTerminalSessionForThread(
  sessions: ReadonlyArray<ProviderSession>,
  threadId: ThreadId,
): ProviderSession | undefined {
  return sessions.find(
    (session) =>
      session.threadId === threadId &&
      (session.status === "connecting" ||
        session.status === "ready" ||
        session.status === "running" ||
        session.status === "waiting"),
  );
}

function bindingRecordsTerminalTurn(
  binding: ProviderRuntimeBindingWithMetadata,
  turnId: TurnId,
): boolean {
  const runtimePayload = binding.runtimePayload;
  if (
    runtimePayload === null ||
    typeof runtimePayload !== "object" ||
    Array.isArray(runtimePayload)
  ) {
    return false;
  }
  const payload = runtimePayload as Record<string, unknown>;
  const lastRuntimeEvent = payload.lastRuntimeEvent;
  const watchdogOwnsTerminalMarker =
    typeof lastRuntimeEvent === "string" && lastRuntimeEvent.startsWith("provider.turn.watchdog.");
  return (
    !watchdogOwnsTerminalMarker &&
    typeof payload.lastTerminalTurnId === "string" &&
    payload.lastTerminalTurnId === turnId
  );
}

function activeTurnKey(threadId: ThreadId, turnId: TurnId): string {
  return `${threadId}:${turnId}`;
}

const makeProviderSessionReaper = (options?: ProviderSessionReaperLiveOptions) =>
  Effect.gen(function* () {
    const providerService = yield* ProviderService;
    const directory = yield* ProviderSessionDirectory;
    const projectionSnapshotQuery = yield* ProjectionSnapshotQuery;
    const pendingApprovalRepository = yield* ProjectionPendingApprovalRepository;
    const orchestrationEngine = yield* OrchestrationEngineService;

    const inactivityThresholdMs = Math.max(
      1,
      options?.inactivityThresholdMs ?? DEFAULT_INACTIVITY_THRESHOLD_MS,
    );
    const sweepIntervalMs = Math.max(1, options?.sweepIntervalMs ?? DEFAULT_SWEEP_INTERVAL_MS);
    const permissionRequestTimeoutMs = Math.max(
      1,
      options?.permissionRequestTimeoutMs ?? DEFAULT_PERMISSION_REQUEST_TIMEOUT_MS,
    );
    const stopTimeoutMs = Math.max(1, options?.stopTimeoutMs ?? DEFAULT_STOP_TIMEOUT_MS);
    const projectionLagDeferrals = new Map<ThreadId, TurnId>();

    const stopProviderSession = <E, R>(input: {
      readonly threadId: ThreadId;
      readonly turnId: TurnId;
      readonly reason: string;
      readonly sessionOwnershipId?: string;
      readonly allowLegacyActiveTurnMatch?: boolean;
      readonly onOwned: Effect.Effect<void, E, R>;
      readonly onStopped: Effect.Effect<void, E, R>;
    }) =>
      providerService
        .stopFailedSession({
          threadId: input.threadId,
          turnId: input.turnId,
          reason: input.reason,
          ...(input.sessionOwnershipId !== undefined
            ? { sessionOwnershipId: input.sessionOwnershipId }
            : input.allowLegacyActiveTurnMatch === true
              ? { allowLegacyActiveTurnMatch: true }
              : { requireSessionAbsent: true }),
          onOwned: input.onOwned,
          onStopped: input.onStopped,
        })
        .pipe(
          Effect.timeoutOption(Duration.millis(stopTimeoutMs)),
          Effect.flatMap((result) =>
            Option.isSome(result)
              ? Effect.succeed(result.value)
              : Effect.logWarning("provider.turn.watchdog.stop-timeout", {
                  threadId: input.threadId,
                  turnId: input.turnId,
                  stopTimeoutMs,
                }).pipe(Effect.as(false)),
          ),
          Effect.catchCause((cause) =>
            Cause.hasInterruptsOnly(cause)
              ? Effect.failCause(cause)
              : Effect.logWarning("provider.turn.watchdog.stop-failed", {
                  threadId: input.threadId,
                  turnId: input.turnId,
                  cause,
                }).pipe(Effect.as(false)),
          ),
        );

    const persistReleasedBinding = Effect.fn("ProviderSessionReaper.persistReleasedBinding")(
      function* (input: {
        readonly binding: ProviderRuntimeBindingWithMetadata;
        readonly turnId: TurnId;
        readonly reason: string;
        readonly releasedAt: string;
        readonly lastRuntimeEvent: string;
      }) {
        const latest = Option.getOrUndefined(yield* directory.getBinding(input.binding.threadId));
        const latestRuntimePayload = latest?.runtimePayload ?? input.binding.runtimePayload;
        const preservedRuntimePayload =
          latestRuntimePayload !== null &&
          typeof latestRuntimePayload === "object" &&
          !Array.isArray(latestRuntimePayload)
            ? latestRuntimePayload
            : {};
        const providerInstanceId = latest?.providerInstanceId ?? input.binding.providerInstanceId;
        const resumeCursor = latest?.resumeCursor ?? input.binding.resumeCursor;
        yield* directory.upsert({
          threadId: input.binding.threadId,
          provider: latest?.provider ?? input.binding.provider,
          ...(providerInstanceId !== undefined ? { providerInstanceId } : {}),
          runtimeMode: latest?.runtimeMode ?? input.binding.runtimeMode ?? "full-access",
          status: "stopped",
          ...(resumeCursor !== undefined ? { resumeCursor } : {}),
          runtimePayload: {
            ...preservedRuntimePayload,
            activeTurnId: null,
            sendTurnOperationId: null,
            lastError: input.reason,
            lastRuntimeEvent: input.lastRuntimeEvent,
            lastRuntimeEventAt: input.releasedAt,
            lastTerminalTurnId: input.turnId,
          },
        });
      },
    );

    const failActiveTurn = Effect.fn("ProviderSessionReaper.failActiveTurn")(function* (input: {
      readonly binding: ProviderRuntimeBindingWithMetadata;
      readonly thread: OrchestrationThreadShell;
      readonly turnId: TurnId;
      readonly reason: string;
      readonly failedAt: string;
      readonly expiredApprovalRequestIds?: ReadonlyArray<string>;
      readonly allowLegacyActiveTurnMatch?: boolean;
    }) {
      const key = activeTurnKey(input.thread.id, input.turnId);
      const providerInstanceId =
        input.thread.session?.providerInstanceId ?? input.binding.providerInstanceId;
      const runtimePayload: Record<string, unknown> =
        input.binding.runtimePayload !== null &&
        typeof input.binding.runtimePayload === "object" &&
        !Array.isArray(input.binding.runtimePayload)
          ? (input.binding.runtimePayload as Record<string, unknown>)
          : {};
      const sessionOwnershipId =
        typeof runtimePayload.sessionOwnershipId === "string"
          ? runtimePayload.sessionOwnershipId
          : undefined;
      const providerStopped = yield* stopProviderSession({
        threadId: input.thread.id,
        turnId: input.turnId,
        reason: input.reason,
        ...(sessionOwnershipId !== undefined ? { sessionOwnershipId } : {}),
        ...(input.allowLegacyActiveTurnMatch === true ? { allowLegacyActiveTurnMatch: true } : {}),
        onOwned: Effect.gen(function* () {
          yield* orchestrationEngine.dispatch(
            {
              type: "thread.session.set",
              commandId: CommandId.make(`provider-turn-watchdog:${key}`),
              threadId: input.thread.id,
              session: {
                threadId: input.thread.id,
                status: "error",
                providerName: input.thread.session?.providerName ?? input.binding.provider,
                ...(providerInstanceId !== undefined ? { providerInstanceId } : {}),
                runtimeMode:
                  input.thread.session?.runtimeMode ?? input.binding.runtimeMode ?? "full-access",
                activeTurnId: null,
                lastError: input.reason,
                updatedAt: input.failedAt,
              },
              createdAt: input.failedAt,
            },
            threadAudienceSystemDispatchAuthority(input.thread, "ProviderSessionReaper"),
          );

          yield* Effect.forEach(
            input.expiredApprovalRequestIds ?? [],
            (requestId) =>
              // `approval.resolved` is the canonical event consumed by
              // ProjectionPipeline to mark the pending-approval row resolved
              // and decrement the thread shell's pending count.
              orchestrationEngine.dispatch(
                {
                  type: "thread.activity.append",
                  commandId: CommandId.make(`provider-turn-watchdog:approval:${requestId}`),
                  threadId: input.thread.id,
                  activity: {
                    id: EventId.make(`provider-turn-watchdog:approval:${requestId}`),
                    tone: "error",
                    kind: "approval.resolved",
                    summary: "Approval request timed out",
                    payload: { requestId, decision: "cancel" },
                    turnId: input.turnId,
                    createdAt: input.failedAt,
                  },
                  createdAt: input.failedAt,
                },
                threadAudienceSystemDispatchAuthority(input.thread, "ProviderSessionReaper"),
              ),
            { concurrency: 1 },
          ).pipe(Effect.asVoid);
        }),
        onStopped: Effect.gen(function* () {
          yield* orchestrationEngine.dispatch(
            {
              type: "thread.session.set",
              commandId: CommandId.make(`provider-turn-watchdog:release:${key}`),
              threadId: input.thread.id,
              session: {
                threadId: input.thread.id,
                status: "stopped",
                providerName: input.thread.session?.providerName ?? input.binding.provider,
                ...(providerInstanceId !== undefined ? { providerInstanceId } : {}),
                runtimeMode:
                  input.thread.session?.runtimeMode ?? input.binding.runtimeMode ?? "full-access",
                activeTurnId: null,
                lastError: input.reason,
                updatedAt: input.failedAt,
              },
              createdAt: input.failedAt,
            },
            threadAudienceSystemDispatchAuthority(input.thread, "ProviderSessionReaper"),
          );
          yield* persistReleasedBinding({
            binding: input.binding,
            turnId: input.turnId,
            reason: input.reason,
            releasedAt: input.failedAt,
            lastRuntimeEvent: "provider.turn.watchdog.failed",
          });
        }),
      });
      if (!providerStopped) {
        yield* Effect.logInfo("provider.turn.watchdog.cleanup-pending-or-stale", {
          threadId: input.thread.id,
          turnId: input.turnId,
        });
        return;
      }

      yield* Effect.logWarning("provider.turn.watchdog.failed-turn", {
        threadId: input.thread.id,
        turnId: input.turnId,
        provider: input.binding.provider,
        reason: input.reason,
      });
    });

    const sweep = Effect.gen(function* () {
      const bindings = yield* directory.listBindings();
      const liveSessions = yield* providerService.listSessions().pipe(
        Effect.map(Option.some),
        Effect.catchCause((cause) =>
          Cause.hasInterruptsOnly(cause)
            ? Effect.failCause(cause)
            : Effect.logWarning("provider.turn.watchdog.session-snapshot-failed", {
                cause,
              }).pipe(Effect.as(Option.none<ReadonlyArray<ProviderSession>>())),
        ),
      );
      const now = yield* Clock.currentTimeMillis;
      let reapedCount = 0;

      for (const binding of bindings) {
        const thread = yield* projectionSnapshotQuery
          .getThreadShellById(binding.threadId)
          .pipe(Effect.map(Option.getOrUndefined));
        if (
          thread?.session?.activeTurnId != null &&
          thread.latestTurn?.turnId === thread.session.activeTurnId &&
          thread.latestTurn.state === "running"
        ) {
          const activeTurnId = thread.session.activeTurnId;
          const pendingApprovals = yield* pendingApprovalRepository.listByThreadId({
            threadId: binding.threadId,
          });
          const turnStartedAtMs = Date.parse(
            thread.latestTurn.startedAt ?? thread.latestTurn.requestedAt,
          );
          const expiredApprovals = pendingApprovals.filter((approval) => {
            if (approval.status !== "pending") return false;
            if (
              approval.turnId !== activeTurnId &&
              !(
                approval.turnId === null &&
                !Number.isNaN(turnStartedAtMs) &&
                Date.parse(approval.createdAt) >= turnStartedAtMs
              )
            ) {
              return false;
            }
            const createdAtMs = Date.parse(approval.createdAt);
            return !Number.isNaN(createdAtMs) && now - createdAtMs >= permissionRequestTimeoutMs;
          });
          const failedAt = DateTime.formatIso(yield* DateTime.now);
          const expiredApprovalRequestIds = expiredApprovals.map((approval) => approval.requestId);

          // ProviderService can persist terminal runtime state, or a replacement
          // can become live in the adapter, before the projection consumes the
          // canonical event. Record one full sweep for projection catch-up. A
          // non-terminal same-thread session remains live evidence after that
          // sweep and must never be stopped as though the provider disappeared.
          const bindingRecordsTerminal = bindingRecordsTerminalTurn(binding, activeTurnId);
          const exactTurnSession = Option.isSome(liveSessions)
            ? providerSessionForActiveTurn(liveSessions.value, binding.threadId, activeTurnId)
            : undefined;
          const nonTerminalThreadSession = Option.isSome(liveSessions)
            ? nonTerminalSessionForThread(liveSessions.value, binding.threadId)
            : undefined;
          const sessionProjectionMayLag =
            nonTerminalThreadSession !== undefined &&
            !liveSessionStillRunsTurn(exactTurnSession, activeTurnId);
          if (
            (bindingRecordsTerminal || sessionProjectionMayLag) &&
            projectionLagDeferrals.get(binding.threadId) !== activeTurnId
          ) {
            projectionLagDeferrals.set(binding.threadId, activeTurnId);
            continue;
          }
          if (!bindingRecordsTerminal && !sessionProjectionMayLag) {
            projectionLagDeferrals.delete(binding.threadId);
          }

          if (sessionProjectionMayLag) {
            continue;
          }

          if (binding.status === "error" || binding.status === "stopped") {
            yield* failActiveTurn({
              binding,
              thread,
              turnId: activeTurnId,
              reason:
                binding.status === "error"
                  ? "Provider session failed while the turn was running."
                  : "Provider session closed while the turn was running.",
              failedAt,
              expiredApprovalRequestIds,
            });
            continue;
          }

          if (Option.isNone(liveSessions)) {
            // A failed global snapshot is not evidence that this session is
            // gone or alive. Binding failures above remain actionable, while
            // both missing-session detection and permission expiry wait for a
            // healthy enumeration on a later sweep.
            continue;
          }

          const liveSession = exactTurnSession;
          if (liveSessionStillRunsTurn(liveSession, activeTurnId)) {
            if (expiredApprovals.length > 0) {
              const oldest = expiredApprovals[0]!;
              yield* failActiveTurn({
                binding,
                thread,
                turnId: activeTurnId,
                reason: `Provider permission request '${oldest.requestId}' was unanswered for ${permissionRequestTimeoutMs}ms.`,
                failedAt,
                expiredApprovalRequestIds,
                allowLegacyActiveTurnMatch: true,
              });
            }
            continue;
          }

          if (liveSession?.status === "error" || liveSession?.status === "closed") {
            yield* failActiveTurn({
              binding,
              thread,
              turnId: activeTurnId,
              reason:
                liveSession.lastError ??
                (liveSession.status === "error"
                  ? "Provider session failed while the turn was running."
                  : "Provider session closed while the turn was running."),
              failedAt,
              expiredApprovalRequestIds,
            });
            continue;
          }

          // All non-terminal statuses are live or projection-lag evidence. Only
          // a terminal exact owner or a genuinely absent same-thread session may
          // reach the failure paths above/below.
          if (nonTerminalThreadSession !== undefined) {
            continue;
          }

          // A successful global snapshot with no live owner for the exact
          // active turn is definitive process/session loss. Fail in this
          // sweep; the identity-bound stop path fences any concurrent
          // replacement before physical cleanup.
          yield* failActiveTurn({
            binding,
            thread,
            turnId: activeTurnId,
            reason: `Provider session disappeared while turn '${activeTurnId}' was running.`,
            failedAt,
            expiredApprovalRequestIds,
          });
          continue;
        }

        projectionLagDeferrals.delete(binding.threadId);

        const terminalFailedSession = thread?.session?.status === "error" ? thread.session : null;
        const terminalFailedTurn =
          terminalFailedSession !== null && thread?.latestTurn?.state === "error"
            ? thread.latestTurn
            : null;
        if (terminalFailedSession !== null && terminalFailedTurn !== null && thread !== undefined) {
          const releasedAt = DateTime.formatIso(yield* DateTime.now);
          const reason = terminalFailedSession.lastError ?? "Provider turn failed";
          const runtimePayload: Record<string, unknown> =
            binding.runtimePayload !== null &&
            typeof binding.runtimePayload === "object" &&
            !Array.isArray(binding.runtimePayload)
              ? (binding.runtimePayload as Record<string, unknown>)
              : {};
          const sessionOwnershipId =
            typeof runtimePayload.sessionOwnershipId === "string"
              ? runtimePayload.sessionOwnershipId
              : undefined;
          const providerStopped = yield* stopProviderSession({
            threadId: binding.threadId,
            turnId: terminalFailedTurn.turnId,
            reason,
            ...(sessionOwnershipId !== undefined ? { sessionOwnershipId } : {}),
            onOwned: Effect.void,
            onStopped: Effect.gen(function* () {
              yield* orchestrationEngine.dispatch(
                {
                  type: "thread.session.set",
                  commandId: CommandId.make(
                    `provider-turn-watchdog:retry-release:${binding.threadId}:${terminalFailedTurn.turnId}`,
                  ),
                  threadId: binding.threadId,
                  session: {
                    threadId: binding.threadId,
                    status: "stopped",
                    providerName: terminalFailedSession.providerName ?? binding.provider,
                    ...(terminalFailedSession.providerInstanceId !== undefined
                      ? { providerInstanceId: terminalFailedSession.providerInstanceId }
                      : binding.providerInstanceId !== undefined
                        ? { providerInstanceId: binding.providerInstanceId }
                        : {}),
                    runtimeMode:
                      terminalFailedSession.runtimeMode ?? binding.runtimeMode ?? "full-access",
                    activeTurnId: null,
                    lastError: reason,
                    updatedAt: releasedAt,
                  },
                  createdAt: releasedAt,
                },
                threadAudienceSystemDispatchAuthority(thread, "ProviderSessionReaper"),
              );
              yield* persistReleasedBinding({
                binding,
                turnId: terminalFailedTurn.turnId,
                reason,
                releasedAt,
                lastRuntimeEvent: "provider.turn.watchdog.retry-released",
              });
            }),
          });
          if (!providerStopped) {
            continue;
          }
          continue;
        }

        if (thread?.session?.activeTurnId != null) {
          continue;
        }

        if (binding.status === "stopped") {
          continue;
        }

        const lastSeenMs = Date.parse(binding.lastSeenAt);
        if (Number.isNaN(lastSeenMs)) {
          yield* Effect.logWarning("provider.session.reaper.invalid-last-seen", {
            threadId: binding.threadId,
            provider: binding.provider,
            lastSeenAt: binding.lastSeenAt,
          });
          continue;
        }

        const idleDurationMs = now - lastSeenMs;
        if (idleDurationMs < inactivityThresholdMs) {
          continue;
        }

        const reaped = yield* providerService.stopSession({ threadId: binding.threadId }).pipe(
          Effect.tap(() =>
            Effect.logInfo("provider.session.reaped", {
              threadId: binding.threadId,
              provider: binding.provider,
              idleDurationMs,
              reason: "inactivity_threshold",
            }),
          ),
          Effect.as(true),
          Effect.catchCause((cause) =>
            Effect.logWarning("provider.session.reaper.stop-failed", {
              threadId: binding.threadId,
              provider: binding.provider,
              idleDurationMs,
              cause,
            }).pipe(Effect.as(false)),
          ),
        );

        if (reaped) {
          reapedCount += 1;
        }
      }

      if (reapedCount > 0) {
        yield* Effect.logInfo("provider.session.reaper.sweep-complete", {
          reapedCount,
          totalBindings: bindings.length,
        });
      }
    });

    const start: ProviderSessionReaperShape["start"] = () =>
      Effect.gen(function* () {
        yield* Effect.forkScoped(
          sweep.pipe(
            Effect.catch((error: unknown) =>
              Effect.logWarning("provider.session.reaper.sweep-failed", {
                error,
              }),
            ),
            Effect.catchDefect((defect: unknown) =>
              Effect.logWarning("provider.session.reaper.sweep-defect", {
                defect,
              }),
            ),
            Effect.repeat(Schedule.spaced(Duration.millis(sweepIntervalMs))),
          ),
        );

        yield* Effect.logInfo("provider.session.reaper.started", {
          inactivityThresholdMs,
          sweepIntervalMs,
          permissionRequestTimeoutMs,
        });
      });

    return {
      start,
    } satisfies ProviderSessionReaperShape;
  });

export const makeProviderSessionReaperLive = (options?: ProviderSessionReaperLiveOptions) =>
  Layer.effect(ProviderSessionReaper, makeProviderSessionReaper(options)).pipe(
    Layer.provideMerge(ProjectionPendingApprovalRepositoryLive),
  );

export const ProviderSessionReaperLive = makeProviderSessionReaperLive();
