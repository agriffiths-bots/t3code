import { CommandId } from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schedule from "effect/Schedule";

import * as McpProviderSession from "../../mcp/McpProviderSession.ts";
import * as McpSessionRegistry from "../../mcp/McpSessionRegistry.ts";
import { OrchestrationEngineService } from "../../orchestration/Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "../../orchestration/Services/ProjectionSnapshotQuery.ts";
import { ProviderSessionDirectory } from "../Services/ProviderSessionDirectory.ts";
import {
  ProviderSessionReaper,
  type ProviderSessionReaperShape,
} from "../Services/ProviderSessionReaper.ts";
import { ProviderService } from "../Services/ProviderService.ts";

const DEFAULT_INACTIVITY_THRESHOLD_MS = 30 * 60 * 1000;
const DEFAULT_SWEEP_INTERVAL_MS = 5 * 60 * 1000;

export interface ProviderSessionReaperLiveOptions {
  readonly inactivityThresholdMs?: number;
  readonly sweepIntervalMs?: number;
}

const makeProviderSessionReaper = (options?: ProviderSessionReaperLiveOptions) =>
  Effect.gen(function* () {
    const providerService = yield* ProviderService;
    const directory = yield* ProviderSessionDirectory;
    const projectionSnapshotQuery = yield* ProjectionSnapshotQuery;
    const orchestrationEngine = yield* OrchestrationEngineService;

    const inactivityThresholdMs = Math.max(
      1,
      options?.inactivityThresholdMs ?? DEFAULT_INACTIVITY_THRESHOLD_MS,
    );
    const sweepIntervalMs = Math.max(1, options?.sweepIntervalMs ?? DEFAULT_SWEEP_INTERVAL_MS);

    const sweep = Effect.gen(function* () {
      const bindings = yield* directory.listBindings();
      const activeProviderThreadIds = yield* providerService.listSessions().pipe(
        Effect.map((sessions) => new Set(sessions.map((session) => String(session.threadId)))),
        Effect.catchCause((cause) =>
          Effect.logWarning("provider.session.reaper.list-active-sessions-failed", {
            cause,
          }).pipe(Effect.as(null)),
        ),
      );
      const now = yield* Clock.currentTimeMillis;
      const nowIso = DateTime.formatIso(Option.getOrThrow(DateTime.make(now)));
      let reapedCount = 0;
      let reconciledCount = 0;

      for (const binding of bindings) {
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

        const thread = yield* projectionSnapshotQuery
          .getThreadShellById(binding.threadId)
          .pipe(Effect.map(Option.getOrUndefined));
        if (thread?.session?.activeTurnId != null) {
          const session = thread.session;
          const activeTurnId = session.activeTurnId;
          const latestTurn = thread.latestTurn;
          const canReconcile =
            activeProviderThreadIds !== null &&
            !activeProviderThreadIds.has(String(binding.threadId)) &&
            latestTurn !== null &&
            latestTurn.turnId === activeTurnId &&
            latestTurn.state !== "running";

          if (canReconcile) {
            const reconciled = yield* orchestrationEngine
              .dispatch({
                type: "thread.session.set",
                commandId: CommandId.make(
                  `server:provider-session-reconcile:${binding.threadId}:${activeTurnId}`,
                ),
                threadId: binding.threadId,
                session: {
                  threadId: binding.threadId,
                  status: "stopped",
                  providerName: session.providerName ?? binding.provider,
                  ...(session.providerInstanceId !== undefined
                    ? { providerInstanceId: session.providerInstanceId }
                    : binding.providerInstanceId !== undefined
                      ? { providerInstanceId: binding.providerInstanceId }
                      : {}),
                  runtimeMode: session.runtimeMode,
                  activeTurnId: null,
                  lastError: session.lastError,
                  updatedAt: nowIso,
                },
                createdAt: nowIso,
              })
              .pipe(
                Effect.flatMap(() => McpSessionRegistry.revokeActiveMcpThread(binding.threadId)),
                Effect.tap(() =>
                  Effect.sync(() => McpProviderSession.clearMcpProviderSession(binding.threadId)),
                ),
                Effect.flatMap(() =>
                  directory.upsert({
                    threadId: binding.threadId,
                    provider: binding.provider,
                    ...(binding.providerInstanceId !== undefined
                      ? { providerInstanceId: binding.providerInstanceId }
                      : {}),
                    ...(binding.adapterKey !== undefined ? { adapterKey: binding.adapterKey } : {}),
                    runtimeMode: binding.runtimeMode ?? session.runtimeMode,
                    status: "stopped",
                    resumeCursor: binding.resumeCursor,
                    runtimePayload: {
                      activeTurnId: null,
                      lastRuntimeEvent: "provider-session-reconciled",
                      lastRuntimeEventAt: nowIso,
                    },
                  }),
                ),
                Effect.tap(() =>
                  Effect.logInfo("provider.session.reaper.reconciled-terminal-active-turn", {
                    threadId: binding.threadId,
                    provider: binding.provider,
                    activeTurnId,
                    turnState: latestTurn?.state,
                    idleDurationMs,
                  }),
                ),
                Effect.as(true),
                Effect.catchCause((cause) =>
                  Effect.logWarning("provider.session.reaper.reconcile-failed", {
                    threadId: binding.threadId,
                    provider: binding.provider,
                    activeTurnId,
                    idleDurationMs,
                    cause,
                  }).pipe(Effect.as(false)),
                ),
              );
            if (reconciled) {
              reconciledCount += 1;
            }
            continue;
          }

          yield* Effect.logDebug("provider.session.reaper.skipped-active-turn", {
            threadId: binding.threadId,
            activeTurnId: session.activeTurnId,
            idleDurationMs,
            latestTurnState: latestTurn?.state ?? null,
            activeProviderSessionListed:
              activeProviderThreadIds === null
                ? "unknown"
                : activeProviderThreadIds.has(String(binding.threadId)),
          });
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
          reconciledCount,
          totalBindings: bindings.length,
        });
      } else if (reconciledCount > 0) {
        yield* Effect.logInfo("provider.session.reaper.sweep-complete", {
          reapedCount,
          reconciledCount,
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
        });
      });

    return {
      start,
    } satisfies ProviderSessionReaperShape;
  });

export const makeProviderSessionReaperLive = (options?: ProviderSessionReaperLiveOptions) =>
  Layer.effect(ProviderSessionReaper, makeProviderSessionReaper(options));

export const ProviderSessionReaperLive = makeProviderSessionReaperLive();
