import {
  PlanUsageSnapshotSchema,
  type EnvironmentId,
  type PlanUsageSnapshot,
  type ProviderInstanceId,
} from "@t3tools/contracts";
import { useAtomValue } from "@effect/atom-react";
import { ManagedRelay } from "@t3tools/client-runtime/relay";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { useEffect, useRef, useState } from "react";

import { environmentCatalog } from "~/connection/catalog";
import { runtime } from "~/lib/runtime";
import { usePreparedConnection } from "~/state/session";
import { serverEnvironment } from "~/state/server";
import { useAtomCommand } from "~/state/use-atom-command";

const decodePlanUsageSnapshot = Schema.decodeUnknownSync(PlanUsageSnapshotSchema);
const POLL_INTERVAL_MS = 60_000;
const DPOP_REFRESH_MARGIN_MS = 60_000;
const CLOUDFLARE_ACCESS_FETCH_HEADERS = new Set([
  "cf-access-client-id",
  "cf-access-client-secret",
  "cf-access-jwt-assertion",
]);

const createPlanUsageDpopProof = (input: {
  readonly method: "GET";
  readonly url: string;
  readonly accessToken: string;
}) =>
  runtime.runPromise(
    ManagedRelay.ManagedRelayDpopSigner.pipe(
      Effect.flatMap((signer) =>
        signer.createProof({
          method: input.method,
          url: input.url,
          accessToken: input.accessToken,
        }),
      ),
    ),
  );

function preparedConnectionFetchHeaders(
  connection: ReturnType<typeof usePreparedConnection>,
): Record<string, string> {
  if (connection._tag === "None") return {};
  const headers: Record<string, string> = {};
  for (const [name, value] of Object.entries(connection.value.socketHeaders ?? {})) {
    const normalized = name.toLowerCase();
    if (CLOUDFLARE_ACCESS_FETCH_HEADERS.has(normalized)) {
      headers[normalized] = value;
    }
  }
  return headers;
}

export async function planUsageRequestHeaders(
  connection: ReturnType<typeof usePreparedConnection>,
  url: string,
  createDpopProof: typeof createPlanUsageDpopProof = createPlanUsageDpopProof,
): Promise<HeadersInit | null> {
  if (connection._tag === "None") return null;
  const baseHeaders = preparedConnectionFetchHeaders(connection);
  const authorization = connection.value.httpAuthorization;
  if (authorization === null) return baseHeaders;
  if (authorization._tag === "Bearer") {
    return { ...baseHeaders, authorization: `Bearer ${authorization.token}` };
  }
  return {
    ...baseHeaders,
    authorization: `DPoP ${authorization.accessToken}`,
    dpop: await createDpopProof({
      method: "GET",
      url,
      accessToken: authorization.accessToken,
    }),
  };
}

export function planUsageRequestCredentials(
  connection: ReturnType<typeof usePreparedConnection>,
): RequestCredentials | undefined {
  if (connection._tag === "None") return undefined;
  return connection.value.httpAuthorization === null ? "include" : undefined;
}

export function planUsageConnectionKey(
  connection: ReturnType<typeof usePreparedConnection>,
  providerInstanceId: ProviderInstanceId | null = null,
): string | null {
  if (connection._tag === "None") return null;
  const authorization = connection.value.httpAuthorization;
  return [
    connection.value.environmentId,
    connection.value.httpBaseUrl,
    authorization?._tag ?? "Cookie",
    providerInstanceId ?? "default",
  ].join(":");
}

export function planUsageDpopNeedsRefresh(
  connection: ReturnType<typeof usePreparedConnection>,
  now: number,
): boolean {
  if (connection._tag === "None") return false;
  const authorization = connection.value.httpAuthorization;
  return (
    authorization?._tag === "Dpop" &&
    authorization.expiresAtEpochMs !== undefined &&
    authorization.expiresAtEpochMs <= now + DPOP_REFRESH_MARGIN_MS
  );
}

export function usePlanUsage(
  environmentId: EnvironmentId | null,
  providerInstanceId: ProviderInstanceId | null,
): PlanUsageSnapshot | null {
  const liveServerConfig = useAtomValue(serverEnvironment.liveConfigValueAtom(environmentId));
  const connection = usePreparedConnection(environmentId);
  const [snapshotState, setSnapshotState] = useState<{
    readonly key: string;
    readonly snapshot: PlanUsageSnapshot;
  } | null>(null);
  const retryEnvironment = useAtomCommand(environmentCatalog.retryNow, {
    label: "plan usage auth refresh",
    reportFailure: false,
    reportDefect: false,
  });
  const requestedDpopRefreshKeyRef = useRef<string | null>(null);
  const aggregateUsageStreamPendingOrAvailable =
    providerInstanceId === null &&
    (liveServerConfig === null || liveServerConfig.planUsage !== undefined);

  useEffect(() => {
    if (aggregateUsageStreamPendingOrAvailable) {
      setSnapshotState(null);
      return;
    }
    if (connection._tag === "None") {
      setSnapshotState(null);
      return;
    }

    const key = planUsageConnectionKey(connection, providerInstanceId);
    if (key === null) {
      setSnapshotState(null);
      return;
    }
    setSnapshotState(null);
    requestedDpopRefreshKeyRef.current = null;
    const requestUrl = new URL("/api/plan-usage", connection.value.httpBaseUrl);
    if (providerInstanceId) {
      requestUrl.searchParams.set("providerInstanceId", providerInstanceId);
    }
    const url = requestUrl.toString();
    let cancelled = false;
    const refreshDpopConnection = async () => {
      if (requestedDpopRefreshKeyRef.current === key) return;
      requestedDpopRefreshKeyRef.current = key;
      await retryEnvironment(connection.value.environmentId);
    };
    const load = async () => {
      try {
        if (planUsageDpopNeedsRefresh(connection, Date.now())) {
          await refreshDpopConnection();
          return;
        }
        const headers = await planUsageRequestHeaders(connection, url);
        if (headers === null) {
          if (!cancelled) setSnapshotState(null);
          return;
        }
        const credentials = planUsageRequestCredentials(connection);
        const response = await fetch(url, {
          headers,
          ...(credentials ? { credentials } : {}),
        });
        if (!response.ok) {
          if (!cancelled) setSnapshotState(null);
          if (response.status === 401 && connection.value.httpAuthorization?._tag === "Dpop") {
            await refreshDpopConnection();
          }
          return;
        }
        const decoded = decodePlanUsageSnapshot(await response.json());
        if (!cancelled) setSnapshotState({ key, snapshot: decoded });
      } catch {
        if (!cancelled) setSnapshotState(null);
      }
    };

    void load();
    const interval = window.setInterval(() => void load(), POLL_INTERVAL_MS);
    return () => {
      cancelled = true;
      window.clearInterval(interval);
    };
  }, [aggregateUsageStreamPendingOrAvailable, connection, providerInstanceId, retryEnvironment]);

  const currentKey = planUsageConnectionKey(connection, providerInstanceId);
  if (connection._tag !== "None" && providerInstanceId === null) {
    return (
      liveServerConfig?.planUsage ??
      (snapshotState && snapshotState.key === currentKey ? snapshotState.snapshot : null)
    );
  }
  return snapshotState && snapshotState.key === currentKey ? snapshotState.snapshot : null;
}
