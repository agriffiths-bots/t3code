import type { OrchestrationThreadDetailSnapshot, ThreadId } from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import { HttpClient } from "effect/unstable/http";

import type { PreparedConnection } from "../connection/model.ts";
import { environmentEndpointUrl } from "../environment/endpoint.ts";
import { ManagedRelayDpopSigner } from "../relay/managedRelay.ts";
import {
  executeEnvironmentHttpRequest,
  makeEnvironmentHttpApiClient,
  type RemoteEnvironmentRequestError,
} from "../rpc/http.ts";
import { buildEnvironmentAuthHeaders, withEnvironmentCredentials } from "./environmentHttpAuth.ts";

// Bounded so a pathologically slow endpoint cannot block the (cheaper) socket
// fallback for long. The cached thread renders while this runs, so the wait only
// delays the transition to live data on the first open, not the initial paint.
const DEFAULT_THREAD_SNAPSHOT_TIMEOUT_MS = 6_000;

/**
 * Load a thread's detail snapshot over HTTP instead of embedding it in the
 * WebSocket subscription's first frame. The response is gzip-compressible by
 * the transport and keeps the (potentially multi-KB) snapshot off the socket.
 */
export const fetchEnvironmentThreadSnapshot = Effect.fn(
  "clientRuntime.state.fetchEnvironmentThreadSnapshot",
)(function* (input: {
  readonly prepared: PreparedConnection;
  readonly threadId: ThreadId;
  readonly signer: Option.Option<ManagedRelayDpopSigner["Service"]>;
  readonly timeoutMs?: number;
}) {
  const requestUrl = environmentEndpointUrl(
    input.prepared.httpBaseUrl,
    `/api/orchestration/threads/${input.threadId}`,
  );
  const client = yield* makeEnvironmentHttpApiClient(input.prepared.httpBaseUrl);
  const headers = yield* buildEnvironmentAuthHeaders(
    input.prepared.httpAuthorization,
    "GET",
    requestUrl,
    input.signer,
  );
  return yield* executeEnvironmentHttpRequest(
    requestUrl,
    input.timeoutMs ?? DEFAULT_THREAD_SNAPSHOT_TIMEOUT_MS,
    withEnvironmentCredentials(
      input.prepared.httpAuthorization,
      client.orchestration.threadSnapshot({
        params: { threadId: input.threadId },
        headers,
      }),
    ),
  );
});

export type FetchEnvironmentThreadSnapshotError = RemoteEnvironmentRequestError;

export type ThreadSnapshotLoadResult =
  | {
      readonly kind: "found";
      readonly snapshot: OrchestrationThreadDetailSnapshot;
    }
  | {
      readonly kind: "missing";
    }
  | {
      readonly kind: "unavailable";
    };

/**
 * Loads a thread's detail snapshot over HTTP. Initial loads use `load`, which
 * collapses missing/unavailable snapshots into `Option.none()` so callers can
 * fall back to the socket-embedded snapshot. Active reconciliation uses
 * `loadForReconcile`, which keeps missing snapshots distinct from transient HTTP
 * failures so an archived/deleted thread can be removed from active detail state.
 */
export class ThreadSnapshotLoader extends Context.Service<
  ThreadSnapshotLoader,
  {
    readonly load: (
      prepared: PreparedConnection,
      threadId: ThreadId,
    ) => Effect.Effect<Option.Option<OrchestrationThreadDetailSnapshot>>;
    readonly loadForReconcile: (
      prepared: PreparedConnection,
      threadId: ThreadId,
    ) => Effect.Effect<ThreadSnapshotLoadResult>;
  }
>()("@t3tools/client-runtime/state/threadSnapshotHttp/ThreadSnapshotLoader") {}

export const threadSnapshotLoaderLayer: Layer.Layer<
  ThreadSnapshotLoader,
  never,
  HttpClient.HttpClient
> = Layer.effect(
  ThreadSnapshotLoader,
  Effect.gen(function* () {
    const httpClient = yield* HttpClient.HttpClient;
    // Resolve the DPoP signer optionally: it is only needed for relay/DPoP
    // connections, so the loader must not hard-require it (bearer/primary
    // connections work without one).
    const signer = yield* Effect.serviceOption(ManagedRelayDpopSigner);
    const loadForReconcile = (prepared: PreparedConnection, threadId: ThreadId) =>
      fetchEnvironmentThreadSnapshot({ prepared, threadId, signer }).pipe(
        Effect.map(
          (snapshot): ThreadSnapshotLoadResult => ({
            kind: "found",
            snapshot,
          }),
        ),
        Effect.provideService(HttpClient.HttpClient, httpClient),
        Effect.catchTags({
          EnvironmentResourceNotFoundError: () =>
            Effect.logDebug("Thread snapshot not found over HTTP.").pipe(
              Effect.annotateLogs({ threadId }),
              Effect.as({ kind: "missing" } as const),
            ),
        }),
        Effect.catchCause((cause) =>
          Effect.logWarning("Could not load the thread snapshot over HTTP.").pipe(
            Effect.annotateLogs({ threadId, cause: Cause.pretty(cause) }),
            Effect.as({ kind: "unavailable" } as const),
          ),
        ),
      );
    return ThreadSnapshotLoader.of({
      load: (prepared: PreparedConnection, threadId: ThreadId) =>
        loadForReconcile(prepared, threadId).pipe(
          Effect.map((result) =>
            result.kind === "found"
              ? Option.some(result.snapshot)
              : Option.none<OrchestrationThreadDetailSnapshot>(),
          ),
        ),
      loadForReconcile,
    });
  }),
);
