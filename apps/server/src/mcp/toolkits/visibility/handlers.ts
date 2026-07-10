import {
  ExecutionEnvironmentDescriptor,
  IsoDateTime,
  type ServerProvider,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { HttpClient, HttpClientRequest, HttpClientResponse } from "effect/unstable/http";

import * as ServerEnvironment from "../../../environment/ServerEnvironment.ts";
import { ProviderRegistry } from "../../../provider/Services/ProviderRegistry.ts";
import * as McpPeerClient from "../../../subagents/McpPeerClient.ts";
import * as SubagentPeerHttp from "../../../subagents/SubagentPeerHttp.ts";
import * as SubagentPeerRegistry from "../../../subagents/SubagentPeerRegistry.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import {
  ListBackendsOutput,
  ListBackendsToolError,
  VisibilityToolkit,
  type BackendProvider,
  type BackendSummary,
  type ListBackendsOutput as ListBackendsOutputType,
} from "./tools.ts";

const PEER_INVENTORY_TIMEOUT = Duration.seconds(5);
const PEER_DESCRIPTOR_TIMEOUT = Duration.seconds(2);
const PEER_CLOSE_TIMEOUT = Duration.seconds(1);

const decodeListBackendsOutput = Schema.decodeUnknownEffect(ListBackendsOutput);

class PeerInventoryError extends Schema.TaggedErrorClass<PeerInventoryError>()(
  "PeerInventoryError",
  { message: Schema.String },
) {}

const providerLabel = (provider: ServerProvider): string =>
  provider.displayName ?? provider.instanceId;

const providerAvailable = (provider: ServerProvider): boolean =>
  provider.availability !== "unavailable" &&
  provider.enabled &&
  provider.installed &&
  (provider.status === "ready" || provider.status === "warning");

const providerSummary = (provider: ServerProvider): BackendProvider => ({
  instanceId: provider.instanceId,
  driver: provider.driver,
  label: providerLabel(provider),
  ...(provider.displayName !== undefined ? { displayName: provider.displayName } : {}),
  enabled: provider.enabled,
  installed: provider.installed,
  status: provider.status,
  availability: provider.availability ?? "available",
  available: providerAvailable(provider),
  models: provider.models.map((model) => ({ slug: model.slug, name: model.name })),
});

const toToolError = (error: unknown): ListBackendsToolError =>
  new ListBackendsToolError({
    message: error instanceof Error ? error.message : "Failed to list T3 Code backends.",
  });

const errorMessage = (error: unknown): string =>
  error instanceof Error ? error.message : String(error);

const closePeerSession = (session: McpPeerClient.McpPeerClientSession) =>
  session.close().pipe(Effect.timeout(PEER_CLOSE_TIMEOUT), Effect.ignore);

const fetchPeerInventory = (
  peer: SubagentPeerRegistry.SubagentPeer,
  httpClient: HttpClient.HttpClient,
) =>
  Effect.acquireUseRelease(
    McpPeerClient.connect(peer).pipe(Effect.provideService(HttpClient.HttpClient, httpClient)),
    (session) =>
      Effect.gen(function* () {
        const result = yield* session.callTool({ name: "t3_list_backends", arguments: {} });
        if (result.isError === true) {
          return yield* new PeerInventoryError({
            message: `Peer '${peer.alias}' rejected t3_list_backends.`,
          });
        }
        const decoded = yield* decodeListBackendsOutput(result.structuredContent);
        if (decoded.backends.length !== 1) {
          return yield* new PeerInventoryError({
            message: `Peer '${peer.alias}' returned ${decoded.backends.length} local backend rows; expected 1.`,
          });
        }
        return decoded.backends[0]!;
      }),
    closePeerSession,
  ).pipe(Effect.timeout(PEER_INVENTORY_TIMEOUT));

const fetchPeerDescriptor = (
  peer: SubagentPeerRegistry.SubagentPeer,
  httpClient: HttpClient.HttpClient,
) => {
  const request = HttpClientRequest.get(
    SubagentPeerHttp.environmentUrl(peer.httpBaseUrl, "/.well-known/t3/environment"),
  ).pipe(HttpClientRequest.setHeaders(SubagentPeerHttp.cloudflareAccessHeaders(peer.cfAccess)));
  return httpClient
    .execute(request)
    .pipe(
      Effect.flatMap(HttpClientResponse.filterStatusOk),
      Effect.flatMap(HttpClientResponse.schemaBodyJson(ExecutionEnvironmentDescriptor)),
      Effect.timeout(PEER_DESCRIPTOR_TIMEOUT),
    );
};

const probePeer = Effect.fn("VisibilityToolkit.probePeer")(function* (
  peer: SubagentPeerRegistry.SubagentPeer,
  peerRegistry: SubagentPeerRegistry.SubagentPeerRegistry["Service"],
  httpClient: HttpClient.HttpClient,
) {
  const inventory = yield* fetchPeerInventory(peer, httpClient).pipe(
    Effect.match({
      onFailure: (left) => ({ _tag: "Left", left }) as const,
      onSuccess: (right) => ({ _tag: "Right", right }) as const,
    }),
  );
  if (inventory._tag === "Right") {
    const seenAt = IsoDateTime.make(DateTime.formatIso(yield* DateTime.now));
    yield* peerRegistry.updateLastSeen(peer.alias, seenAt).pipe(
      Effect.catch((error) =>
        Effect.logWarning("failed to update peer inventory last-seen time", {
          alias: peer.alias,
          cause: error.message,
        }),
      ),
    );
    return {
      ...inventory.right,
      alias: peer.alias,
      lastSeenAt: seenAt,
    } satisfies BackendSummary;
  }

  const descriptor = yield* fetchPeerDescriptor(peer, httpClient).pipe(Effect.option);
  const knownDescriptor = Option.getOrUndefined(descriptor);
  return {
    alias: peer.alias,
    environmentId: knownDescriptor?.environmentId ?? peer.environmentId,
    label: knownDescriptor?.label ?? peer.alias,
    os: knownDescriptor?.platform.os ?? "unknown",
    status: knownDescriptor === undefined ? "offline" : "error",
    ...(peer.lastSeenAt === undefined ? {} : { lastSeenAt: peer.lastSeenAt }),
    error: errorMessage(inventory.left),
    providers: [],
  } satisfies BackendSummary;
});

const makeHandlers = Effect.fn("VisibilityToolkit.makeHandlers")(function* () {
  const providerRegistry = yield* ProviderRegistry;
  const serverEnvironment = yield* ServerEnvironment.ServerEnvironment;
  const peerRegistry = yield* SubagentPeerRegistry.SubagentPeerRegistry;
  const httpClient = yield* HttpClient.HttpClient;

  const listBackends = () =>
    Effect.gen(function* () {
      const invocation = yield* McpInvocationContext.requireAnyMcpCapability([
        "thread-management",
        "subagent:list",
      ]);
      const [providers, descriptor] = yield* Effect.all([
        providerRegistry.getProviders,
        serverEnvironment.getDescriptor,
      ]);
      const local = {
        alias: "local",
        environmentId: descriptor.environmentId,
        label: descriptor.label,
        os: descriptor.platform.os,
        status: "online",
        providers: providers.map(providerSummary),
      } satisfies BackendSummary;

      if (!McpInvocationContext.isProviderInvocationScope(invocation)) {
        return { backends: [local] } satisfies ListBackendsOutputType;
      }

      const peers = yield* peerRegistry.list;
      const remote = yield* Effect.forEach(
        peers,
        (peer) => probePeer(peer, peerRegistry, httpClient),
        { concurrency: 4 },
      );
      return { backends: [local, ...remote] } satisfies ListBackendsOutputType;
    }).pipe(Effect.mapError(toToolError));

  return {
    t3_list_backends: listBackends,
  } satisfies Parameters<typeof VisibilityToolkit.toLayer>[0];
});

export const VisibilityToolkitHandlersLive = Layer.unwrap(
  makeHandlers().pipe(Effect.map((handlers) => VisibilityToolkit.toLayer(handlers))),
);
