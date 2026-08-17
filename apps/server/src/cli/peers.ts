import {
  AuthAccessTokenResult,
  AuthAccessTokenType,
  AuthEnvironmentBootstrapTokenType,
  AuthStandardClientScopes,
  AuthTokenExchangeGrantType,
  ExecutionEnvironmentDescriptor,
} from "@t3tools/contracts";
import { encodeOAuthScope } from "@t3tools/shared/oauthScope";
import { resolveRemotePairingTarget } from "@t3tools/shared/remote";
import * as Console from "effect/Console";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as References from "effect/References";
import * as Schema from "effect/Schema";
import { Argument, Command, Flag, GlobalFlag } from "effect/unstable/cli";
import * as CliError from "effect/unstable/cli/CliError";
import {
  FetchHttpClient,
  HttpClient,
  HttpClientRequest,
  HttpClientResponse,
} from "effect/unstable/http";

import * as ServerConfig from "../config.ts";
import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as SubagentPeerRegistry from "../subagents/SubagentPeerRegistry.ts";
import {
  cloudflareAccessHeaders,
  environmentUrl,
  SUBAGENT_PEER_MCP_TOKEN_PATH,
  type SubagentPeerMcpTokenRequest,
  SubagentPeerMcpTokenResult,
} from "../subagents/SubagentPeerHttp.ts";
import { authLocationFlags, type CliAuthLocationFlags, resolveCliAuthConfig } from "./config.ts";

export class PeerCommandInputError extends CliError.UserError {
  override get message() {
    return typeof this.cause === "string" ? this.cause : "Invalid peer command input.";
  }
}

export class PeerPairingError extends Schema.TaggedErrorClass<PeerPairingError>()(
  "PeerPairingError",
  {
    operation: Schema.Literals([
      "resolve-pairing-target",
      "fetch-descriptor",
      "exchange-token",
      "mint-peer-token",
    ]),
    status: Schema.optional(Schema.Number),
    cause: Schema.Defect(),
  },
) {
  override get message(): string {
    const status = this.status === undefined ? "" : ` (HTTP ${this.status})`;
    return `Failed to ${this.operation} for subagent peer${status}.`;
  }
}

interface PairPeerInput {
  readonly alias: string;
  readonly pairingUrl?: string | undefined;
  readonly host?: string | undefined;
  readonly pairingCode?: string | undefined;
  readonly mcpEndpoint?: string | undefined;
  readonly cloudflareAccessToken?: string | undefined;
  readonly cloudflareAccessClientId?: string | undefined;
  readonly cloudflareAccessClientSecret?: string | undefined;
}

type PeerCloudflareAccess = SubagentPeerRegistry.SubagentPeerCloudflareAccess;

const PEER_PAIRING_REQUEST_TIMEOUT = Duration.seconds(5);
const PEER_PAIRING_FETCH_REQUEST_INIT = { redirect: "manual" } satisfies RequestInit;

const decodeSubagentPeerAlias = Schema.decodeUnknownEffect(SubagentPeerRegistry.SubagentPeerAlias);

const cloudflareAccessFromInput = Effect.fn("peers.cloudflareAccessFromInput")(function* (input: {
  readonly cloudflareAccessToken?: string | undefined;
  readonly cloudflareAccessClientId?: string | undefined;
  readonly cloudflareAccessClientSecret?: string | undefined;
}): Effect.fn.Return<PeerCloudflareAccess | undefined, PeerCommandInputError> {
  const token = input.cloudflareAccessToken?.trim() ?? "";
  const clientId = input.cloudflareAccessClientId?.trim() ?? "";
  const clientSecret = input.cloudflareAccessClientSecret?.trim() ?? "";
  if (clientId.length > 0 !== clientSecret.length > 0) {
    return yield* new PeerCommandInputError({
      cause: "--cf-access-client-id and --cf-access-client-secret must be provided together.",
    });
  }
  if (clientId.length > 0 && clientSecret.length > 0) {
    return { _tag: "service-token", clientId, clientSecret };
  }
  if (token.length > 0) {
    return { _tag: "jwt", jwt: token };
  }
  return undefined;
});

const isPeerPairingError = Schema.is(PeerPairingError);

const responseJson = <A>(
  request: HttpClientRequest.HttpClientRequest,
  schema: Schema.Decoder<A>,
  operation: PeerPairingError["operation"],
  timeout = PEER_PAIRING_REQUEST_TIMEOUT,
) =>
  Effect.gen(function* () {
    const httpClient = yield* HttpClient.HttpClient;
    const response = yield* httpClient.execute(request).pipe(
      Effect.mapError(
        (cause) =>
          new PeerPairingError({
            operation,
            cause,
          }),
      ),
    );
    const ok = yield* HttpClientResponse.filterStatusOk(response).pipe(
      Effect.mapError(
        (cause) =>
          new PeerPairingError({
            operation,
            status: response.status,
            cause,
          }),
      ),
    );
    return yield* HttpClientResponse.schemaBodyJson(schema)(ok).pipe(
      Effect.mapError((cause) =>
        isPeerPairingError(cause)
          ? cause
          : new PeerPairingError({
              operation,
              cause,
            }),
      ),
    );
  }).pipe(
    Effect.timeout(timeout),
    Effect.mapError((cause) =>
      isPeerPairingError(cause)
        ? cause
        : new PeerPairingError({
            operation,
            cause,
          }),
    ),
  );

const preflightAliasAvailable = Effect.fn("peers.preflightAliasAvailable")(function* (
  registry: SubagentPeerRegistry.SubagentPeerRegistry["Service"],
  alias: string,
) {
  const decodedAlias = yield* decodeSubagentPeerAlias(alias).pipe(
    Effect.mapError(
      (cause) =>
        new PeerCommandInputError({
          cause,
        }),
    ),
  );
  const existing = yield* registry.getByAlias(decodedAlias);
  if (Option.isSome(existing)) {
    return yield* new SubagentPeerRegistry.SubagentPeerAliasExistsError({ alias: decodedAlias });
  }
  return decodedAlias;
});

const normalizeMcpEndpointOverride = (
  mcpEndpoint: string | undefined,
): Effect.Effect<string | undefined, PeerCommandInputError> =>
  mcpEndpoint === undefined
    ? Effect.as(Effect.void, undefined as string | undefined)
    : Effect.try({
        try: () => new URL(mcpEndpoint).toString(),
        catch: (cause) =>
          new PeerCommandInputError({
            cause,
          }),
      });

export const pairPeer = Effect.fn("peers.pairPeer")(function* (
  registry: SubagentPeerRegistry.SubagentPeerRegistry["Service"],
  input: PairPeerInput,
) {
  const alias = yield* preflightAliasAvailable(registry, input.alias);
  const mcpEndpoint = yield* normalizeMcpEndpointOverride(input.mcpEndpoint);
  const pairingInput = {
    ...(input.pairingUrl !== undefined ? { pairingUrl: input.pairingUrl } : {}),
    ...(input.host !== undefined ? { host: input.host } : {}),
    ...(input.pairingCode !== undefined ? { pairingCode: input.pairingCode } : {}),
    ...(input.cloudflareAccessToken !== undefined
      ? { cloudflareAccessToken: input.cloudflareAccessToken }
      : {}),
    ...(input.cloudflareAccessClientId !== undefined
      ? { cloudflareAccessClientId: input.cloudflareAccessClientId }
      : {}),
    ...(input.cloudflareAccessClientSecret !== undefined
      ? { cloudflareAccessClientSecret: input.cloudflareAccessClientSecret }
      : {}),
  };
  const target = yield* Effect.try({
    try: () => resolveRemotePairingTarget(pairingInput),
    catch: (cause) =>
      new PeerPairingError({
        operation: "resolve-pairing-target",
        cause,
      }),
  });
  const cfAccess = yield* cloudflareAccessFromInput({
    cloudflareAccessToken: target.cloudflareAccessToken ?? input.cloudflareAccessToken,
    cloudflareAccessClientId: target.cloudflareAccessClientId ?? input.cloudflareAccessClientId,
    cloudflareAccessClientSecret:
      target.cloudflareAccessClientSecret ?? input.cloudflareAccessClientSecret,
  });
  const headers = cloudflareAccessHeaders(cfAccess);
  const descriptor = yield* responseJson(
    HttpClientRequest.get(environmentUrl(target.httpBaseUrl, "/.well-known/t3/environment")).pipe(
      HttpClientRequest.setHeaders(headers),
    ),
    ExecutionEnvironmentDescriptor,
    "fetch-descriptor",
  );
  const body = {
    grant_type: AuthTokenExchangeGrantType,
    subject_token: target.credential,
    subject_token_type: AuthEnvironmentBootstrapTokenType,
    requested_token_type: AuthAccessTokenType,
    audience_ceiling: "private",
    scope: encodeOAuthScope(AuthStandardClientScopes),
    client_label: `subagent-peer:${alias}`,
    client_device_type: "bot",
  } satisfies Record<string, string>;
  const access = yield* responseJson(
    HttpClientRequest.post(environmentUrl(target.httpBaseUrl, "/oauth/token")).pipe(
      HttpClientRequest.setHeaders(headers),
      HttpClientRequest.bodyUrlParams(body),
    ),
    AuthAccessTokenResult,
    "exchange-token",
  );
  const sourceEnvironment = yield* Effect.serviceOption(ServerEnvironment.ServerEnvironment);
  const sourceEnvironmentId = Option.isSome(sourceEnvironment)
    ? yield* sourceEnvironment.value.getEnvironmentId
    : undefined;
  const peerTokenBody = (
    sourceEnvironmentId !== undefined ? { sourceEnvironmentId } : {}
  ) satisfies SubagentPeerMcpTokenRequest;
  const peerToken = yield* responseJson(
    HttpClientRequest.post(environmentUrl(target.httpBaseUrl, SUBAGENT_PEER_MCP_TOKEN_PATH)).pipe(
      HttpClientRequest.setHeaders({
        ...headers,
        authorization: `${access.token_type} ${access.access_token}`,
      }),
      HttpClientRequest.bodyJsonUnsafe(peerTokenBody),
    ),
    SubagentPeerMcpTokenResult,
    "mint-peer-token",
  );

  return yield* registry.add({
    alias,
    environmentId: descriptor.environmentId,
    httpBaseUrl: target.httpBaseUrl,
    ...(mcpEndpoint !== undefined ? { mcpEndpoint } : {}),
    credential: new SubagentPeerRegistry.SubagentPeerBearerCredential({
      token: peerToken.token,
    }),
    ...(cfAccess ? { cfAccess } : {}),
  });
});

const jsonFlag = Flag.boolean("json").pipe(
  Flag.withDescription("Emit JSON instead of human-readable output."),
  Flag.withDefault(false),
);

const optionalStringFlag = (name: string, description: string) =>
  Flag.string(name).pipe(Flag.withDescription(description), Flag.optional);

const aliasArgument = Argument.string("alias").pipe(
  Argument.withDescription("Peer alias used by agents in `target`."),
);

const redactedPeer = (peer: SubagentPeerRegistry.SubagentPeer) => ({
  alias: peer.alias,
  environmentId: peer.environmentId,
  httpBaseUrl: peer.httpBaseUrl,
  mcpEndpoint: peer.mcpEndpoint,
  credential: { _tag: peer.credential._tag },
  cfAccess: peer.cfAccess ? { _tag: peer.cfAccess._tag } : undefined,
  pairedAt: peer.pairedAt,
  lastSeenAt: peer.lastSeenAt,
});

const formatPeerList = (
  peers: ReadonlyArray<SubagentPeerRegistry.SubagentPeer>,
  options: { readonly json: boolean },
): string => {
  if (options.json) {
    return JSON.stringify(peers.map(redactedPeer), null, 2);
  }
  if (peers.length === 0) {
    return "No subagent peers registered.";
  }
  return peers
    .map((peer) => {
      const lastSeen = peer.lastSeenAt ? `, last seen ${peer.lastSeenAt}` : "";
      return `${peer.alias}\t${peer.environmentId}\t${peer.httpBaseUrl}${lastSeen}`;
    })
    .join("\n");
};

const formatPeerAdded = (
  peer: SubagentPeerRegistry.SubagentPeer,
  options: { readonly json: boolean },
): string =>
  options.json
    ? JSON.stringify(redactedPeer(peer), null, 2)
    : `Registered subagent peer '${peer.alias}' (${peer.environmentId}).`;

const runWithPeerRegistry = <A, E>(
  flags: CliAuthLocationFlags,
  run: (
    registry: SubagentPeerRegistry.SubagentPeerRegistry["Service"],
  ) => Effect.Effect<A, E, HttpClient.HttpClient>,
  options?: {
    readonly quietLogs?: boolean;
  },
) =>
  Effect.gen(function* () {
    const logLevel = yield* GlobalFlag.LogLevel;
    const config = yield* resolveCliAuthConfig(flags, logLevel);
    const minimumLogLevel = options?.quietLogs ? "Error" : config.logLevel;
    const configLayer = ServerConfig.layer(config);
    const secretStoreLayer = ServerSecretStore.layer.pipe(Layer.provide(configLayer));
    return yield* Effect.gen(function* () {
      const registry = yield* SubagentPeerRegistry.SubagentPeerRegistry;
      return yield* run(registry);
    }).pipe(
      Effect.provide(
        Layer.mergeAll(
          SubagentPeerRegistry.layer.pipe(
            Layer.provide(secretStoreLayer),
            Layer.provide(configLayer),
          ),
          ServerEnvironment.layer.pipe(Layer.provide(secretStoreLayer), Layer.provide(configLayer)),
          FetchHttpClient.layer,
          Layer.succeed(FetchHttpClient.RequestInit, PEER_PAIRING_FETCH_REQUEST_INIT),
          Layer.succeed(References.MinimumLogLevel, minimumLogLevel),
        ),
      ),
    );
  });

const optionValue = <A>(option: Option.Option<A>): A | undefined => Option.getOrUndefined(option);

const peersAddCommand = Command.make("add", {
  ...authLocationFlags,
  alias: aliasArgument,
  pairingUrl: optionalStringFlag(
    "pairing-url",
    "Full peer pairing URL, usually copied from the target backend.",
  ),
  host: optionalStringFlag("host", "Peer HTTP(S) base URL when using --pairing-code."),
  pairingCode: optionalStringFlag("pairing-code", "One-time pairing token for --host."),
  bearer: optionalStringFlag("bearer", "Already-issued peer bearer token."),
  environmentId: optionalStringFlag("environment-id", "Peer environment id for --bearer."),
  httpBaseUrl: optionalStringFlag("http-base-url", "Peer HTTP base URL for --bearer."),
  mcpEndpoint: optionalStringFlag("mcp-endpoint", "Override peer MCP endpoint URL."),
  cfAccessToken: optionalStringFlag("cf-access-token", "Cloudflare Access JWT."),
  cfAccessClientId: optionalStringFlag(
    "cf-access-client-id",
    "Cloudflare Access service token client id.",
  ),
  cfAccessClientSecret: optionalStringFlag(
    "cf-access-client-secret",
    "Cloudflare Access service token client secret.",
  ),
  json: jsonFlag,
}).pipe(
  Command.withDescription("Register a subagent peer by pairing or explicit bearer credentials."),
  Command.withHandler((flags) =>
    runWithPeerRegistry(
      flags,
      (registry) =>
        Effect.gen(function* () {
          const bearer = optionValue(flags.bearer);
          const environmentId = optionValue(flags.environmentId);
          const httpBaseUrl = optionValue(flags.httpBaseUrl);
          const mcpEndpoint = optionValue(flags.mcpEndpoint);
          const pairingUrl = optionValue(flags.pairingUrl);
          const host = optionValue(flags.host);
          const pairingCode = optionValue(flags.pairingCode);
          const cfAccessToken = optionValue(flags.cfAccessToken);
          const cfAccessClientId = optionValue(flags.cfAccessClientId);
          const cfAccessClientSecret = optionValue(flags.cfAccessClientSecret);
          const cfAccess = yield* cloudflareAccessFromInput({
            cloudflareAccessToken: cfAccessToken,
            cloudflareAccessClientId: cfAccessClientId,
            cloudflareAccessClientSecret: cfAccessClientSecret,
          });
          const peer =
            bearer !== undefined
              ? yield* (() => {
                  if (environmentId === undefined || httpBaseUrl === undefined) {
                    return Effect.fail(
                      new PeerCommandInputError({
                        cause: "--bearer requires --environment-id and --http-base-url.",
                      }),
                    );
                  }
                  return registry.add({
                    alias: flags.alias,
                    environmentId,
                    httpBaseUrl,
                    ...(mcpEndpoint !== undefined ? { mcpEndpoint } : {}),
                    credential: new SubagentPeerRegistry.SubagentPeerBearerCredential({
                      token: bearer,
                    }),
                    ...(cfAccess ? { cfAccess } : {}),
                  });
                })()
              : yield* pairPeer(registry, {
                  alias: flags.alias,
                  ...(pairingUrl !== undefined ? { pairingUrl } : {}),
                  ...(host !== undefined ? { host } : {}),
                  ...(pairingCode !== undefined ? { pairingCode } : {}),
                  ...(mcpEndpoint !== undefined ? { mcpEndpoint } : {}),
                  ...(cfAccessToken !== undefined ? { cloudflareAccessToken: cfAccessToken } : {}),
                  ...(cfAccessClientId !== undefined
                    ? { cloudflareAccessClientId: cfAccessClientId }
                    : {}),
                  ...(cfAccessClientSecret !== undefined
                    ? { cloudflareAccessClientSecret: cfAccessClientSecret }
                    : {}),
                });
          yield* Console.log(formatPeerAdded(peer, { json: flags.json }));
        }),
      {
        quietLogs: flags.json,
      },
    ),
  ),
);

const peersListCommand = Command.make("list", {
  ...authLocationFlags,
  json: jsonFlag,
}).pipe(
  Command.withDescription("List registered subagent peers without revealing bearer tokens."),
  Command.withHandler((flags) =>
    runWithPeerRegistry(
      flags,
      (registry) =>
        Effect.gen(function* () {
          const peers = yield* registry.list;
          yield* Console.log(formatPeerList(peers, { json: flags.json }));
        }),
      {
        quietLogs: flags.json,
      },
    ),
  ),
);

const peersRemoveCommand = Command.make("remove", {
  ...authLocationFlags,
  alias: aliasArgument,
}).pipe(
  Command.withDescription("Remove a registered subagent peer."),
  Command.withHandler((flags) =>
    runWithPeerRegistry(flags, (registry) =>
      Effect.gen(function* () {
        yield* registry.remove(flags.alias);
        yield* Console.log(`Removed subagent peer '${flags.alias}'.`);
      }),
    ),
  ),
);

export const peersCommand = Command.make("peers").pipe(
  Command.withDescription("Manage cross-backend subagent peers."),
  Command.withSubcommands([peersAddCommand, peersListCommand, peersRemoveCommand]),
);

export const __testing = {
  cloudflareAccessHeaders,
  cloudflareAccessFromInput,
  formatPeerAdded,
  formatPeerList,
  peerPairingFetchRequestInit: PEER_PAIRING_FETCH_REQUEST_INIT,
  responseJson,
  redactedPeer,
};
