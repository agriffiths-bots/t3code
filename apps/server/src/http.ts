import Mime from "@effect/platform-node/Mime";
import {
  type AuthSessionId,
  AuthOrchestrationOperateScope,
  AuthOrchestrationReadScope,
  EnvironmentHttpApi,
  ProviderInstanceId,
  ServerNotificationAckAction,
  ServerNotificationRecoveryInput,
} from "@t3tools/contracts";
import { decodeOtlpTraceRecords } from "@t3tools/shared/observability";
import * as Data from "effect/Data";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import { cast } from "effect/Function";
import * as Cookies from "effect/unstable/http/Cookies";
import {
  HttpBody,
  HttpClient,
  HttpClientResponse,
  HttpRouter,
  HttpServerResponse,
  HttpServerRequest,
  HttpServerRespondable,
} from "effect/unstable/http";
import * as HttpApiBuilder from "effect/unstable/httpapi/HttpApiBuilder";
import { OtlpTracer } from "effect/unstable/observability";

import * as ServerConfig from "./config.ts";
import {
  ASSET_ROUTE_PREFIX,
  ASSET_SURFACE_BIND_PATH,
  ASSET_SURFACE_CREDENTIAL_HEADER,
  ASSET_SURFACE_RELAY_PREFIX,
  assetSurfaceCookieName,
  assetSurfaceCookiePrefix,
  resolveAsset,
  verifyAssetSurfaceCredential,
} from "./assets/AssetAccess.ts";
import * as BrowserTraceCollector from "./observability/BrowserTraceCollector.ts";
import * as EnvironmentAuth from "./auth/EnvironmentAuth.ts";
import * as SessionStore from "./auth/SessionStore.ts";
import * as DeviceNotifications from "./notifications/DeviceNotifications.ts";
import * as McpSessionRegistry from "./mcp/McpSessionRegistry.ts";
import { traceRelayRequest } from "./cloud/traceRelayRequest.ts";
import {
  annotateEnvironmentRequest,
  failEnvironmentScopeRequired,
  failEnvironmentAuthInvalid,
  failEnvironmentInternal,
  requireCookieAuthCsrfOrigin,
  configuredCookieAuthCsrfOriginsEffect,
} from "./auth/http.ts";
import * as ServerEnvironment from "./environment/ServerEnvironment.ts";
import {
  browserApiCorsAllowedHeaders,
  browserApiCorsAllowedMethods,
  configuredBrowserCookieCredentialOrigins,
  normalizeCorsOrigin,
} from "./httpCors.ts";
import { buildElevenLabsRequest, resolveVoiceId, validateTtsText } from "./tts/ttsRequest.logic.ts";
import * as PlanUsageSnapshot from "./usage/PlanUsageSnapshot.ts";
import {
  SUBAGENT_PEER_MCP_TOKEN_PATH,
  SubagentPeerMcpTokenRequest,
  type SubagentPeerMcpTokenResult,
} from "./subagents/SubagentPeerHttp.ts";

const OTLP_TRACES_PROXY_PATH = "/api/observability/v1/traces";
const TTS_SPEAK_PATH = "/api/tts/speak";
const PLAN_USAGE_PATH = "/api/plan-usage";
const NOTIFICATION_ACK_PATH = "/api/notifications/ack";
const NOTIFICATION_RECOVERY_PATH = "/api/notifications/recover";
const LOOPBACK_HOSTNAMES = new Set(["127.0.0.1", "::1", "localhost"]);
const DESKTOP_RENDERER_ORIGINS = ["t3code://app", "t3code-dev://app"];
const BROWSER_API_CORS_MAX_AGE_SECONDS = 600;

function browserApiCredentialedCorsOrigins(config: ServerConfig.ServerConfig["Service"]) {
  const origins = new Set<string>(configuredBrowserCookieCredentialOrigins(config));
  const devOrigin = config.devUrl?.origin;
  if (devOrigin) {
    origins.add(devOrigin);
    for (const desktopOrigin of DESKTOP_RENDERER_ORIGINS) {
      origins.add(desktopOrigin);
    }
  }
  return origins;
}

function browserApiCorsHeaders(input: {
  readonly origin: string | undefined;
  readonly credentialedOrigins: ReadonlySet<string>;
  readonly preflight: boolean;
}) {
  const origin = normalizeCorsOrigin(input.origin);
  const credentialed = origin !== null && input.credentialedOrigins.has(origin);
  return {
    "access-control-allow-origin": credentialed ? origin : "*",
    ...(credentialed ? { "access-control-allow-credentials": "true", vary: "Origin" } : {}),
    ...(input.preflight
      ? {
          "access-control-allow-methods": browserApiCorsAllowedMethods.join(", "),
          "access-control-allow-headers": browserApiCorsAllowedHeaders.join(", "),
          "access-control-max-age": String(BROWSER_API_CORS_MAX_AGE_SECONDS),
        }
      : {}),
  };
}

export const browserApiCorsLayer = Layer.unwrap(
  Effect.gen(function* () {
    const config = yield* ServerConfig.ServerConfig;
    const credentialedOrigins = browserApiCredentialedCorsOrigins(config);
    return HttpRouter.middleware(
      <E, R>(httpApp: Effect.Effect<HttpServerResponse.HttpServerResponse, E, R>) =>
        Effect.flatMap(HttpServerRequest.HttpServerRequest, (request) => {
          const headers = browserApiCorsHeaders({
            origin: request.headers.origin,
            credentialedOrigins,
            preflight: request.method === "OPTIONS",
          });
          if (request.method === "OPTIONS") {
            return Effect.succeed(HttpServerResponse.empty({ status: 204, headers }));
          }
          return Effect.map(httpApp, (response) =>
            HttpServerResponse.setHeaders(response, headers),
          );
        }),
      { global: true },
    );
  }),
);

export function isLoopbackHostname(hostname: string): boolean {
  const normalizedHostname = hostname
    .trim()
    .toLowerCase()
    .replace(/^\[(.*)\]$/, "$1");
  return LOOPBACK_HOSTNAMES.has(normalizedHostname);
}

export function resolveDevRedirectUrl(devUrl: URL, requestUrl: URL): string {
  const redirectUrl = new URL(devUrl.toString());
  redirectUrl.pathname = requestUrl.pathname;
  redirectUrl.search = requestUrl.search;
  redirectUrl.hash = requestUrl.hash;
  return redirectUrl.toString();
}

const authenticateRawRequestWithScope = (
  request: HttpServerRequest.HttpServerRequest,
  scope: typeof AuthOrchestrationReadScope | typeof AuthOrchestrationOperateScope,
) =>
  Effect.gen(function* () {
    const serverAuth = yield* EnvironmentAuth.EnvironmentAuth;
    const trustedOrigins = yield* configuredCookieAuthCsrfOriginsEffect;
    const session = yield* serverAuth.authenticateHttpRequest(request).pipe(
      Effect.catchIf(EnvironmentAuth.isServerAuthCredentialError, (error) =>
        failEnvironmentAuthInvalid(EnvironmentAuth.serverAuthCredentialReason(error)),
      ),
      Effect.catchIf(EnvironmentAuth.isServerAuthInternalError, (error) =>
        failEnvironmentInternal("internal_error", error),
      ),
    );
    yield* requireCookieAuthCsrfOrigin({ request, session, trustedOrigins });
    if (!session.scopes.includes(scope)) {
      return yield* failEnvironmentScopeRequired(scope);
    }
    return session;
  });

const authenticateRawRouteWithScope = (
  scope: typeof AuthOrchestrationReadScope | typeof AuthOrchestrationOperateScope,
) =>
  Effect.gen(function* () {
    const request = yield* HttpServerRequest.HttpServerRequest;
    return yield* authenticateRawRequestWithScope(request, scope);
  });

const confirmRawRequestSessionActive = (
  request: HttpServerRequest.HttpServerRequest,
  sessionId: AuthSessionId,
) =>
  Effect.gen(function* () {
    const serverAuth = yield* EnvironmentAuth.EnvironmentAuth;
    return yield* serverAuth.confirmHttpRequestSessionActive(request, sessionId).pipe(
      Effect.catchIf(EnvironmentAuth.isServerAuthCredentialError, (error) =>
        failEnvironmentAuthInvalid(EnvironmentAuth.serverAuthCredentialReason(error)),
      ),
      Effect.catchIf(EnvironmentAuth.isServerAuthInternalError, (error) =>
        failEnvironmentInternal("internal_error", error),
      ),
    );
  });

export const serverEnvironmentHttpApiLayer = HttpApiBuilder.group(
  EnvironmentHttpApi,
  "metadata",
  Effect.fnUntraced(function* (handlers) {
    const serverEnvironment = yield* ServerEnvironment.ServerEnvironment;
    return handlers.handle(
      "descriptor",
      Effect.fn("environment.metadata.descriptor")(function* (args) {
        yield* annotateEnvironmentRequest(args.endpoint.name);
        return yield* serverEnvironment.getDescriptor;
      }, traceRelayRequest),
    );
  }),
);

class DecodeOtlpTraceRecordsError extends Data.TaggedError("DecodeOtlpTraceRecordsError")<{
  readonly cause: unknown;
  readonly bodyJson: OtlpTracer.TraceData;
}> {}

export const otlpTracesProxyRouteLayer = HttpRouter.add(
  "POST",
  OTLP_TRACES_PROXY_PATH,
  Effect.gen(function* () {
    yield* authenticateRawRouteWithScope(AuthOrchestrationOperateScope);
    const request = yield* HttpServerRequest.HttpServerRequest;
    const config = yield* ServerConfig.ServerConfig;
    const otlpTracesUrl = config.otlpTracesUrl;
    const browserTraceCollector = yield* BrowserTraceCollector.BrowserTraceCollector;
    const httpClient = yield* HttpClient.HttpClient;
    const bodyJson = cast<unknown, OtlpTracer.TraceData>(yield* request.json);

    yield* Effect.try({
      try: () => decodeOtlpTraceRecords(bodyJson),
      catch: (cause) => new DecodeOtlpTraceRecordsError({ cause, bodyJson }),
    }).pipe(
      Effect.flatMap((records) => browserTraceCollector.record(records)),
      Effect.catch((cause) =>
        Effect.logWarning("Failed to decode browser OTLP traces", {
          cause,
          bodyJson,
        }),
      ),
    );

    if (otlpTracesUrl === undefined) {
      return HttpServerResponse.empty({ status: 204 });
    }

    return yield* httpClient
      .post(otlpTracesUrl, {
        body: HttpBody.jsonUnsafe(bodyJson),
      })
      .pipe(
        Effect.flatMap(HttpClientResponse.filterStatusOk),
        Effect.as(HttpServerResponse.empty({ status: 204 })),
        Effect.tapError((cause) =>
          Effect.logWarning("Failed to export browser OTLP traces", {
            cause,
            otlpTracesUrl,
          }),
        ),
        Effect.orElseSucceed(() =>
          HttpServerResponse.text("Trace export failed.", { status: 502 }),
        ),
      );
  }).pipe(
    Effect.catchTags({
      EnvironmentAuthInvalidError: HttpServerRespondable.toResponse,
      EnvironmentInternalError: HttpServerRespondable.toResponse,
      EnvironmentScopeRequiredError: HttpServerRespondable.toResponse,
    }),
  ),
);

export const mcpPeerTokenRouteLayer = HttpRouter.add(
  "POST",
  SUBAGENT_PEER_MCP_TOKEN_PATH,
  Effect.gen(function* () {
    const request = yield* HttpServerRequest.HttpServerRequest;
    const session = yield* authenticateRawRequestWithScope(request, AuthOrchestrationOperateScope);
    const body = yield* request.json.pipe(
      Effect.flatMap(Schema.decodeUnknownEffect(SubagentPeerMcpTokenRequest)),
      Effect.orElseSucceed(() => ({}) as SubagentPeerMcpTokenRequest),
    );
    if (body.sourceEnvironmentId === undefined) {
      return HttpServerResponse.jsonUnsafe(
        {
          error: "invalid_request",
          message: "sourceEnvironmentId is required when minting a subagent peer token.",
        },
        { status: 400 },
      );
    }
    const issued = yield* McpSessionRegistry.issueActiveMcpPeerCredential({
      sourceSessionId: session.sessionId,
      sourceEnvironmentId: body.sourceEnvironmentId,
      ...(session.expiresAt ? { expiresAt: session.expiresAt.epochMilliseconds } : {}),
    }).pipe(
      Effect.catchTag("McpPeerTokenStoreError", (error) =>
        failEnvironmentInternal("internal_error", error),
      ),
    );
    if (issued === undefined) {
      return yield* failEnvironmentInternal("internal_error");
    }
    const revokeIssuedPeerToken = McpSessionRegistry.revokeActiveMcpPeerCredential(
      issued.peerTokenId,
    ).pipe(
      Effect.catchTag("McpPeerTokenStoreError", (error) =>
        failEnvironmentInternal("internal_error", error),
      ),
    );
    const confirmedSession = yield* confirmRawRequestSessionActive(request, session.sessionId).pipe(
      Effect.catch((error) => revokeIssuedPeerToken.pipe(Effect.andThen(Effect.fail(error)))),
    );
    if (confirmedSession.sessionId !== session.sessionId) {
      yield* revokeIssuedPeerToken;
      return yield* failEnvironmentAuthInvalid("invalid_credential");
    }
    const response: SubagentPeerMcpTokenResult = {
      peerTokenId: issued.peerTokenId,
      token: issued.token,
      authorizationHeader: issued.authorizationHeader,
      issuedAt: issued.issuedAt,
      capabilities: issued.capabilities,
    };
    return HttpServerResponse.jsonUnsafe(response, {
      headers: { "cache-control": "no-store" },
    });
  }).pipe(
    Effect.catchTags({
      EnvironmentAuthInvalidError: HttpServerRespondable.toResponse,
      EnvironmentInternalError: HttpServerRespondable.toResponse,
      EnvironmentScopeRequiredError: HttpServerRespondable.toResponse,
    }),
  ),
);

class TtsConfigMissingError extends Data.TaggedError("TtsConfigMissingError")<{}> {}

class TtsTextInvalidError extends Data.TaggedError("TtsTextInvalidError")<{
  readonly reason: "empty" | "too_long";
}> {}

class TtsUpstreamError extends Data.TaggedError("TtsUpstreamError")<{
  readonly status: number;
}> {}

const TtsSpeakRequest = Schema.Struct({
  text: Schema.String,
  voiceId: Schema.optional(Schema.String),
});

const decodeTtsSpeakRequest = Schema.decodeUnknownEffect(TtsSpeakRequest);
const emptyTtsSpeakRequest: typeof TtsSpeakRequest.Type = { text: "" };

const NotificationAckRequest = Schema.Struct({
  notificationId: Schema.String,
  ackToken: Schema.String,
  action: ServerNotificationAckAction,
});
const decodeNotificationAckRequest = Schema.decodeUnknownEffect(NotificationAckRequest);
const decodeNotificationRecoveryRequest = Schema.decodeUnknownEffect(
  ServerNotificationRecoveryInput,
);

export const ttsSpeakHandler = Effect.gen(function* () {
  yield* authenticateRawRouteWithScope(AuthOrchestrationOperateScope);
  const request = yield* HttpServerRequest.HttpServerRequest;
  const httpClient = yield* HttpClient.HttpClient;

  const apiKey = process.env.ELEVENLABS_API_KEY;
  const defaultVoiceId = process.env.ELEVENLABS_VOICE_ID;
  if (!apiKey) {
    return yield* new TtsConfigMissingError();
  }

  const body = yield* request.json;
  const raw = yield* decodeTtsSpeakRequest(body).pipe(
    Effect.orElseSucceed(() => emptyTtsSpeakRequest),
  );

  const validated = validateTtsText(raw.text);
  if (!validated.ok) {
    return yield* new TtsTextInvalidError({ reason: validated.reason });
  }

  const voiceId = resolveVoiceId(raw.voiceId, defaultVoiceId);
  if (!voiceId) {
    return yield* new TtsConfigMissingError();
  }

  const upstream = buildElevenLabsRequest({ text: validated.text, voiceId, apiKey });
  const response = yield* httpClient
    .post(upstream.url, {
      headers: upstream.headers,
      body: HttpBody.jsonUnsafe(upstream.body),
    })
    .pipe(
      Effect.flatMap(HttpClientResponse.filterStatusOk),
      Effect.catchTag(
        "HttpClientError",
        (cause) => new TtsUpstreamError({ status: cause.response?.status ?? 502 }),
      ),
    );

  return HttpServerResponse.stream(response.stream, { contentType: "audio/mpeg" });
}).pipe(
  Effect.catchTags({
    EnvironmentAuthInvalidError: HttpServerRespondable.toResponse,
    EnvironmentInternalError: HttpServerRespondable.toResponse,
    EnvironmentScopeRequiredError: HttpServerRespondable.toResponse,
    TtsConfigMissingError: () =>
      Effect.succeed(HttpServerResponse.text("Text-to-speech is not configured.", { status: 503 })),
    TtsTextInvalidError: (cause) =>
      Effect.succeed(
        HttpServerResponse.text(
          cause.reason === "too_long" ? "Text too long for dictation." : "No text to dictate.",
          { status: 400 },
        ),
      ),
    TtsUpstreamError: (cause) =>
      Effect.logWarning("ElevenLabs TTS upstream failed", { status: cause.status }).pipe(
        Effect.as(HttpServerResponse.text("Dictation upstream failed.", { status: 502 })),
      ),
  }),
);

export const ttsSpeakRouteLayer = HttpRouter.add("POST", TTS_SPEAK_PATH, ttsSpeakHandler);

export const notificationAckRouteLayer = HttpRouter.add(
  "POST",
  NOTIFICATION_ACK_PATH,
  Effect.gen(function* () {
    const request = yield* HttpServerRequest.HttpServerRequest;
    const raw = yield* request.json;
    const input = yield* decodeNotificationAckRequest(raw).pipe(
      Effect.mapError(() => "bad_request" as const),
    );
    const notifications = yield* DeviceNotifications.DeviceNotifications;
    const result = yield* notifications.ackNotification(input, { requireAckToken: true });
    return HttpServerResponse.jsonUnsafe(result, {
      headers: {
        "Cache-Control": "no-store",
        "X-Content-Type-Options": "nosniff",
      },
    });
  }).pipe(
    Effect.catch((cause) =>
      cause === "bad_request"
        ? Effect.succeed(HttpServerResponse.text("Bad Request", { status: 400 }))
        : Effect.logWarning("Notification acknowledgement failed", { cause }).pipe(
            Effect.as(HttpServerResponse.text("Internal Server Error", { status: 500 })),
          ),
    ),
  ),
);

export const notificationRecoveryRouteLayer = HttpRouter.add(
  "POST",
  NOTIFICATION_RECOVERY_PATH,
  Effect.gen(function* () {
    const request = yield* HttpServerRequest.HttpServerRequest;
    const raw = yield* request.json.pipe(Effect.mapError(() => "not_found" as const));
    const input = yield* decodeNotificationRecoveryRequest(raw).pipe(
      Effect.mapError(() => "not_found" as const),
    );
    const notifications = yield* DeviceNotifications.DeviceNotifications;
    const result = yield* notifications
      .recoverSubscription(input)
      .pipe(Effect.catchTag("ServerNotificationEndpointError", () => Effect.succeed(null)));
    if (result === null) {
      return HttpServerResponse.text("Not Found", { status: 404 });
    }
    return HttpServerResponse.jsonUnsafe(result, {
      headers: {
        "Cache-Control": "no-store",
        "X-Content-Type-Options": "nosniff",
      },
    });
  }).pipe(
    Effect.catch((cause) =>
      cause === "not_found"
        ? Effect.succeed(HttpServerResponse.text("Not Found", { status: 404 }))
        : Effect.logWarning("Push subscription recovery failed", { cause }).pipe(
            Effect.as(HttpServerResponse.text("Internal Server Error", { status: 500 })),
          ),
    ),
  ),
);

export const planUsageRouteLayer = HttpRouter.add(
  "GET",
  PLAN_USAGE_PATH,
  Effect.gen(function* () {
    const session = yield* authenticateRawRouteWithScope(AuthOrchestrationReadScope);
    if (session.audienceCeiling === "factory") {
      return yield* failEnvironmentScopeRequired(AuthOrchestrationReadScope);
    }
    const request = yield* HttpServerRequest.HttpServerRequest;
    const url = HttpServerRequest.toURL(request);
    if (Option.isNone(url)) {
      return HttpServerResponse.text("Bad Request", { status: 400 });
    }

    const decodeProviderInstanceId = Schema.decodeUnknownOption(ProviderInstanceId);
    const rawProviderInstanceId = url.value.searchParams.get("providerInstanceId");
    const decodedProviderInstanceId = rawProviderInstanceId
      ? decodeProviderInstanceId(rawProviderInstanceId)
      : Option.none();
    if (rawProviderInstanceId && Option.isNone(decodedProviderInstanceId)) {
      return HttpServerResponse.text("Bad Request", { status: 400 });
    }
    const providerInstanceId = rawProviderInstanceId
      ? Option.getOrNull(decodedProviderInstanceId)
      : null;
    const planUsage = yield* PlanUsageSnapshot.PlanUsageSnapshotStore;
    const snapshot = yield* planUsage.read(providerInstanceId);

    return HttpServerResponse.jsonUnsafe(snapshot, {
      headers: {
        "Cache-Control": "private, max-age=30",
        "X-Content-Type-Options": "nosniff",
      },
    });
  }).pipe(
    Effect.catchTags({
      EnvironmentAuthInvalidError: HttpServerRespondable.toResponse,
      EnvironmentInternalError: HttpServerRespondable.toResponse,
      EnvironmentScopeRequiredError: HttpServerRespondable.toResponse,
    }),
  ),
);

export const assetRouteLayer = HttpRouter.add(
  "GET",
  `${ASSET_ROUTE_PREFIX}/*`,
  Effect.gen(function* () {
    const request = yield* HttpServerRequest.HttpServerRequest;
    const url = HttpServerRequest.toURL(request);
    if (Option.isNone(url)) {
      return HttpServerResponse.text("Bad Request", { status: 400 });
    }

    const isSurfaceRelay = url.value.pathname.startsWith(`${ASSET_SURFACE_RELAY_PREFIX}/`);
    const routePrefix = isSurfaceRelay ? ASSET_SURFACE_RELAY_PREFIX : ASSET_ROUTE_PREFIX;
    const suffix = url.value.pathname.slice(`${routePrefix}/`.length);
    const separatorIndex = suffix.indexOf("/");
    if (separatorIndex <= 0) {
      return HttpServerResponse.text("Not Found", { status: 404 });
    }

    const explicitSurfaceCredential = request.headers[ASSET_SURFACE_CREDENTIAL_HEADER];
    const requestSurfaceCredentials =
      explicitSurfaceCredential === undefined ? [] : [explicitSurfaceCredential];
    if (isSurfaceRelay) {
      const sessions = yield* SessionStore.SessionStore;
      const surfaceCookiePrefix = assetSurfaceCookiePrefix(sessions.cookieName);
      requestSurfaceCredentials.push(
        ...Object.entries(request.cookies)
          .filter(([name]) => name.startsWith(surfaceCookiePrefix))
          .map(([, credential]) => credential),
      );
    }
    const serverAuth = yield* EnvironmentAuth.EnvironmentAuth;
    const authenticatedSession = yield* serverAuth
      .authenticateHttpRequest(request)
      .pipe(
        Effect.option,
        Effect.map(Option.filter((session) => session.scopes.includes(AuthOrchestrationReadScope))),
      );

    const asset = yield* resolveAsset(
      suffix.slice(0, separatorIndex),
      suffix.slice(separatorIndex + 1),
      {
        ...(Option.isSome(authenticatedSession)
          ? { sessionId: authenticatedSession.value.sessionId }
          : {}),
        surfaceCredentials: requestSurfaceCredentials,
      },
    );
    if (!asset) {
      if (isSurfaceRelay) {
        yield* Effect.logWarning("Asset surface request was masked as not found.", {
          "asset.outcome": "masked_not_found",
          "asset.session_proof_present":
            Option.isSome(authenticatedSession) || requestSurfaceCredentials.length > 0,
        });
      }
      return HttpServerResponse.text("Not Found", { status: 404 });
    }
    return yield* HttpServerResponse.file(asset.path, {
      status: 200,
      headers: {
        // Session-bound assets must be revalidated on every fetch. A private browser cache can
        // outlive or switch sessions and would otherwise bypass the masking-404 proof check.
        "Cache-Control": "no-store",
        "X-Content-Type-Options": "nosniff",
      },
    }).pipe(
      Effect.orElseSucceed(() => HttpServerResponse.text("Internal Server Error", { status: 500 })),
    );
  }),
);

const AssetSurfaceBindingInput = Schema.Struct({
  credential: Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(4096)),
  redirect: Schema.optionalKey(Schema.String.check(Schema.isMaxLength(4096))),
});
const decodeAssetSurfaceBindingInput = Schema.decodeUnknownOption(AssetSurfaceBindingInput);

function validateAssetSurfaceRedirect(value: string): string | null {
  if (!value.startsWith("/") || value.startsWith("//")) return null;
  let url: URL;
  try {
    url = new URL(value, "http://asset.invalid");
  } catch {
    return null;
  }
  const prefix = `${ASSET_SURFACE_RELAY_PREFIX}/`;
  if (!url.pathname.startsWith(prefix) || url.pathname === ASSET_SURFACE_BIND_PATH) return null;
  const suffix = url.pathname.slice(prefix.length);
  const separatorIndex = suffix.indexOf("/");
  if (separatorIndex <= 0 || separatorIndex === suffix.length - 1) return null;
  return `${url.pathname}${url.search}`;
}

export const assetSurfaceBindingRouteLayer = HttpRouter.add(
  "POST",
  ASSET_SURFACE_BIND_PATH,
  Effect.gen(function* () {
    const request = yield* HttpServerRequest.HttpServerRequest;
    const input = yield* request.json.pipe(
      Effect.map(decodeAssetSurfaceBindingInput),
      Effect.orElseSucceed(() => Option.none()),
    );
    if (Option.isNone(input)) {
      return HttpServerResponse.text("Not Found", { status: 404 });
    }
    const redirect =
      input.value.redirect === undefined
        ? null
        : validateAssetSurfaceRedirect(input.value.redirect);
    if (input.value.redirect !== undefined && redirect === null) {
      return HttpServerResponse.text("Not Found", { status: 404 });
    }

    const verified = yield* verifyAssetSurfaceCredential(input.value.credential);
    if (verified === null) {
      return HttpServerResponse.text("Not Found", { status: 404 });
    }

    const sessions = yield* SessionStore.SessionStore;
    const requestOrigin = normalizeCorsOrigin(request.headers.origin);
    const forwardedProtocol = request.headers["x-forwarded-proto"]
      ?.split(",", 1)[0]
      ?.trim()
      .toLowerCase();
    const secure =
      (requestOrigin !== null && new URL(requestOrigin).protocol === "https:") ||
      forwardedProtocol === "https:" ||
      forwardedProtocol === "https" ||
      (() => {
        try {
          return new URL(request.originalUrl).protocol === "https:";
        } catch {
          return false;
        }
      })();
    const cookies = yield* Effect.fromResult(
      Cookies.set(
        Cookies.empty,
        assetSurfaceCookieName(sessions.cookieName, verified.surfaceBindingId),
        input.value.credential,
        {
          expires: DateTime.toDate(DateTime.makeUnsafe(verified.expiresAt)),
          httpOnly: true,
          path: ASSET_SURFACE_RELAY_PREFIX,
          sameSite: "lax",
          secure,
        },
      ),
    ).pipe(Effect.orDie);
    const response =
      redirect === null
        ? HttpServerResponse.empty({
            status: 204,
            headers: { "Cache-Control": "no-store" },
          })
        : HttpServerResponse.redirect(redirect, {
            status: 303,
            headers: { "Cache-Control": "no-store" },
          });
    return HttpServerResponse.mergeCookies(response, cookies);
  }),
);

export const staticAndDevRouteLayer = HttpRouter.add(
  "GET",
  "*",
  Effect.gen(function* () {
    const request = yield* HttpServerRequest.HttpServerRequest;
    const url = HttpServerRequest.toURL(request);

    if (Option.isNone(url)) {
      return HttpServerResponse.text("Bad Request", { status: 400 });
    }

    const config = yield* ServerConfig.ServerConfig;
    if (config.devUrl && isLoopbackHostname(url.value.hostname)) {
      return HttpServerResponse.redirect(resolveDevRedirectUrl(config.devUrl, url.value), {
        status: 302,
      });
    }

    const staticDir =
      config.staticDir ?? (config.devUrl ? yield* ServerConfig.resolveStaticDir() : undefined);
    if (!staticDir) {
      return HttpServerResponse.text("No static directory configured and no dev URL set.", {
        status: 503,
      });
    }

    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const staticRoot = path.resolve(staticDir);
    const staticRequestPath = url.value.pathname === "/" ? "/index.html" : url.value.pathname;
    const rawStaticRelativePath = staticRequestPath.replace(/^[/\\]+/, "");
    const hasRawLeadingParentSegment = rawStaticRelativePath.startsWith("..");
    const staticRelativePath = path.normalize(rawStaticRelativePath).replace(/^[/\\]+/, "");
    const hasPathTraversalSegment = staticRelativePath.startsWith("..");
    if (
      staticRelativePath.length === 0 ||
      hasRawLeadingParentSegment ||
      hasPathTraversalSegment ||
      staticRelativePath.includes("\0")
    ) {
      return HttpServerResponse.text("Invalid static file path", { status: 400 });
    }

    const isWithinStaticRoot = (candidate: string) =>
      candidate === staticRoot ||
      candidate.startsWith(staticRoot.endsWith(path.sep) ? staticRoot : `${staticRoot}${path.sep}`);

    let filePath = path.resolve(staticRoot, staticRelativePath);
    if (!isWithinStaticRoot(filePath)) {
      return HttpServerResponse.text("Invalid static file path", { status: 400 });
    }

    const ext = path.extname(filePath);
    if (!ext) {
      filePath = path.resolve(filePath, "index.html");
      if (!isWithinStaticRoot(filePath)) {
        return HttpServerResponse.text("Invalid static file path", { status: 400 });
      }
    }

    const fileInfo = yield* fileSystem.stat(filePath).pipe(Effect.orElseSucceed(() => null));
    if (!fileInfo || fileInfo.type !== "File") {
      const indexPath = path.resolve(staticRoot, "index.html");
      const indexData = yield* fileSystem
        .readFile(indexPath)
        .pipe(Effect.orElseSucceed(() => null));
      if (!indexData) {
        return HttpServerResponse.text("Not Found", { status: 404 });
      }
      return HttpServerResponse.uint8Array(indexData, {
        status: 200,
        contentType: "text/html; charset=utf-8",
      });
    }

    const contentType = Mime.getType(filePath) ?? "application/octet-stream";
    const data = yield* fileSystem.readFile(filePath).pipe(Effect.orElseSucceed(() => null));
    if (!data) {
      return HttpServerResponse.text("Internal Server Error", { status: 500 });
    }

    return HttpServerResponse.uint8Array(data, {
      status: 200,
      contentType,
    });
  }),
);
