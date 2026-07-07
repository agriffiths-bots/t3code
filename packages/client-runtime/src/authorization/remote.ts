import {
  AuthAccessTokenType,
  type AuthClientPresentationMetadata,
  AuthEnvironmentBootstrapTokenType,
  AuthTokenExchangeGrantType,
  type AuthEnvironmentScope,
} from "@t3tools/contracts";
import { encodeOAuthScope } from "@t3tools/shared/oauthScope";
import * as Effect from "effect/Effect";
import { FetchHttpClient } from "effect/unstable/http";
import { environmentEndpointUrl } from "../environment/endpoint.ts";
import {
  executeEnvironmentHttpRequest,
  makeEnvironmentHttpApiClient,
  type RemoteEnvironmentRequestError,
} from "../rpc/http.ts";

export {
  RemoteEnvironmentAccessRejectedError,
  RemoteEnvironmentAuthFetchError,
  RemoteEnvironmentAuthInvalidJsonError,
  RemoteEnvironmentAuthTimeoutError,
  RemoteEnvironmentAuthUndeclaredStatusError,
} from "../rpc/http.ts";
export type RemoteEnvironmentAuthError = RemoteEnvironmentRequestError;

const DEFAULT_REMOTE_REQUEST_TIMEOUT_MS = 10_000;

export type CloudflareAccessAuthorization =
  | {
      readonly _tag?: "jwt";
      readonly jwt: string;
    }
  | {
      readonly _tag: "cookie";
      readonly cookieValue: string;
    }
  | {
      readonly _tag: "service-token";
      readonly clientId: string;
      readonly clientSecret: string;
    };

export const cloudflareAccessHeaders = (
  authorization: CloudflareAccessAuthorization | undefined,
): Record<string, string> => {
  if (authorization === undefined) {
    return {};
  }
  if (authorization._tag === "service-token") {
    const clientId = authorization.clientId.trim();
    const clientSecret = authorization.clientSecret.trim();
    if (clientId.length === 0 || clientSecret.length === 0) return {};
    return {
      "cf-access-client-id": clientId,
      "cf-access-client-secret": clientSecret,
    };
  }

  const jwt =
    authorization._tag === "cookie" ? authorization.cookieValue.trim() : authorization.jwt.trim();
  if (jwt.length === 0) return {};
  return {
    "cf-access-jwt-assertion": jwt,
    cookie: `CF_Authorization=${jwt}`,
  };
};

const clientMetadataTokenExchangeFields = (
  clientMetadata: AuthClientPresentationMetadata | undefined,
) => ({
  ...(clientMetadata?.label ? { client_label: clientMetadata.label } : {}),
  ...(clientMetadata?.deviceType ? { client_device_type: clientMetadata.deviceType } : {}),
  ...(clientMetadata?.os ? { client_os: clientMetadata.os } : {}),
});

export const exchangeRemoteDpopAccessToken = Effect.fn(
  "clientRuntime.authorization.exchangeRemoteDpopAccessToken",
)(function* (input: {
  readonly httpBaseUrl: string;
  readonly credential: string;
  readonly scopes?: ReadonlyArray<AuthEnvironmentScope>;
  readonly clientMetadata?: AuthClientPresentationMetadata;
  readonly dpopProof: string;
  readonly cloudflareAccess?: CloudflareAccessAuthorization;
  readonly timeoutMs?: number;
}) {
  const client = yield* makeEnvironmentHttpApiClient(input.httpBaseUrl);
  const response = yield* executeEnvironmentHttpRequest(
    environmentEndpointUrl(input.httpBaseUrl, "/oauth/token"),
    input.timeoutMs ?? DEFAULT_REMOTE_REQUEST_TIMEOUT_MS,
    client.auth.token({
      headers: { ...cloudflareAccessHeaders(input.cloudflareAccess), dpop: input.dpopProof },
      payload: {
        grant_type: AuthTokenExchangeGrantType,
        subject_token: input.credential,
        subject_token_type: AuthEnvironmentBootstrapTokenType,
        requested_token_type: AuthAccessTokenType,
        ...(input.scopes ? { scope: encodeOAuthScope(input.scopes) } : {}),
        ...clientMetadataTokenExchangeFields(input.clientMetadata),
      },
    }),
  );
  return response;
});

export const bootstrapRemoteBearerSession = Effect.fn(
  "clientRuntime.authorization.bootstrapRemoteBearerSession",
)(function* (input: {
  readonly httpBaseUrl: string;
  readonly credential: string;
  readonly scopes?: ReadonlyArray<AuthEnvironmentScope>;
  readonly clientMetadata?: AuthClientPresentationMetadata;
  readonly cloudflareAccess?: CloudflareAccessAuthorization;
  readonly timeoutMs?: number;
}) {
  const client = yield* makeEnvironmentHttpApiClient(input.httpBaseUrl);
  return yield* executeEnvironmentHttpRequest(
    environmentEndpointUrl(input.httpBaseUrl, "/oauth/token"),
    input.timeoutMs ?? DEFAULT_REMOTE_REQUEST_TIMEOUT_MS,
    client.auth.token({
      headers: cloudflareAccessHeaders(input.cloudflareAccess),
      payload: {
        grant_type: AuthTokenExchangeGrantType,
        subject_token: input.credential,
        subject_token_type: AuthEnvironmentBootstrapTokenType,
        requested_token_type: AuthAccessTokenType,
        ...(input.scopes ? { scope: encodeOAuthScope(input.scopes) } : {}),
        ...clientMetadataTokenExchangeFields(input.clientMetadata),
      },
    }),
  );
});

export const fetchRemoteSessionState = Effect.fn(
  "clientRuntime.authorization.fetchRemoteSessionState",
)(function* (input: {
  readonly httpBaseUrl: string;
  readonly bearerToken: string;
  readonly cloudflareAccess?: CloudflareAccessAuthorization;
  readonly timeoutMs?: number;
}) {
  const client = yield* makeEnvironmentHttpApiClient(input.httpBaseUrl);
  return yield* executeEnvironmentHttpRequest(
    environmentEndpointUrl(input.httpBaseUrl, "/api/auth/session"),
    input.timeoutMs ?? DEFAULT_REMOTE_REQUEST_TIMEOUT_MS,
    client.auth.session({
      headers: {
        ...cloudflareAccessHeaders(input.cloudflareAccess),
        authorization: `Bearer ${input.bearerToken}`,
      },
    }),
  );
});

export const fetchRemoteDpopSessionState = Effect.fn(
  "clientRuntime.authorization.fetchRemoteDpopSessionState",
)(function* (input: {
  readonly httpBaseUrl: string;
  readonly accessToken: string;
  readonly dpopProof: string;
  readonly cloudflareAccess?: CloudflareAccessAuthorization;
  readonly timeoutMs?: number;
}) {
  const client = yield* makeEnvironmentHttpApiClient(input.httpBaseUrl);
  return yield* executeEnvironmentHttpRequest(
    environmentEndpointUrl(input.httpBaseUrl, "/api/auth/session"),
    input.timeoutMs ?? DEFAULT_REMOTE_REQUEST_TIMEOUT_MS,
    client.auth.session({
      headers: {
        ...cloudflareAccessHeaders(input.cloudflareAccess),
        authorization: `DPoP ${input.accessToken}`,
        dpop: input.dpopProof,
      },
    }),
  );
});

export const issueRemoteWebSocketTicket = Effect.fn(
  "clientRuntime.authorization.issueRemoteWebSocketTicket",
)(function* (input: {
  readonly httpBaseUrl: string;
  readonly bearerToken: string;
  readonly cloudflareAccess?: CloudflareAccessAuthorization;
  readonly timeoutMs?: number;
}) {
  const client = yield* makeEnvironmentHttpApiClient(input.httpBaseUrl);
  return yield* executeEnvironmentHttpRequest(
    environmentEndpointUrl(input.httpBaseUrl, "/api/auth/websocket-ticket"),
    input.timeoutMs ?? DEFAULT_REMOTE_REQUEST_TIMEOUT_MS,
    client.auth.webSocketTicket({
      headers: {
        ...cloudflareAccessHeaders(input.cloudflareAccess),
        authorization: `Bearer ${input.bearerToken}`,
      },
    }),
  );
});

export const issueBrowserSessionWebSocketTicket = Effect.fn(
  "clientRuntime.authorization.issueBrowserSessionWebSocketTicket",
)(function* (input: {
  readonly httpBaseUrl: string;
  readonly cloudflareAccess?: CloudflareAccessAuthorization;
  readonly timeoutMs?: number;
}) {
  const client = yield* makeEnvironmentHttpApiClient(input.httpBaseUrl);
  return yield* executeEnvironmentHttpRequest(
    environmentEndpointUrl(input.httpBaseUrl, "/api/auth/websocket-ticket"),
    input.timeoutMs ?? DEFAULT_REMOTE_REQUEST_TIMEOUT_MS,
    client.auth
      .webSocketTicket({
        headers: cloudflareAccessHeaders(input.cloudflareAccess),
      })
      .pipe(Effect.provideService(FetchHttpClient.RequestInit, { credentials: "include" })),
  );
});

export const issueRemoteDpopWebSocketTicket = Effect.fn(
  "clientRuntime.authorization.issueRemoteDpopWebSocketTicket",
)(function* (input: {
  readonly httpBaseUrl: string;
  readonly accessToken: string;
  readonly dpopProof: string;
  readonly cloudflareAccess?: CloudflareAccessAuthorization;
  readonly timeoutMs?: number;
}) {
  const client = yield* makeEnvironmentHttpApiClient(input.httpBaseUrl);
  return yield* executeEnvironmentHttpRequest(
    environmentEndpointUrl(input.httpBaseUrl, "/api/auth/websocket-ticket"),
    input.timeoutMs ?? DEFAULT_REMOTE_REQUEST_TIMEOUT_MS,
    client.auth.webSocketTicket({
      headers: {
        ...cloudflareAccessHeaders(input.cloudflareAccess),
        authorization: `DPoP ${input.accessToken}`,
        dpop: input.dpopProof,
      },
    }),
  );
});

export const resolveRemoteWebSocketConnectionUrl = Effect.fn(
  "clientRuntime.authorization.resolveRemoteWebSocketConnectionUrl",
)(function* (input: {
  readonly wsBaseUrl: string;
  readonly httpBaseUrl: string;
  readonly bearerToken: string;
  readonly cloudflareAccess?: CloudflareAccessAuthorization;
  readonly timeoutMs?: number;
}) {
  const issued = yield* issueRemoteWebSocketTicket({
    httpBaseUrl: input.httpBaseUrl,
    bearerToken: input.bearerToken,
    ...(input.cloudflareAccess ? { cloudflareAccess: input.cloudflareAccess } : {}),
    ...(input.timeoutMs ? { timeoutMs: input.timeoutMs } : {}),
  });

  const url = new URL(input.wsBaseUrl);
  if (url.pathname === "" || url.pathname === "/") {
    url.pathname = "/ws";
  }
  url.searchParams.set("wsTicket", issued.ticket);
  return url.toString();
});

export const resolveBrowserSessionWebSocketConnectionUrl = Effect.fn(
  "clientRuntime.authorization.resolveBrowserSessionWebSocketConnectionUrl",
)(function* (input: {
  readonly wsBaseUrl: string;
  readonly httpBaseUrl: string;
  readonly cloudflareAccess?: CloudflareAccessAuthorization;
  readonly timeoutMs?: number;
}) {
  const issued = yield* issueBrowserSessionWebSocketTicket({
    httpBaseUrl: input.httpBaseUrl,
    ...(input.cloudflareAccess ? { cloudflareAccess: input.cloudflareAccess } : {}),
    ...(input.timeoutMs ? { timeoutMs: input.timeoutMs } : {}),
  });

  const url = new URL(input.wsBaseUrl);
  if (url.pathname === "" || url.pathname === "/") {
    url.pathname = "/ws";
  }
  url.searchParams.set("wsTicket", issued.ticket);
  return url.toString();
});

export const resolveRemoteDpopWebSocketConnectionUrl = Effect.fn(
  "clientRuntime.authorization.resolveRemoteDpopWebSocketConnectionUrl",
)(function* (input: {
  readonly wsBaseUrl: string;
  readonly httpBaseUrl: string;
  readonly accessToken: string;
  readonly dpopProof: string;
  readonly cloudflareAccess?: CloudflareAccessAuthorization;
  readonly timeoutMs?: number;
}) {
  const issued = yield* issueRemoteDpopWebSocketTicket({
    httpBaseUrl: input.httpBaseUrl,
    accessToken: input.accessToken,
    dpopProof: input.dpopProof,
    ...(input.cloudflareAccess ? { cloudflareAccess: input.cloudflareAccess } : {}),
    ...(input.timeoutMs ? { timeoutMs: input.timeoutMs } : {}),
  });
  const url = new URL(input.wsBaseUrl);
  if (url.pathname === "" || url.pathname === "/") {
    url.pathname = "/ws";
  }
  url.searchParams.set("wsTicket", issued.ticket);
  return url.toString();
});
