import { RelayEnvironmentConnectScope } from "@t3tools/contracts/relay";
import { withRelayClientTracing } from "@t3tools/shared/relayTracing";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import * as RemoteEnvironmentAuthorization from "../authorization/service.ts";
import {
  cloudflareAccessHeaders,
  type CloudflareAccessAuthorization,
} from "../authorization/remote.ts";
import * as ManagedRelay from "../relay/managedRelay.ts";
import * as ClientCapabilities from "../platform/capabilities.ts";
import {
  BearerConnectionCredential,
  BearerConnectionProfile,
  type ConnectionCatalogEntry,
  SshConnectionProfile,
} from "./catalog.ts";
import * as ConnectionCredentialStore from "./credentialStore.ts";
import {
  credentialMissingError,
  environmentMismatchError,
  mapManagedRelayError,
  profileMissingError,
} from "./errors.ts";
import type {
  BearerConnectionTarget,
  ConnectionTarget,
  PreparedConnection,
  PrimaryConnectionTarget,
  RelayConnectionTarget,
  SshConnectionTarget,
} from "./model.ts";
import { ConnectionBlockedError, type ConnectionAttemptError } from "./model.ts";
import * as ConnectionProfileStore from "./profileStore.ts";

export class ConnectionResolver extends Context.Service<
  ConnectionResolver,
  {
    readonly prepare: (
      entry: ConnectionCatalogEntry,
    ) => Effect.Effect<PreparedConnection, ConnectionAttemptError>;
  }
>()("@t3tools/client-runtime/connection/resolver/ConnectionResolver") {}

const isBearerProfile = Schema.is(BearerConnectionProfile);
const isSshProfile = Schema.is(SshConnectionProfile);
const isBearerCredential = Schema.is(BearerConnectionCredential);

function cloudflareAccessFromCredential(credential: BearerConnectionCredential) {
  const clientId = credential.cloudflareAccessClientId?.trim() ?? "";
  const clientSecret = credential.cloudflareAccessClientSecret?.trim() ?? "";
  if (clientId.length > 0 && clientSecret.length > 0) {
    return {
      _tag: "service-token" as const,
      clientId,
      clientSecret,
    };
  }
  const cookieValue = credential.cloudflareAccessCookie?.trim() ?? "";
  if (cookieValue.length > 0) {
    return {
      _tag: "cookie" as const,
      cookieValue,
    };
  }
  const jwt = credential.cloudflareAccessToken?.trim() ?? "";
  return jwt.length > 0 ? { jwt } : undefined;
}

function validateCloudflareAccessRuntime(
  cloudflareAccess: ReturnType<typeof cloudflareAccessFromCredential>,
  installer: ClientCapabilities.CloudflareAccessCookieInstaller["Service"],
) {
  const canInstallRequestHeaders =
    installer.supportsRequestHeaders === true && installer.installRequestHeaders !== undefined;
  if (
    cloudflareAccess?._tag === "service-token" &&
    !ClientCapabilities.canPassWebSocketHeaderOptions() &&
    !canInstallRequestHeaders
  ) {
    return Effect.fail(
      new ConnectionBlockedError({
        reason: "unsupported",
        detail:
          "Cloudflare Access service-token connections require a client that can send WebSocket headers. Use a header-capable desktop or headless client, or save a browser-compatible Cloudflare Access JWT/cookie connection.",
      }),
    );
  }
  if (
    cloudflareAccess !== undefined &&
    cloudflareAccess._tag !== "service-token" &&
    !ClientCapabilities.canPassWebSocketHeaderOptions() &&
    installer.supportsCookieInstall !== true &&
    !canInstallRequestHeaders
  ) {
    return Effect.fail(
      new ConnectionBlockedError({
        reason: "unsupported",
        detail:
          "Cloudflare Access JWT/cookie connections require a client that can preserve those credentials for WebSocket connections. Use the desktop app, a header-capable client, or sign in interactively before connecting.",
      }),
    );
  }
  return Effect.void;
}

function cloudflareAccessCookieValue(
  cloudflareAccess: CloudflareAccessAuthorization | undefined,
): string | undefined {
  if (cloudflareAccess === undefined || cloudflareAccess._tag === "service-token") {
    return undefined;
  }
  const value =
    cloudflareAccess._tag === "cookie" ? cloudflareAccess.cookieValue : cloudflareAccess.jwt;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

const installCloudflareAccessTransport = Effect.fn(
  "clientRuntime.connection.broker.installCloudflareAccessTransport",
)(function* (
  installer: ClientCapabilities.CloudflareAccessCookieInstaller["Service"],
  httpBaseUrl: string,
  cloudflareAccess: CloudflareAccessAuthorization | undefined,
) {
  if (cloudflareAccess === undefined) {
    if (
      installer.supportsRequestHeaders === true &&
      installer.installRequestHeaders !== undefined
    ) {
      yield* installer.installRequestHeaders({
        httpBaseUrl,
        headers: {},
      });
    }
    return;
  }
  const cookieValue = cloudflareAccessCookieValue(cloudflareAccess);
  if (installer.supportsRequestHeaders === true && installer.installRequestHeaders !== undefined) {
    yield* installer.installRequestHeaders({
      httpBaseUrl,
      headers: cloudflareAccessHeaders(cloudflareAccess),
      clearCookies: true,
      ...(cookieValue ? { cookieValue } : {}),
    });
    return;
  }
  if (cookieValue !== undefined && installer.supportsCookieInstall === true) {
    yield* installer.install({ httpBaseUrl, cookieValue });
  }
});

function primarySocketUrl(target: PrimaryConnectionTarget): string {
  const url = new URL(target.wsBaseUrl);
  if (url.pathname === "" || url.pathname === "/") {
    url.pathname = "/ws";
  }
  return url.toString();
}

const makePrimaryBroker = Effect.fn("clientRuntime.connection.broker.makePrimary")(function* () {
  const auth = yield* ClientCapabilities.PrimaryEnvironmentAuth;
  const remote = yield* RemoteEnvironmentAuthorization.RemoteEnvironmentAuthorization;

  return Effect.fn("clientRuntime.connection.broker.primary")(function* (
    target: PrimaryConnectionTarget,
  ) {
    const bearerToken = yield* auth.bearerToken;
    if (Option.isNone(bearerToken)) {
      return {
        environmentId: target.environmentId,
        label: target.label,
        httpBaseUrl: target.httpBaseUrl,
        socketUrl: primarySocketUrl(target),
        httpAuthorization: null,
        target,
      } satisfies PreparedConnection;
    }

    const authorized = yield* remote.authorizeBearer({
      expectedEnvironmentId: target.environmentId,
      httpBaseUrl: target.httpBaseUrl,
      wsBaseUrl: target.wsBaseUrl,
      bearerToken: bearerToken.value,
    });
    return {
      ...authorized,
      target,
    } satisfies PreparedConnection;
  });
});

const makeBearerBroker = Effect.fn("clientRuntime.connection.broker.makeBearer")(function* () {
  const credentials = yield* ConnectionCredentialStore.ConnectionCredentialStore;
  const remote = yield* RemoteEnvironmentAuthorization.RemoteEnvironmentAuthorization;
  const cookieInstaller = yield* ClientCapabilities.CloudflareAccessCookieInstaller;

  return Effect.fn("clientRuntime.connection.broker.bearer")(function* (
    entry: ConnectionCatalogEntry & { readonly target: BearerConnectionTarget },
  ) {
    const target = entry.target;
    const profile = yield* Option.match(entry.profile, {
      onNone: () => Effect.fail(profileMissingError(target.connectionId)),
      onSome: Effect.succeed,
    });
    if (!isBearerProfile(profile)) {
      return yield* new ConnectionBlockedError({
        reason: "configuration",
        detail: `Connection profile ${target.connectionId} is not a bearer connection.`,
      });
    }
    if (profile.environmentId !== target.environmentId) {
      return yield* environmentMismatchError({
        expected: target.environmentId,
        actual: profile.environmentId,
      });
    }
    const credential = yield* credentials.get(target.connectionId).pipe(
      Effect.flatMap(
        Option.match({
          onNone: () => Effect.fail(credentialMissingError(target.connectionId)),
          onSome: Effect.succeed,
        }),
      ),
    );
    if (!isBearerCredential(credential)) {
      return yield* credentialMissingError(target.connectionId);
    }
    const cloudflareAccess = cloudflareAccessFromCredential(credential);
    yield* validateCloudflareAccessRuntime(cloudflareAccess, cookieInstaller);
    yield* installCloudflareAccessTransport(cookieInstaller, profile.httpBaseUrl, cloudflareAccess);
    const authorized = yield* remote.authorizeBearer({
      expectedEnvironmentId: target.environmentId,
      httpBaseUrl: profile.httpBaseUrl,
      wsBaseUrl: profile.wsBaseUrl,
      bearerToken: credential.token,
      ...(cloudflareAccess ? { cloudflareAccess } : {}),
    });
    return {
      environmentId: authorized.environmentId,
      label: authorized.label,
      httpBaseUrl: authorized.httpBaseUrl,
      socketUrl: authorized.socketUrl,
      ...(authorized.socketHeaders ? { socketHeaders: authorized.socketHeaders } : {}),
      httpAuthorization: authorized.httpAuthorization,
      target,
    } satisfies PreparedConnection;
  });
});

const makeRelayBroker = Effect.fn("clientRuntime.connection.broker.makeRelay")(function* () {
  const relay = yield* ManagedRelay.ManagedRelayClient;
  const session = yield* ClientCapabilities.CloudSession;
  const identity = yield* ClientCapabilities.RelayDeviceIdentity;
  const remote = yield* RemoteEnvironmentAuthorization.RemoteEnvironmentAuthorization;

  return Effect.fnUntraced(
    function* (target: RelayConnectionTarget) {
      const authorized = yield* remote.authorizeDpop({
        expectedEnvironmentId: target.environmentId,
        obtainBootstrap: Effect.gen(function* () {
          const clerkToken = yield* session.clerkToken.pipe(
            Effect.withSpan("relay.connection.cloudSessionToken.resolve"),
          );
          const deviceId = yield* identity.deviceId.pipe(
            Effect.withSpan("relay.connection.deviceIdentity.resolve"),
          );
          const connected = yield* relay
            .connectEnvironment({
              clerkToken,
              scopes: [RelayEnvironmentConnectScope],
              environmentId: target.environmentId,
              ...(Option.isSome(deviceId) ? { deviceId: deviceId.value } : {}),
            })
            .pipe(Effect.mapError(mapManagedRelayError));
          if (connected.environmentId !== target.environmentId) {
            return yield* environmentMismatchError({
              expected: target.environmentId,
              actual: connected.environmentId,
            });
          }
          return connected;
        }).pipe(Effect.withSpan("relay.connection.bootstrap.obtain")),
      });
      return {
        environmentId: authorized.environmentId,
        label: authorized.label,
        httpBaseUrl: authorized.httpBaseUrl,
        socketUrl: authorized.socketUrl,
        httpAuthorization: authorized.httpAuthorization,
        target,
      } satisfies PreparedConnection;
    },
    Effect.withSpan("clientRuntime.connection.broker.relay"),
    withRelayClientTracing,
  );
});

const makeSshBroker = Effect.fn("clientRuntime.connection.broker.makeSsh")(function* () {
  const profiles = yield* ConnectionProfileStore.ConnectionProfileStore;
  const ssh = yield* ClientCapabilities.SshEnvironmentGateway;
  const remote = yield* RemoteEnvironmentAuthorization.RemoteEnvironmentAuthorization;

  return Effect.fn("clientRuntime.connection.broker.ssh")(function* (
    entry: ConnectionCatalogEntry & { readonly target: SshConnectionTarget },
  ) {
    const target = entry.target;
    const profile = yield* Option.match(entry.profile, {
      onNone: () => Effect.fail(profileMissingError(target.connectionId)),
      onSome: Effect.succeed,
    });
    if (!isSshProfile(profile)) {
      return yield* new ConnectionBlockedError({
        reason: "configuration",
        detail: `Connection profile ${target.connectionId} is not an SSH connection.`,
      });
    }
    if (profile.environmentId !== target.environmentId) {
      return yield* environmentMismatchError({
        expected: target.environmentId,
        actual: profile.environmentId,
      });
    }
    const prepared = yield* ssh.prepare({
      connectionId: target.connectionId,
      expectedEnvironmentId: target.environmentId,
      target: profile.target,
    });
    yield* profiles.put(
      new SshConnectionProfile({
        connectionId: profile.connectionId,
        environmentId: profile.environmentId,
        label: profile.label,
        target: prepared.bootstrap.target,
      }),
    );
    const authorized = yield* remote.authorizeBearer({
      expectedEnvironmentId: target.environmentId,
      httpBaseUrl: prepared.bootstrap.httpBaseUrl,
      wsBaseUrl: prepared.bootstrap.wsBaseUrl,
      bearerToken: prepared.bearerToken,
    });
    return {
      environmentId: authorized.environmentId,
      label: authorized.label,
      httpBaseUrl: authorized.httpBaseUrl,
      socketUrl: authorized.socketUrl,
      ...(authorized.socketHeaders ? { socketHeaders: authorized.socketHeaders } : {}),
      httpAuthorization: authorized.httpAuthorization,
      target,
    } satisfies PreparedConnection;
  });
});

export const make = Effect.gen(function* () {
  const primary = yield* makePrimaryBroker();
  const bearer = yield* makeBearerBroker();
  const relay = yield* makeRelayBroker();
  const ssh = yield* makeSshBroker();

  const prepare = Effect.fn("clientRuntime.connection.broker.prepare")(function* (
    entry: ConnectionCatalogEntry,
  ) {
    const target: ConnectionTarget = entry.target;
    yield* Effect.annotateCurrentSpan({
      "connection.environment.id": target.environmentId,
      "connection.target.kind": target._tag,
    });
    switch (target._tag) {
      case "PrimaryConnectionTarget":
        return yield* primary(target);
      case "BearerConnectionTarget":
        return yield* bearer({ ...entry, target });
      case "RelayConnectionTarget":
        return yield* relay(target);
      case "SshConnectionTarget":
        return yield* ssh({ ...entry, target });
    }
  });

  return ConnectionResolver.of({ prepare });
});

export const layer = Layer.effect(ConnectionResolver, make);
