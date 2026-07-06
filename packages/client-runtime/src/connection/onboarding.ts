import type { DesktopSshEnvironmentTarget, EnvironmentId } from "@t3tools/contracts";
import { resolveRemotePairingTarget } from "@t3tools/shared/remote";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SubscriptionRef from "effect/SubscriptionRef";
import * as HttpClient from "effect/unstable/http/HttpClient";

import {
  bootstrapRemoteBearerSession,
  cloudflareAccessHeaders,
  type CloudflareAccessAuthorization,
} from "../authorization/remote.ts";
import { deriveWsBaseUrl, normalizeHttpBaseUrl } from "../environment/endpoint.ts";
import { fetchRemoteEnvironmentDescriptor } from "../environment/descriptor.ts";
import * as ClientCapabilities from "../platform/capabilities.ts";
import {
  BearerConnectionCredential,
  BearerConnectionProfile,
  BearerConnectionRegistration,
  type ConnectionCatalogEntry,
  type ConnectionCredential,
  SshConnectionProfile,
  SshConnectionRegistration,
} from "./catalog.ts";
import * as ConnectionCredentialStore from "./credentialStore.ts";
import { mapRemoteEnvironmentError } from "./errors.ts";
import {
  BearerConnectionTarget,
  ConnectionBlockedError,
  SshConnectionTarget,
  type ConnectionAttemptError,
} from "./model.ts";
import * as Persistence from "../platform/persistence.ts";
import * as EnvironmentRegistry from "./registry.ts";

export interface PairingConnectionInput {
  readonly pairingUrl?: string;
  readonly host?: string;
  readonly pairingCode?: string;
  readonly cloudflareAccessToken?: string;
  readonly cloudflareAccessClientId?: string;
  readonly cloudflareAccessClientSecret?: string;
  readonly cloudflareAccessCookie?: string;
}

export interface SshConnectionInput {
  readonly target: DesktopSshEnvironmentTarget;
  readonly label?: string;
}

export interface BearerConnectionUpdateInput {
  readonly environmentId: EnvironmentId;
  readonly label: string;
  readonly httpBaseUrl: string;
}

export class ConnectionOnboarding extends Context.Service<
  ConnectionOnboarding,
  {
    readonly registerPairing: (
      input: PairingConnectionInput,
    ) => Effect.Effect<
      EnvironmentId,
      ConnectionAttemptError | Persistence.ConnectionPersistenceError
    >;
    readonly registerSsh: (
      input: SshConnectionInput,
    ) => Effect.Effect<
      EnvironmentId,
      ConnectionAttemptError | Persistence.ConnectionPersistenceError
    >;
    readonly updateBearer: (
      input: BearerConnectionUpdateInput,
    ) => Effect.Effect<void, ConnectionAttemptError | Persistence.ConnectionPersistenceError>;
  }
>()("@t3tools/client-runtime/connection/onboarding/ConnectionOnboarding") {}

const resolvePairingTarget = Effect.fn("clientRuntime.connection.onboarding.resolvePairingTarget")(
  function* (input: PairingConnectionInput) {
    return yield* Effect.try({
      try: () => resolveRemotePairingTarget(input),
      catch: (cause) =>
        new ConnectionBlockedError({
          reason: "configuration",
          detail: cause instanceof Error ? cause.message : "The pairing details are invalid.",
        }),
    });
  },
);

function cloudflareAccessFromPairingInput(
  input: PairingConnectionInput,
  target: ReturnType<typeof resolveRemotePairingTarget>,
) {
  const clientId = target.cloudflareAccessClientId?.trim() ?? "";
  const clientSecret = target.cloudflareAccessClientSecret?.trim() ?? "";
  if (clientId.length > 0 && clientSecret.length > 0) {
    return {
      _tag: "service-token" as const,
      clientId,
      clientSecret,
    };
  }
  const cookieValue = input.cloudflareAccessCookie?.trim() ?? "";
  if (cookieValue.length > 0) {
    return {
      _tag: "cookie" as const,
      cookieValue,
    };
  }
  const jwt = target.cloudflareAccessToken?.trim() ?? "";
  return jwt.length > 0 ? { jwt } : undefined;
}

function validateCloudflareAccessRuntime(
  cloudflareAccess: ReturnType<typeof cloudflareAccessFromPairingInput>,
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
          "Cloudflare Access service-token pairing requires a client that can send WebSocket headers. Use a header-capable desktop or headless client, or pair from a browser with a Cloudflare Access JWT/cookie session.",
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
          "Cloudflare Access JWT/cookie pairing requires a client that can preserve those credentials for WebSocket connections. Use the desktop app, a header-capable client, or sign in interactively before pairing.",
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
  "clientRuntime.connection.onboarding.installCloudflareAccessTransport",
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
        clearCookies: true,
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

export const preparePairingRegistration = Effect.fn(
  "clientRuntime.connection.onboarding.preparePairingRegistration",
)(function* (input: PairingConnectionInput) {
  const target = yield* resolvePairingTarget(input);
  const presentation = yield* ClientCapabilities.ClientPresentation;
  const installer = yield* ClientCapabilities.CloudflareAccessCookieInstaller;
  const cloudflareAccess = cloudflareAccessFromPairingInput(input, target);
  yield* validateCloudflareAccessRuntime(cloudflareAccess, installer);
  yield* installCloudflareAccessTransport(installer, target.httpBaseUrl, cloudflareAccess);
  const descriptor = yield* fetchRemoteEnvironmentDescriptor({
    httpBaseUrl: target.httpBaseUrl,
    ...(cloudflareAccess ? { cloudflareAccess } : {}),
  }).pipe(Effect.mapError(mapRemoteEnvironmentError));
  const access = yield* bootstrapRemoteBearerSession({
    httpBaseUrl: target.httpBaseUrl,
    credential: target.credential,
    scopes: presentation.scopes,
    clientMetadata: presentation.metadata,
    ...(cloudflareAccess ? { cloudflareAccess } : {}),
  }).pipe(Effect.mapError(mapRemoteEnvironmentError));
  const connectionId = `bearer:${descriptor.environmentId}`;

  return new BearerConnectionRegistration({
    target: new BearerConnectionTarget({
      environmentId: descriptor.environmentId,
      label: descriptor.label,
      connectionId,
    }),
    profile: new BearerConnectionProfile({
      connectionId,
      environmentId: descriptor.environmentId,
      label: descriptor.label,
      httpBaseUrl: target.httpBaseUrl,
      wsBaseUrl: target.wsBaseUrl,
    }),
    credential: new BearerConnectionCredential({
      token: access.access_token,
      ...(target.cloudflareAccessToken
        ? { cloudflareAccessToken: target.cloudflareAccessToken }
        : {}),
      ...(input.cloudflareAccessCookie
        ? { cloudflareAccessCookie: input.cloudflareAccessCookie }
        : {}),
      ...(target.cloudflareAccessClientId && target.cloudflareAccessClientSecret
        ? {
            cloudflareAccessClientId: target.cloudflareAccessClientId,
            cloudflareAccessClientSecret: target.cloudflareAccessClientSecret,
          }
        : {}),
    }),
  });
});

export const registerPairingConnection = Effect.fn(
  "clientRuntime.connection.onboarding.registerPairingConnection",
)(function* (input: PairingConnectionInput) {
  const registration = yield* preparePairingRegistration(input);
  const registry = yield* EnvironmentRegistry.EnvironmentRegistry;
  yield* registry.register(registration);
  return registration.target.environmentId;
});

const isBearerCredential = Schema.is(BearerConnectionCredential);
const isBearerProfile = Schema.is(BearerConnectionProfile);

export const updateBearerConnection = Effect.fn(
  "clientRuntime.connection.onboarding.updateBearerConnection",
)(function* (input: BearerConnectionUpdateInput) {
  const registry = yield* EnvironmentRegistry.EnvironmentRegistry;
  const credentials = yield* ConnectionCredentialStore.ConnectionCredentialStore;
  const entry = (yield* SubscriptionRef.get(registry.entries)).get(input.environmentId);
  const credential =
    entry?.target._tag === "BearerConnectionTarget"
      ? yield* credentials.get(entry.target.connectionId)
      : Option.none();
  const registration = yield* prepareBearerConnectionUpdate({
    input,
    entry: Option.fromUndefinedOr(entry),
    credential,
  });
  yield* registry.register(registration);
});

export const prepareBearerConnectionUpdate = Effect.fn(
  "clientRuntime.connection.onboarding.prepareBearerConnectionUpdate",
)(function* (options: {
  readonly input: BearerConnectionUpdateInput;
  readonly entry: Option.Option<ConnectionCatalogEntry>;
  readonly credential: Option.Option<ConnectionCredential>;
}) {
  const entry = Option.getOrNull(options.entry);
  if (
    entry === undefined ||
    entry === null ||
    entry.target._tag !== "BearerConnectionTarget" ||
    Option.isNone(entry.profile) ||
    !isBearerProfile(entry.profile.value)
  ) {
    return yield* new ConnectionBlockedError({
      reason: "configuration",
      detail: "Only saved bearer environments can be edited.",
    });
  }

  const credential = options.credential;
  if (Option.isNone(credential) || !isBearerCredential(credential.value)) {
    return yield* new ConnectionBlockedError({
      reason: "authentication",
      detail: "The saved bearer credential is unavailable.",
    });
  }

  const label = options.input.label.trim();
  if (label === "") {
    return yield* new ConnectionBlockedError({
      reason: "configuration",
      detail: "Environment label cannot be empty.",
    });
  }
  const httpBaseUrl = yield* Effect.try({
    try: () => normalizeHttpBaseUrl(options.input.httpBaseUrl),
    catch: (cause) =>
      new ConnectionBlockedError({
        reason: "configuration",
        detail: cause instanceof Error ? cause.message : "The environment URL is invalid.",
      }),
  });
  const connectionId = entry.target.connectionId;
  return new BearerConnectionRegistration({
    target: new BearerConnectionTarget({
      environmentId: options.input.environmentId,
      label,
      connectionId,
    }),
    profile: new BearerConnectionProfile({
      connectionId,
      environmentId: options.input.environmentId,
      label,
      httpBaseUrl,
      wsBaseUrl: deriveWsBaseUrl(httpBaseUrl),
    }),
    credential: credential.value,
  });
});

export const prepareSshRegistration = Effect.fn(
  "clientRuntime.connection.onboarding.prepareSshRegistration",
)(function* (input: SshConnectionInput) {
  const gateway = yield* ClientCapabilities.SshEnvironmentGateway;
  const provisioned = yield* gateway.provision(input.target);
  const connectionId = `ssh:${provisioned.environmentId}`;
  const label = input.label?.trim() || provisioned.label || provisioned.bootstrap.target.alias;

  return new SshConnectionRegistration({
    target: new SshConnectionTarget({
      environmentId: provisioned.environmentId,
      label,
      connectionId,
    }),
    profile: new SshConnectionProfile({
      connectionId,
      environmentId: provisioned.environmentId,
      label,
      target: provisioned.bootstrap.target,
    }),
  });
});

export const registerSshConnection = Effect.fn(
  "clientRuntime.connection.onboarding.registerSshConnection",
)(function* (input: SshConnectionInput) {
  const registration = yield* prepareSshRegistration(input);
  const registry = yield* EnvironmentRegistry.EnvironmentRegistry;
  yield* registry.register(registration);
  return registration.target.environmentId;
});

export const make = Effect.gen(function* () {
  const registry = yield* EnvironmentRegistry.EnvironmentRegistry;
  const presentation = yield* ClientCapabilities.ClientPresentation;
  const httpClient = yield* HttpClient.HttpClient;
  const ssh = yield* ClientCapabilities.SshEnvironmentGateway;
  const credentials = yield* ConnectionCredentialStore.ConnectionCredentialStore;
  const cloudflareAccessInstaller = yield* ClientCapabilities.CloudflareAccessCookieInstaller;

  return ConnectionOnboarding.of({
    registerPairing: (input) =>
      registerPairingConnection(input).pipe(
        Effect.provideService(EnvironmentRegistry.EnvironmentRegistry, registry),
        Effect.provideService(ClientCapabilities.ClientPresentation, presentation),
        Effect.provideService(
          ClientCapabilities.CloudflareAccessCookieInstaller,
          cloudflareAccessInstaller,
        ),
        Effect.provideService(HttpClient.HttpClient, httpClient),
      ),
    registerSsh: (input) =>
      registerSshConnection(input).pipe(
        Effect.provideService(EnvironmentRegistry.EnvironmentRegistry, registry),
        Effect.provideService(ClientCapabilities.SshEnvironmentGateway, ssh),
      ),
    updateBearer: (input) =>
      updateBearerConnection(input).pipe(
        Effect.provideService(EnvironmentRegistry.EnvironmentRegistry, registry),
        Effect.provideService(ConnectionCredentialStore.ConnectionCredentialStore, credentials),
      ),
  });
});

export const layer = Layer.effect(ConnectionOnboarding, make);
