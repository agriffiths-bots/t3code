import { EnvironmentId, type DesktopSshEnvironmentTarget } from "@t3tools/contracts";
import { RelayEnvironmentConnectScope } from "@t3tools/contracts/relay";
import { RelayClientTracer } from "@t3tools/shared/relayTracing";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Tracer from "effect/Tracer";
import { afterEach, vi } from "vite-plus/test";

import * as ManagedRelay from "../relay/managedRelay.ts";
import * as ConnectionResolver from "./resolver.ts";
import * as ClientCapabilities from "../platform/capabilities.ts";
import * as RemoteEnvironmentAuthorization from "../authorization/service.ts";
import {
  BearerConnectionCredential,
  BearerConnectionProfile,
  type ConnectionCatalogEntry,
  SshConnectionProfile,
  type ConnectionCredential,
  type ConnectionProfile,
} from "./catalog.ts";
import * as ConnectionCredentialStore from "./credentialStore.ts";
import {
  BearerConnectionTarget,
  ConnectionTransientError,
  PrimaryConnectionTarget,
  RelayConnectionTarget,
  SshConnectionTarget,
  type ConnectionTarget,
} from "./model.ts";
import * as ConnectionProfileStore from "./profileStore.ts";

const ENVIRONMENT_ID = EnvironmentId.make("environment-1");
const ENDPOINT = {
  httpBaseUrl: "https://environment.example.test",
  wsBaseUrl: "wss://environment.example.test",
  providerKind: "cloudflare_tunnel" as const,
};
const SSH_TARGET: DesktopSshEnvironmentTarget = {
  alias: "development",
  hostname: "development.example.test",
  username: "developer",
  port: 22,
};

afterEach(() => {
  vi.unstubAllGlobals();
});

function enableHeaderCapableWebSocketRuntime() {
  vi.stubGlobal(
    "WebSocket",
    ClientCapabilities.markWebSocketHeaderOptionsCapable(function WebSocket() {}),
  );
}

function catalogEntry(
  target: ConnectionTarget,
  profile: Option.Option<ConnectionProfile> = Option.none(),
): ConnectionCatalogEntry {
  return { target, profile };
}

function unsupported<A>(name: string): Effect.Effect<A> {
  return Effect.die(new Error(`Unexpected relay call: ${name}`));
}

function collectingTracer(spans: Array<string>): Tracer.Tracer {
  return Tracer.make({
    span: (options) => {
      const span = new Tracer.NativeSpan(options);
      const end = span.end.bind(span);
      span.end = (endTime, exit) => {
        end(endTime, exit);
        spans.push(span.name);
      };
      return span;
    },
  });
}

function relayClient(
  connectEnvironment: ManagedRelay.ManagedRelayClient["Service"]["connectEnvironment"],
) {
  return ManagedRelay.ManagedRelayClient.of({
    relayUrl: "https://relay.example.test",
    listEnvironments: () => unsupported("listEnvironments"),
    listDevices: () => unsupported("listDevices"),
    createEnvironmentLinkChallenge: () => unsupported("createEnvironmentLinkChallenge"),
    linkEnvironment: () => unsupported("linkEnvironment"),
    unlinkEnvironment: () => unsupported("unlinkEnvironment"),
    getEnvironmentStatus: () => unsupported("getEnvironmentStatus"),
    connectEnvironment,
    registerDevice: () => unsupported("registerDevice"),
    unregisterDevice: () => unsupported("unregisterDevice"),
    registerLiveActivity: () => unsupported("registerLiveActivity"),
    resetTokenCache: Effect.void,
  });
}

const makeDependencies = Effect.fn("TestConnectionResolver.makeDependencies")((options?: {
  readonly profiles?: ReadonlyArray<ConnectionProfile>;
  readonly credentials?: ReadonlyArray<readonly [string, ConnectionCredential]>;
  readonly connectEnvironment?: ManagedRelay.ManagedRelayClient["Service"]["connectEnvironment"];
  readonly authorizeBearer?: RemoteEnvironmentAuthorization.RemoteEnvironmentAuthorization["Service"]["authorizeBearer"];
  readonly authorizeDpop?: RemoteEnvironmentAuthorization.RemoteEnvironmentAuthorization["Service"]["authorizeDpop"];
  readonly primaryBearerToken?: string;
  readonly prepareSsh?: ClientCapabilities.SshEnvironmentGateway["Service"]["prepare"];
  readonly installCloudflareAccessCookie?: ClientCapabilities.CloudflareAccessCookieInstaller["Service"]["install"];
  readonly installCloudflareAccessRequestHeaders?: ClientCapabilities.CloudflareAccessCookieInstaller["Service"]["installRequestHeaders"];
  readonly supportsCloudflareAccessCookieInstall?: boolean;
  readonly supportsCloudflareAccessRequestHeaders?: boolean;
}) => {
  const profiles = new Map(
    (options?.profiles ?? []).map((profile) => [profile.connectionId, profile]),
  );
  const credentials = new Map(options?.credentials ?? []);

  const profileStore = ConnectionProfileStore.ConnectionProfileStore.of({
    get: (connectionId) => Effect.succeed(Option.fromNullishOr(profiles.get(connectionId))),
    put: (profile) => Effect.sync(() => void profiles.set(profile.connectionId, profile)),
    remove: (connectionId) => Effect.sync(() => void profiles.delete(connectionId)),
  });
  const credentialStore = ConnectionCredentialStore.ConnectionCredentialStore.of({
    get: (connectionId) => Effect.succeed(Option.fromNullishOr(credentials.get(connectionId))),
    put: (connectionId, credential) =>
      Effect.sync(() => void credentials.set(connectionId, credential)),
    remove: (connectionId) => Effect.sync(() => void credentials.delete(connectionId)),
  });
  const remote = RemoteEnvironmentAuthorization.RemoteEnvironmentAuthorization.of({
    authorizeBearer:
      options?.authorizeBearer ??
      ((input) =>
        Effect.succeed({
          environmentId: input.expectedEnvironmentId,
          label: "Authorized bearer environment",
          httpBaseUrl: input.httpBaseUrl,
          socketUrl: "wss://authorized.example.test/ws?wsTicket=bearer",
          httpAuthorization: {
            _tag: "Bearer" as const,
            token: input.bearerToken,
          },
        })),
    authorizeDpop:
      options?.authorizeDpop ??
      ((input) =>
        input.obtainBootstrap.pipe(
          Effect.as({
            environmentId: input.expectedEnvironmentId,
            label: "Authorized relay environment",
            httpBaseUrl: ENDPOINT.httpBaseUrl,
            socketUrl: "wss://authorized.example.test/ws?wsTicket=dpop",
            httpAuthorization: {
              _tag: "Dpop" as const,
              accessToken: "dpop-access-token",
            },
          }),
        )),
  });
  const ssh = ClientCapabilities.SshEnvironmentGateway.of({
    provision: () => Effect.die("unused"),
    prepare:
      options?.prepareSsh ??
      (() =>
        Effect.succeed({
          bootstrap: {
            target: SSH_TARGET,
            httpBaseUrl: "http://127.0.0.1:4010",
            wsBaseUrl: "ws://127.0.0.1:4010",
            pairingToken: null,
          },
          bearerToken: "ssh-bearer",
        })),
    disconnect: () => Effect.void,
  });

  const dependencies = Layer.mergeAll(
    Layer.succeed(ConnectionProfileStore.ConnectionProfileStore, profileStore),
    Layer.succeed(ConnectionCredentialStore.ConnectionCredentialStore, credentialStore),
    Layer.succeed(
      ClientCapabilities.CloudSession,
      ClientCapabilities.CloudSession.of({ clerkToken: Effect.succeed("clerk-session") }),
    ),
    Layer.succeed(
      ClientCapabilities.PrimaryEnvironmentAuth,
      ClientCapabilities.PrimaryEnvironmentAuth.of({
        bearerToken: Effect.succeed(Option.fromNullishOr(options?.primaryBearerToken)),
      }),
    ),
    Layer.succeed(
      ClientCapabilities.CloudflareAccessCookieInstaller,
      ClientCapabilities.CloudflareAccessCookieInstaller.of({
        supportsCookieInstall:
          options?.supportsCloudflareAccessCookieInstall ??
          options?.installCloudflareAccessCookie !== undefined,
        supportsRequestHeaders:
          options?.supportsCloudflareAccessRequestHeaders ??
          options?.installCloudflareAccessRequestHeaders !== undefined,
        install: options?.installCloudflareAccessCookie ?? (() => Effect.void),
        ...(options?.installCloudflareAccessRequestHeaders
          ? { installRequestHeaders: options.installCloudflareAccessRequestHeaders }
          : {}),
      }),
    ),
    Layer.succeed(
      ClientCapabilities.RelayDeviceIdentity,
      ClientCapabilities.RelayDeviceIdentity.of({
        deviceId: Effect.succeed(Option.some("device-1")),
      }),
    ),
    Layer.succeed(RemoteEnvironmentAuthorization.RemoteEnvironmentAuthorization, remote),
    Layer.succeed(ClientCapabilities.SshEnvironmentGateway, ssh),
    Layer.succeed(
      ManagedRelay.ManagedRelayClient,
      relayClient(
        options?.connectEnvironment ??
          ((input) =>
            Effect.succeed({
              environmentId: input.environmentId,
              endpoint: ENDPOINT,
              credential: "relay-bootstrap",
              expiresAt: "2026-06-06T00:00:00.000Z",
            })),
      ),
    ),
  );

  return Effect.succeed(ConnectionResolver.layer.pipe(Layer.provide(dependencies)));
});

describe("ConnectionResolver", () => {
  it.effect("prepares a primary environment without remote capabilities", () =>
    Effect.gen(function* () {
      const brokerLayer = yield* makeDependencies();
      const broker = yield* ConnectionResolver.ConnectionResolver.pipe(Effect.provide(brokerLayer));
      const target = new PrimaryConnectionTarget({
        environmentId: ENVIRONMENT_ID,
        label: "Primary",
        httpBaseUrl: "http://127.0.0.1:3777",
        wsBaseUrl: "ws://127.0.0.1:3777",
      });

      expect(yield* broker.prepare(catalogEntry(target))).toEqual({
        environmentId: ENVIRONMENT_ID,
        label: "Primary",
        httpBaseUrl: "http://127.0.0.1:3777",
        socketUrl: "ws://127.0.0.1:3777/ws",
        httpAuthorization: null,
        target,
      });
    }),
  );

  it.effect("authorizes a desktop primary environment with its platform bearer token", () =>
    Effect.gen(function* () {
      const bearerInputs = yield* Ref.make<ReadonlyArray<string>>([]);
      const brokerLayer = yield* makeDependencies({
        primaryBearerToken: "desktop-bearer",
        authorizeBearer: (input) =>
          Ref.update(bearerInputs, (values) => [...values, input.bearerToken]).pipe(
            Effect.as({
              environmentId: input.expectedEnvironmentId,
              label: "Primary",
              httpBaseUrl: input.httpBaseUrl,
              socketUrl: "ws://127.0.0.1:3777/ws?wsTicket=desktop",
              httpAuthorization: {
                _tag: "Bearer" as const,
                token: input.bearerToken,
              },
            }),
          ),
      });
      const broker = yield* ConnectionResolver.ConnectionResolver.pipe(Effect.provide(brokerLayer));
      const target = new PrimaryConnectionTarget({
        environmentId: ENVIRONMENT_ID,
        label: "Primary",
        httpBaseUrl: "http://127.0.0.1:3777",
        wsBaseUrl: "ws://127.0.0.1:3777",
      });

      expect(yield* broker.prepare(catalogEntry(target))).toMatchObject({
        socketUrl: "ws://127.0.0.1:3777/ws?wsTicket=desktop",
        httpAuthorization: { _tag: "Bearer", token: "desktop-bearer" },
        target,
      });
      expect(yield* Ref.get(bearerInputs)).toEqual(["desktop-bearer"]);
    }),
  );

  it.effect("uses the registered bearer profile without re-reading the profile store", () =>
    Effect.gen(function* () {
      const bearerInputs = yield* Ref.make<ReadonlyArray<string>>([]);
      const target = new BearerConnectionTarget({
        environmentId: ENVIRONMENT_ID,
        label: "Saved",
        connectionId: "saved-1",
      });
      const profile = new BearerConnectionProfile({
        connectionId: "saved-1",
        environmentId: ENVIRONMENT_ID,
        label: "Saved",
        httpBaseUrl: ENDPOINT.httpBaseUrl,
        wsBaseUrl: ENDPOINT.wsBaseUrl,
      });
      const brokerLayer = yield* makeDependencies({
        credentials: [["saved-1", new BearerConnectionCredential({ token: "secret-bearer" })]],
        authorizeBearer: (input) =>
          Ref.update(bearerInputs, (values) => [...values, input.bearerToken]).pipe(
            Effect.as({
              environmentId: input.expectedEnvironmentId,
              label: "Saved",
              httpBaseUrl: input.httpBaseUrl,
              socketUrl: "wss://environment.example.test/ws?wsTicket=ticket",
              httpAuthorization: {
                _tag: "Bearer" as const,
                token: input.bearerToken,
              },
            }),
          ),
      });
      const broker = yield* ConnectionResolver.ConnectionResolver.pipe(Effect.provide(brokerLayer));

      expect(
        (yield* broker.prepare(catalogEntry(target, Option.some(profile)))).socketUrl,
      ).toContain("wsTicket=ticket");
      expect(yield* Ref.get(bearerInputs)).toEqual(["secret-bearer"]);
    }),
  );

  it.effect("clears stale desktop Cloudflare Access headers for plain bearer credentials", () =>
    Effect.gen(function* () {
      const installedHeaders = yield* Ref.make<ReadonlyArray<unknown>>([]);
      const target = new BearerConnectionTarget({
        environmentId: ENVIRONMENT_ID,
        label: "Saved",
        connectionId: "saved-1",
      });
      const profile = new BearerConnectionProfile({
        connectionId: "saved-1",
        environmentId: ENVIRONMENT_ID,
        label: "Saved",
        httpBaseUrl: ENDPOINT.httpBaseUrl,
        wsBaseUrl: ENDPOINT.wsBaseUrl,
      });
      const brokerLayer = yield* makeDependencies({
        credentials: [["saved-1", new BearerConnectionCredential({ token: "secret-bearer" })]],
        installCloudflareAccessRequestHeaders: (input) =>
          Ref.update(installedHeaders, (values) => [...values, input]),
      });
      const broker = yield* ConnectionResolver.ConnectionResolver.pipe(Effect.provide(brokerLayer));

      yield* broker.prepare(catalogEntry(target, Option.some(profile)));

      expect(yield* Ref.get(installedHeaders)).toEqual([
        {
          httpBaseUrl: ENDPOINT.httpBaseUrl,
          headers: {},
        },
      ]);
    }),
  );

  it.effect("passes Cloudflare Access service-token credentials during bearer authorization", () =>
    Effect.gen(function* () {
      enableHeaderCapableWebSocketRuntime();
      const accessInputs = yield* Ref.make<ReadonlyArray<unknown>>([]);
      const target = new BearerConnectionTarget({
        environmentId: ENVIRONMENT_ID,
        label: "Saved",
        connectionId: "saved-1",
      });
      const profile = new BearerConnectionProfile({
        connectionId: "saved-1",
        environmentId: ENVIRONMENT_ID,
        label: "Saved",
        httpBaseUrl: ENDPOINT.httpBaseUrl,
        wsBaseUrl: ENDPOINT.wsBaseUrl,
      });
      const brokerLayer = yield* makeDependencies({
        credentials: [
          [
            "saved-1",
            new BearerConnectionCredential({
              token: "secret-bearer",
              cloudflareAccessClientId: "client-id",
              cloudflareAccessClientSecret: "client-secret",
            }),
          ],
        ],
        authorizeBearer: (input) =>
          Ref.update(accessInputs, (values) => [...values, input.cloudflareAccess]).pipe(
            Effect.as({
              environmentId: input.expectedEnvironmentId,
              label: "Saved",
              httpBaseUrl: input.httpBaseUrl,
              socketUrl: "wss://environment.example.test/ws?wsTicket=ticket",
              httpAuthorization: {
                _tag: "Bearer" as const,
                token: input.bearerToken,
              },
            }),
          ),
      });
      const broker = yield* ConnectionResolver.ConnectionResolver.pipe(Effect.provide(brokerLayer));

      yield* broker.prepare(catalogEntry(target, Option.some(profile)));

      expect(yield* Ref.get(accessInputs)).toEqual([
        {
          _tag: "service-token",
          clientId: "client-id",
          clientSecret: "client-secret",
        },
      ]);
    }),
  );

  it.effect(
    "installs service-token headers when the desktop transport installer is available",
    () =>
      Effect.gen(function* () {
        vi.stubGlobal("window", {});
        vi.stubGlobal("navigator", { product: "Gecko" });
        vi.stubGlobal("WebSocket", function WebSocket() {});
        const installedHeaders = yield* Ref.make<
          ReadonlyArray<{
            readonly httpBaseUrl: string;
            readonly headers: Readonly<Record<string, string>>;
          }>
        >([]);
        const target = new BearerConnectionTarget({
          environmentId: ENVIRONMENT_ID,
          label: "Saved",
          connectionId: "saved-1",
        });
        const profile = new BearerConnectionProfile({
          connectionId: "saved-1",
          environmentId: ENVIRONMENT_ID,
          label: "Saved",
          httpBaseUrl: ENDPOINT.httpBaseUrl,
          wsBaseUrl: ENDPOINT.wsBaseUrl,
        });
        const brokerLayer = yield* makeDependencies({
          credentials: [
            [
              "saved-1",
              new BearerConnectionCredential({
                token: "secret-bearer",
                cloudflareAccessClientId: "client-id",
                cloudflareAccessClientSecret: "client-secret",
              }),
            ],
          ],
          installCloudflareAccessRequestHeaders: (input) =>
            Ref.update(installedHeaders, (values) => [...values, input]),
        });
        const broker = yield* ConnectionResolver.ConnectionResolver.pipe(
          Effect.provide(brokerLayer),
        );

        yield* broker.prepare(catalogEntry(target, Option.some(profile)));

        expect(yield* Ref.get(installedHeaders)).toEqual([
          {
            httpBaseUrl: ENDPOINT.httpBaseUrl,
            headers: {
              "cf-access-client-id": "client-id",
              "cf-access-client-secret": "client-secret",
            },
            clearCookies: true,
          },
        ]);
      }),
  );

  it.effect("installs saved Cloudflare Access cookies before bearer authorization", () =>
    Effect.gen(function* () {
      const events = yield* Ref.make<ReadonlyArray<string>>([]);
      const installedCookies = yield* Ref.make<
        ReadonlyArray<{ readonly httpBaseUrl: string; readonly cookieValue: string }>
      >([]);
      const accessInputs = yield* Ref.make<ReadonlyArray<unknown>>([]);
      const target = new BearerConnectionTarget({
        environmentId: ENVIRONMENT_ID,
        label: "Saved",
        connectionId: "saved-1",
      });
      const profile = new BearerConnectionProfile({
        connectionId: "saved-1",
        environmentId: ENVIRONMENT_ID,
        label: "Saved",
        httpBaseUrl: ENDPOINT.httpBaseUrl,
        wsBaseUrl: ENDPOINT.wsBaseUrl,
      });
      const brokerLayer = yield* makeDependencies({
        credentials: [
          [
            "saved-1",
            new BearerConnectionCredential({
              token: "secret-bearer",
              cloudflareAccessCookie: "cf-access-cookie",
            }),
          ],
        ],
        installCloudflareAccessCookie: (input) =>
          Effect.gen(function* () {
            yield* Ref.update(events, (values) => [...values, "install"]);
            yield* Ref.update(installedCookies, (values) => [...values, input]);
          }),
        authorizeBearer: (input) =>
          Effect.gen(function* () {
            yield* Ref.update(events, (values) => [...values, "authorize"]);
            yield* Ref.update(accessInputs, (values) => [...values, input.cloudflareAccess]);
            return {
              environmentId: input.expectedEnvironmentId,
              label: "Saved",
              httpBaseUrl: input.httpBaseUrl,
              socketUrl: "wss://environment.example.test/ws?wsTicket=ticket",
              httpAuthorization: {
                _tag: "Bearer" as const,
                token: input.bearerToken,
              },
            };
          }),
      });
      const broker = yield* ConnectionResolver.ConnectionResolver.pipe(Effect.provide(brokerLayer));

      yield* broker.prepare(catalogEntry(target, Option.some(profile)));

      expect(yield* Ref.get(installedCookies)).toEqual([
        {
          httpBaseUrl: ENDPOINT.httpBaseUrl,
          cookieValue: "cf-access-cookie",
        },
      ]);
      expect(yield* Ref.get(events)).toEqual(["install", "authorize"]);
      expect(yield* Ref.get(accessInputs)).toEqual([
        {
          _tag: "cookie",
          cookieValue: "cf-access-cookie",
        },
      ]);
    }),
  );

  it.effect(
    "installs saved Cloudflare Access JWTs as desktop cookies before bearer authorization",
    () =>
      Effect.gen(function* () {
        const installedCookies = yield* Ref.make<
          ReadonlyArray<{ readonly httpBaseUrl: string; readonly cookieValue: string }>
        >([]);
        const target = new BearerConnectionTarget({
          environmentId: ENVIRONMENT_ID,
          label: "Saved",
          connectionId: "saved-1",
        });
        const profile = new BearerConnectionProfile({
          connectionId: "saved-1",
          environmentId: ENVIRONMENT_ID,
          label: "Saved",
          httpBaseUrl: ENDPOINT.httpBaseUrl,
          wsBaseUrl: ENDPOINT.wsBaseUrl,
        });
        const brokerLayer = yield* makeDependencies({
          credentials: [
            [
              "saved-1",
              new BearerConnectionCredential({
                token: "secret-bearer",
                cloudflareAccessToken: "cf-access-jwt",
              }),
            ],
          ],
          installCloudflareAccessCookie: (input) =>
            Ref.update(installedCookies, (values) => [...values, input]),
        });
        const broker = yield* ConnectionResolver.ConnectionResolver.pipe(
          Effect.provide(brokerLayer),
        );

        yield* broker.prepare(catalogEntry(target, Option.some(profile)));

        expect(yield* Ref.get(installedCookies)).toEqual([
          {
            httpBaseUrl: ENDPOINT.httpBaseUrl,
            cookieValue: "cf-access-jwt",
          },
        ]);
      }),
  );

  it.effect(
    "blocks saved Cloudflare Access service-token credentials in browser websocket runtimes",
    () =>
      Effect.gen(function* () {
        vi.stubGlobal("window", {});
        vi.stubGlobal("navigator", { product: "Gecko" });
        const target = new BearerConnectionTarget({
          environmentId: ENVIRONMENT_ID,
          label: "Saved",
          connectionId: "saved-1",
        });
        const profile = new BearerConnectionProfile({
          connectionId: "saved-1",
          environmentId: ENVIRONMENT_ID,
          label: "Saved",
          httpBaseUrl: ENDPOINT.httpBaseUrl,
          wsBaseUrl: ENDPOINT.wsBaseUrl,
        });
        const brokerLayer = yield* makeDependencies({
          credentials: [
            [
              "saved-1",
              new BearerConnectionCredential({
                token: "secret-bearer",
                cloudflareAccessClientId: "client-id",
                cloudflareAccessClientSecret: "client-secret",
              }),
            ],
          ],
          authorizeBearer: () => Effect.die("authorizeBearer should not be called"),
        });
        const broker = yield* ConnectionResolver.ConnectionResolver.pipe(
          Effect.provide(brokerLayer),
        );

        const error = yield* broker
          .prepare(catalogEntry(target, Option.some(profile)))
          .pipe(Effect.flip);

        expect(error).toMatchObject({
          _tag: "ConnectionBlockedError",
          reason: "unsupported",
          message: expect.stringContaining("WebSocket headers"),
        });
      }),
  );

  it.effect("brokers relay credentials with the current cloud session and device identity", () =>
    Effect.gen(function* () {
      const relayInputs = yield* Ref.make<
        ReadonlyArray<{
          readonly clerkToken: string;
          readonly scopes: ReadonlyArray<string>;
          readonly deviceId?: string;
        }>
      >([]);
      const bootstrapCredentials = yield* Ref.make<ReadonlyArray<string>>([]);
      const target = new RelayConnectionTarget({
        environmentId: ENVIRONMENT_ID,
        label: "Cloud",
      });
      const brokerLayer = yield* makeDependencies({
        connectEnvironment: (input) =>
          Ref.update(relayInputs, (values) => [
            ...values,
            {
              clerkToken: input.clerkToken,
              scopes: input.scopes,
              ...(input.deviceId ? { deviceId: input.deviceId } : {}),
            },
          ]).pipe(
            Effect.as({
              environmentId: input.environmentId,
              endpoint: ENDPOINT,
              credential: "relay-bootstrap",
              expiresAt: "2026-06-06T00:00:00.000Z",
            }),
          ),
        authorizeDpop: (input) =>
          input.obtainBootstrap.pipe(
            Effect.tap((bootstrap) =>
              Ref.update(bootstrapCredentials, (values) => [...values, bootstrap.credential]),
            ),
            Effect.as({
              environmentId: input.expectedEnvironmentId,
              label: "Cloud",
              httpBaseUrl: ENDPOINT.httpBaseUrl,
              socketUrl: "wss://environment.example.test/ws?wsTicket=dpop",
              httpAuthorization: {
                _tag: "Dpop" as const,
                accessToken: "dpop-access-token",
              },
            }),
          ),
      });
      const broker = yield* ConnectionResolver.ConnectionResolver.pipe(Effect.provide(brokerLayer));

      expect((yield* broker.prepare(catalogEntry(target))).socketUrl).toContain("wsTicket=dpop");
      expect(yield* Ref.get(relayInputs)).toEqual([
        {
          clerkToken: "clerk-session",
          scopes: [RelayEnvironmentConnectScope],
          deviceId: "device-1",
        },
      ]);
      expect(yield* Ref.get(bootstrapCredentials)).toEqual(["relay-bootstrap"]);
    }),
  );

  it.effect("exports the complete relay authorization flow through the product tracer", () =>
    Effect.gen(function* () {
      const userSpans: Array<string> = [];
      const productSpans: Array<string> = [];
      const target = new RelayConnectionTarget({
        environmentId: ENVIRONMENT_ID,
        label: "Cloud",
      });
      const brokerLayer = yield* makeDependencies({
        authorizeDpop: (input) =>
          input.obtainBootstrap.pipe(
            Effect.as({
              environmentId: input.expectedEnvironmentId,
              label: "Cloud",
              httpBaseUrl: ENDPOINT.httpBaseUrl,
              socketUrl: "wss://environment.example.test/ws?wsTicket=dpop",
              httpAuthorization: {
                _tag: "Dpop" as const,
                accessToken: "dpop-access-token",
              },
            }),
            Effect.withSpan("test.remote.authorizeDpop"),
          ),
      });
      const broker = yield* ConnectionResolver.ConnectionResolver.pipe(Effect.provide(brokerLayer));

      yield* broker
        .prepare(catalogEntry(target))
        .pipe(
          Effect.provideService(RelayClientTracer, Option.some(collectingTracer(productSpans))),
          Effect.withTracer(collectingTracer(userSpans)),
        );

      expect(productSpans).toContain("clientRuntime.connection.broker.relay");
      expect(productSpans).toContain("test.remote.authorizeDpop");
      expect(userSpans).toContain("clientRuntime.connection.broker.prepare");
      expect(userSpans).not.toContain("test.remote.authorizeDpop");
    }),
  );

  it.effect("delegates SSH launch to the platform gateway before remote authorization", () =>
    Effect.gen(function* () {
      const preparedTargets = yield* Ref.make<ReadonlyArray<DesktopSshEnvironmentTarget>>([]);
      const target = new SshConnectionTarget({
        environmentId: ENVIRONMENT_ID,
        label: "SSH",
        connectionId: "ssh-1",
      });
      const profile = new SshConnectionProfile({
        connectionId: "ssh-1",
        environmentId: ENVIRONMENT_ID,
        label: "SSH",
        target: SSH_TARGET,
      });
      const brokerLayer = yield* makeDependencies({
        prepareSsh: (input) =>
          Ref.update(preparedTargets, (values) => [...values, input.target]).pipe(
            Effect.as({
              bootstrap: {
                target: input.target,
                httpBaseUrl: "http://127.0.0.1:4010",
                wsBaseUrl: "ws://127.0.0.1:4010",
                pairingToken: null,
              },
              bearerToken: "ssh-bearer",
            }),
          ),
      });
      const broker = yield* ConnectionResolver.ConnectionResolver.pipe(Effect.provide(brokerLayer));

      expect(
        (yield* broker.prepare(catalogEntry(target, Option.some(profile)))).socketUrl,
      ).toContain("wsTicket=bearer");
      expect(yield* Ref.get(preparedTargets)).toEqual([SSH_TARGET]);
    }),
  );

  it.effect("classifies relay request timeouts as retryable connection failures", () =>
    Effect.gen(function* () {
      const target = new RelayConnectionTarget({
        environmentId: ENVIRONMENT_ID,
        label: "Cloud",
      });
      const brokerLayer = yield* makeDependencies({
        connectEnvironment: () =>
          Effect.fail(
            new ManagedRelay.ManagedRelayRequestTimeoutError({
              activity: "Relay environment connection",
              timeoutMs: ManagedRelay.MANAGED_RELAY_REQUEST_TIMEOUT_MS,
            }),
          ),
      });
      const broker = yield* ConnectionResolver.ConnectionResolver.pipe(Effect.provide(brokerLayer));
      const error = yield* Effect.flip(broker.prepare(catalogEntry(target)));

      expect(error).toBeInstanceOf(ConnectionTransientError);
      expect(error).toMatchObject({ reason: "timeout" });
    }),
  );
});
