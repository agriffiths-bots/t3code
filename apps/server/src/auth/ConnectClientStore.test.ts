import { expect, it } from "@effect/vitest";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";

import * as AuthConnectClients from "../persistence/AuthConnectClients.ts";
import * as ServerSecretStore from "./ServerSecretStore.ts";
import * as ConnectClientStore from "./ConnectClientStore.ts";

const textEncoder = new TextEncoder();
const requestedAt = DateTime.makeUnsafe("2026-06-27T12:00:00.000Z");
const approvedAt = DateTime.makeUnsafe("2026-06-27T12:05:00.000Z");
const rejectedAt = DateTime.makeUnsafe("2026-06-27T12:06:00.000Z");

const approvedRecord: AuthConnectClients.AuthConnectClientRecord = {
  clientProofKeyThumbprint: "client-thumbprint",
  cloudUserId: "cloud-user",
  deviceId: "device-1",
  status: "approved",
  client: {
    label: "Client",
    ipAddress: null,
    userAgent: null,
    deviceType: "desktop",
    os: "macOS",
    browser: null,
  },
  requestedAt,
  updatedAt: approvedAt,
  approvedAt,
  rejectedAt: null,
  revokedAt: null,
  lastSeenAt: null,
};

const secretStoreLayer = Layer.succeed(
  ServerSecretStore.ServerSecretStore,
  ServerSecretStore.ServerSecretStore.of({
    get: () => Effect.succeed(Option.some(textEncoder.encode("client-approval"))),
    set: () => Effect.void,
    create: () => Effect.void,
    getOrCreateRandom: () => Effect.succeed(new Uint8Array()),
    remove: () => Effect.void,
  }),
);

const makeStoreLayer = (
  overrides: Partial<AuthConnectClients.AuthConnectClientRepository["Service"]>,
) =>
  Layer.effect(ConnectClientStore.ConnectClientStore, ConnectClientStore.make).pipe(
    Layer.provide(secretStoreLayer),
    Layer.provide(
      Layer.succeed(
        AuthConnectClients.AuthConnectClientRepository,
        AuthConnectClients.AuthConnectClientRepository.of({
          upsertRequest: () => Effect.succeed(approvedRecord),
          updateStatus: () => Effect.succeed(Option.none()),
          revoke: () => Effect.succeed(false),
          markSeen: () => Effect.succeed(Option.some(approvedRecord)),
          listActive: () => Effect.succeed([]),
          ...overrides,
        }),
      ),
    ),
  );

it.effect("returns rejected when an approved client is rejected before last-seen update", () =>
  Effect.gen(function* () {
    const store = yield* ConnectClientStore.ConnectClientStore;
    const authorization = yield* store.requestClient({
      cloudUserId: "cloud-user",
      clientProofKeyThumbprint: "client-thumbprint",
    });

    expect(authorization.mode).toBe("client-approval");
    expect(authorization.status).toBe("rejected");
  }).pipe(
    Effect.provide(
      makeStoreLayer({
        markSeen: () =>
          Effect.succeed(
            Option.some({
              ...approvedRecord,
              status: "rejected",
              updatedAt: rejectedAt,
              rejectedAt,
            }),
          ),
      }),
    ),
  ),
);

it.effect("returns rejected when an approved client is revoked before last-seen update", () =>
  Effect.gen(function* () {
    const store = yield* ConnectClientStore.ConnectClientStore;
    const authorization = yield* store.requestClient({
      cloudUserId: "cloud-user",
      clientProofKeyThumbprint: "client-thumbprint",
    });

    expect(authorization.mode).toBe("client-approval");
    expect(authorization.status).toBe("rejected");
    if (authorization.mode === "client-approval") {
      expect(authorization.client.status).toBe("rejected");
      expect(authorization.client.approvedAt).toBeNull();
      expect(authorization.client.lastSeenAt).toBeNull();
    }
  }).pipe(
    Effect.provide(
      makeStoreLayer({
        markSeen: () => Effect.succeed(Option.none()),
      }),
    ),
  ),
);
