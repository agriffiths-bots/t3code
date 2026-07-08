import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  AuthAccessTokenType,
  EnvironmentId,
  ExecutionEnvironmentDescriptor,
} from "@t3tools/contracts";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as TestClock from "effect/testing/TestClock";
import { HttpClient, HttpClientRequest, HttpClientResponse } from "effect/unstable/http";

import { __testing, pairPeer, PeerCommandInputError, PeerPairingError } from "./peers.ts";
import * as SubagentPeerRegistry from "../subagents/SubagentPeerRegistry.ts";

const bearerCredential = new SubagentPeerRegistry.SubagentPeerBearerCredential({
  token: "peer-token",
});

const existingPeer: SubagentPeerRegistry.SubagentPeer = {
  alias: "vps",
  environmentId: EnvironmentId.make("env-vps"),
  httpBaseUrl: "https://peer.example/",
  mcpEndpoint: "https://peer.example/mcp",
  credential: bearerCredential,
  pairedAt: "2026-07-08T10:00:00.000Z",
};

const registryService = (
  overrides: Partial<SubagentPeerRegistry.SubagentPeerRegistry["Service"]> = {},
) =>
  ({
    add: () => Effect.die("unexpected add"),
    list: Effect.succeed([]),
    remove: () => Effect.die("unexpected remove"),
    getByAlias: () => Effect.succeed(Option.none()),
    resolveTarget: () => Effect.die("unexpected resolveTarget"),
    updateLastSeen: () => Effect.die("unexpected updateLastSeen"),
    ...overrides,
  }) satisfies SubagentPeerRegistry.SubagentPeerRegistry["Service"];

it.layer(NodeServices.layer)("peers CLI helpers", (it) => {
  it.effect("preflights duplicate aliases before exchanging a remote pairing token", () =>
    Effect.gen(function* () {
      let httpCalls = 0;
      const httpLayer = Layer.succeed(
        HttpClient.HttpClient,
        HttpClient.make(() =>
          Effect.sync(() => {
            httpCalls += 1;
          }).pipe(Effect.andThen(Effect.die("unexpected HTTP call"))),
        ),
      );
      const registry = registryService({
        getByAlias: () => Effect.succeed(Option.some(existingPeer)),
      });

      const error = yield* Effect.flip(
        pairPeer(registry, {
          alias: " vps ",
          pairingUrl: "https://peer.example/pair#token=pair-token",
        }).pipe(Effect.provide(httpLayer)),
      );

      assert.instanceOf(error, SubagentPeerRegistry.SubagentPeerAliasExistsError);
      assert.equal(error.alias, "vps");
      assert.equal(httpCalls, 0);
    }),
  );

  it.effect(
    "preflights invalid MCP endpoint overrides before exchanging a remote pairing token",
    () =>
      Effect.gen(function* () {
        let httpCalls = 0;
        const httpLayer = Layer.succeed(
          HttpClient.HttpClient,
          HttpClient.make(() =>
            Effect.sync(() => {
              httpCalls += 1;
            }).pipe(Effect.andThen(Effect.die("unexpected HTTP call"))),
          ),
        );
        const registry = registryService({
          getByAlias: () => Effect.succeed(Option.none()),
        });

        const error = yield* Effect.flip(
          pairPeer(registry, {
            alias: "vps",
            pairingUrl: "https://peer.example/pair#token=pair-token",
            mcpEndpoint: "not a url",
          }).pipe(Effect.provide(httpLayer)),
        );

        assert.instanceOf(error, PeerCommandInputError);
        assert.equal(httpCalls, 0);
      }),
  );

  it.effect("rejects partial Cloudflare Access service-token flags", () =>
    Effect.gen(function* () {
      const error = yield* Effect.flip(
        __testing.cloudflareAccessFromInput({
          cloudflareAccessClientId: "client-id",
        }),
      );

      assert.instanceOf(error, PeerCommandInputError);
      assert.include(error.message, "--cf-access-client-id and --cf-access-client-secret");
    }),
  );

  it.effect("prefers durable Cloudflare Access service-token credentials over JWTs", () =>
    Effect.gen(function* () {
      const access = yield* __testing.cloudflareAccessFromInput({
        cloudflareAccessToken: "short-lived-jwt",
        cloudflareAccessClientId: "client-id",
        cloudflareAccessClientSecret: "client-secret",
      });

      assert.deepEqual(access, {
        _tag: "service-token",
        clientId: "client-id",
        clientSecret: "client-secret",
      });
    }),
  );

  it("uses manual redirects for peer pairing fetch requests", () => {
    assert.equal(__testing.peerPairingFetchRequestInit.redirect, "manual");
  });

  it.effect("passes MCP endpoint overrides through pairing registration", () =>
    Effect.gen(function* () {
      let capturedAdd: SubagentPeerRegistry.SubagentPeerAddInput | undefined;
      const registry = registryService({
        getByAlias: () => Effect.succeed(Option.none()),
        add: (input) =>
          Effect.sync(() => {
            capturedAdd = input;
            return {
              alias: input.alias,
              environmentId: EnvironmentId.make(input.environmentId),
              httpBaseUrl: input.httpBaseUrl,
              mcpEndpoint: input.mcpEndpoint ?? "https://peer.example/mcp",
              credential: input.credential,
              ...(input.cfAccess ? { cfAccess: input.cfAccess } : {}),
              pairedAt: "2026-07-08T10:00:00.000Z",
            };
          }),
      });
      const httpLayer = Layer.succeed(
        HttpClient.HttpClient,
        HttpClient.make((request) => {
          const response = request.url.endsWith("/.well-known/t3/environment")
            ? Response.json({
                environmentId: "env-peer",
                label: "Peer",
                platform: { os: "linux", arch: "x64" },
                serverVersion: "0.0.0-test",
                capabilities: { repositoryIdentity: true },
              })
            : Response.json({
                access_token: "peer-access-token",
                issued_token_type: AuthAccessTokenType,
                token_type: "Bearer",
                expires_in: 3600,
                scope: "standard",
              });
          return Effect.succeed(HttpClientResponse.fromWeb(request, response));
        }),
      );

      yield* pairPeer(registry, {
        alias: "vps",
        pairingUrl: "https://peer.example/pair#token=pair-token",
        mcpEndpoint: "https://peer.example/custom-mcp",
      }).pipe(Effect.provide(httpLayer));

      assert.equal(capturedAdd?.mcpEndpoint, "https://peer.example/custom-mcp");
    }),
  );

  it.effect("times out peer HTTP requests that never respond", () =>
    Effect.gen(function* () {
      const httpLayer = Layer.succeed(
        HttpClient.HttpClient,
        HttpClient.make(() => Effect.never),
      );

      const errorFiber = yield* __testing
        .responseJson(
          HttpClientRequest.get("https://peer.example/.well-known/t3/environment"),
          ExecutionEnvironmentDescriptor,
          "fetch-descriptor",
          Duration.millis(1),
        )
        .pipe(Effect.provide(httpLayer), Effect.flip, Effect.forkScoped);
      yield* TestClock.adjust(Duration.millis(1));
      const error = yield* Fiber.join(errorFiber);

      assert.instanceOf(error, PeerPairingError);
      assert.equal(error.operation, "fetch-descriptor");
    }).pipe(Effect.provide(TestClock.layer())),
  );
});
