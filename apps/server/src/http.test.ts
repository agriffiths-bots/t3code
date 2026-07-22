import { EnvironmentId } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import type { SubagentPeer } from "./subagents/SubagentPeerRegistry.ts";
import { __assetRelayTesting, isLoopbackHostname, resolveDevRedirectUrl } from "./http.ts";

const peer = (environmentId: string, overrides: Partial<SubagentPeer> = {}): SubagentPeer => ({
  alias: `peer-${environmentId}`,
  environmentId: EnvironmentId.make(environmentId),
  httpBaseUrl: `https://${environmentId}.example.test`,
  mcpEndpoint: `https://${environmentId}.example.test/mcp`,
  credential: { _tag: "bearer", token: `token-${environmentId}` },
  pairedAt: "2026-07-22T00:00:00.000Z",
  ...overrides,
});

describe("http dev routing", () => {
  it("treats localhost and loopback addresses as local", () => {
    expect(isLoopbackHostname("127.0.0.1")).toBe(true);
    expect(isLoopbackHostname("localhost")).toBe(true);
    expect(isLoopbackHostname("::1")).toBe(true);
    expect(isLoopbackHostname("[::1]")).toBe(true);
  });

  it("does not treat LAN addresses as local", () => {
    expect(isLoopbackHostname("192.168.86.35")).toBe(false);
    expect(isLoopbackHostname("10.0.0.24")).toBe(false);
    expect(isLoopbackHostname("example.local")).toBe(false);
  });

  it("preserves path and query when redirecting to the dev server", () => {
    const devUrl = new URL("http://127.0.0.1:5173/");
    const requestUrl = new URL("http://127.0.0.1:3774/pair?token=test-token");

    expect(resolveDevRedirectUrl(devUrl, requestUrl)).toBe(
      "http://127.0.0.1:5173/pair?token=test-token",
    );
  });
});

describe("asset app relay routing", () => {
  it("selects only the peer whose environment id issued the claim", () => {
    const peers = [peer("backend-a"), peer("backend-b")];

    expect(__assetRelayTesting.selectAssetRelayPeer(peers, "backend-b")).toBe(peers[1]);
    expect(__assetRelayTesting.selectAssetRelayPeer(peers, "missing")).toBeUndefined();
  });

  it("uses only server-to-server bearer and service-token headers", () => {
    expect(
      __assetRelayTesting.trustedAssetRelayHeaders(
        peer("backend-a", {
          cfAccess: {
            _tag: "service-token",
            clientId: "service-client",
            clientSecret: "service-secret",
          },
        }),
      ),
    ).toEqual({
      accept: "application/octet-stream",
      authorization: "Bearer token-backend-a",
      "cf-access-client-id": "service-client",
      "cf-access-client-secret": "service-secret",
    });
  });

  it("rejects credential references and browser-bound Cloudflare credentials", () => {
    expect(
      __assetRelayTesting.trustedAssetRelayHeaders(
        peer("credential-ref", {
          credential: { _tag: "credential-ref", ref: "op://vault/item/token" },
        }),
      ),
    ).toBeNull();
    expect(
      __assetRelayTesting.trustedAssetRelayHeaders(
        peer("cookie", { cfAccess: { _tag: "cookie", cookieValue: "browser-cookie" } }),
      ),
    ).toBeNull();
    expect(
      __assetRelayTesting.trustedAssetRelayHeaders(
        peer("jwt", { cfAccess: { _tag: "jwt", jwt: "browser-jwt" } }),
      ),
    ).toBeNull();
  });

  it("forwards valid lengths only for unencoded upstream bodies", () => {
    expect(__assetRelayTesting.relayContentLength({ "content-length": "42" })).toBe(42);
    expect(
      __assetRelayTesting.relayContentLength({
        "content-encoding": "gzip",
        "content-length": "24",
      }),
    ).toBeUndefined();
    expect(__assetRelayTesting.relayContentLength({ "content-length": "invalid" })).toBeUndefined();
  });
});
