import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";

import {
  ServerNotificationRegisterInput,
  ServerProvider,
  ServerWebPushSubscription,
} from "./server.ts";

const decodeServerProvider = Schema.decodeUnknownSync(ServerProvider);
const decodeNotificationRegisterInput = Schema.decodeUnknownSync(ServerNotificationRegisterInput);
const decodeWebPushSubscription = Schema.decodeUnknownSync(ServerWebPushSubscription);

function webPushSubscription(endpoint: string) {
  return {
    endpoint,
    expirationTime: null,
    keys: {
      p256dh: "p256dh-key",
      auth: "auth-key",
    },
  };
}

describe("ServerProvider", () => {
  it("defaults capability arrays when decoding provider snapshots", () => {
    const parsed = decodeServerProvider({
      instanceId: "codex",
      driver: "codex",
      enabled: true,
      installed: true,
      version: "1.0.0",
      status: "ready",
      auth: {
        status: "authenticated",
      },
      checkedAt: "2026-04-10T00:00:00.000Z",
      models: [],
    });

    expect(parsed.slashCommands).toEqual([]);
    expect(parsed.skills).toEqual([]);
    expect(parsed.versionAdvisory).toBeUndefined();
    expect(parsed.updateState).toBeUndefined();
  });

  it("defaults one-click update support when decoding older advisory snapshots", () => {
    const parsed = decodeServerProvider({
      instanceId: "codex",
      driver: "codex",
      enabled: true,
      installed: true,
      version: "1.0.0",
      status: "ready",
      auth: {
        status: "authenticated",
      },
      checkedAt: "2026-04-10T00:00:00.000Z",
      models: [],
      versionAdvisory: {
        status: "behind_latest",
        currentVersion: "1.0.0",
        latestVersion: "1.0.1",
        updateCommand: "npm install -g @openai/codex@latest",
        checkedAt: "2026-04-10T00:00:00.000Z",
        message: "Update available.",
      },
    });

    expect(parsed.versionAdvisory?.canUpdate).toBe(false);
  });

  it("decodes continuation group metadata", () => {
    const parsed = decodeServerProvider({
      instanceId: "codex_personal",
      driver: "codex",
      continuation: { groupKey: "codex:home:/Users/julius/.codex" },
      enabled: true,
      installed: true,
      version: "1.0.0",
      status: "ready",
      auth: {
        status: "authenticated",
      },
      checkedAt: "2026-04-10T00:00:00.000Z",
      models: [],
    });

    expect(parsed.continuation?.groupKey).toBe("codex:home:/Users/julius/.codex");
  });
});

describe("ServerWebPushSubscription", () => {
  it("accepts public HTTPS push-service endpoints", () => {
    const parsed = decodeWebPushSubscription(
      webPushSubscription("https://updates.push.services.mozilla.com/wpush/v2/test"),
    );

    expect(parsed.endpoint).toBe("https://updates.push.services.mozilla.com/wpush/v2/test");
  });

  it("rejects non-HTTPS and non-public endpoints", () => {
    const blockedEndpoints = [
      "http://updates.push.services.mozilla.com/wpush/v2/test",
      "https://localhost/wpush/v2/test",
      "https://127.0.0.1/wpush/v2/test",
      "https://10.0.0.1/wpush/v2/test",
      "https://172.16.0.1/wpush/v2/test",
      "https://192.168.0.1/wpush/v2/test",
      "https://169.254.169.254/latest/meta-data",
      "https://[::1]/wpush/v2/test",
      "https://[fc00::1]/wpush/v2/test",
      "https://[fe80::1]/wpush/v2/test",
    ];

    for (const endpoint of blockedEndpoints) {
      expect(() => decodeWebPushSubscription(webPushSubscription(endpoint)), endpoint).toThrow();
    }
  });

  it("rejects unsafe endpoints through notification registration input", () => {
    expect(() =>
      decodeNotificationRegisterInput({
        deviceId: "device-1",
        deviceKind: "web-push",
        subscription: webPushSubscription("http://169.254.169.254/latest/meta-data"),
      }),
    ).toThrow();
  });
});
