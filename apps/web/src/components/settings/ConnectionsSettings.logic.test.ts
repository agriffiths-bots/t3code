import type { DesktopWslState, MatrixBridgeStatus } from "@t3tools/contracts";
import { ThreadId } from "@t3tools/contracts";
import { describe, expect, it, vi } from "vite-plus/test";
import {
  activeMatrixBridgePairingCode,
  applyWslEnableSelection,
  matrixBridgeConnectionMode,
  EMPTY_MATRIX_BRIDGE_DRAFT,
  matrixBridgeDraftAfterConfigure,
  matrixBridgeDraftFromSavedConfig,
  matrixBridgeSectionAccess,
  matrixBridgeStatusLabel,
  parseMatrixBridgeConfigureInput,
  parsePairingUrlFields,
  parseRemotePairingHostChange,
  parseRemotePairingFields,
  showMatrixBridgeDisconnect,
} from "./ConnectionsSettings.logic";

const baseWslState: DesktopWslState = {
  enabled: false,
  distro: null,
  available: true,
  wslOnly: true,
  distros: [],
  preflightError: null,
};

describe("applyWslEnableSelection", () => {
  it("clears WSL-only and updates the distro before enabling both backends", async () => {
    const calls: Array<string> = [];
    let persistedWslOnly = true;
    let persistedDistro: string | null = "Ubuntu";
    const setWslDistro = vi.fn(async (distro: string | null) => {
      calls.push(`setWslDistro:${distro ?? "default"}`);
      persistedDistro = distro;
      return { ...baseWslState, distro, wslOnly: persistedWslOnly };
    });
    const setWslBackendEnabled = vi.fn(async (enabled: boolean) => {
      calls.push(`setWslBackendEnabled:${enabled}`);
      return {
        ...baseWslState,
        enabled,
        distro: persistedDistro,
        wslOnly: persistedWslOnly,
      };
    });
    const setWslOnly = vi.fn(async (enabled: boolean) => {
      calls.push(`setWslOnly:${enabled}`);
      persistedWslOnly = enabled;
      return { ...baseWslState, distro: persistedDistro, wslOnly: enabled };
    });

    const state = await applyWslEnableSelection({
      bridge: { setWslDistro, setWslBackendEnabled, setWslOnly },
      mode: "both",
      nextDistro: "Debian",
      persistedDistro: "Ubuntu",
    });

    expect(calls).toEqual(["setWslOnly:false", "setWslDistro:Debian", "setWslBackendEnabled:true"]);
    expect(state).toMatchObject({ enabled: true, distro: "Debian", wslOnly: false });
  });

  it("stages WSL-only before enabling without rewriting an unchanged distro", async () => {
    const calls: Array<string> = [];
    let persistedWslOnly = false;
    const setWslDistro = vi.fn(async () => baseWslState);
    const setWslOnly = vi.fn(async (enabled: boolean) => {
      calls.push(`setWslOnly:${enabled}`);
      persistedWslOnly = enabled;
      return { ...baseWslState, wslOnly: enabled };
    });
    const setWslBackendEnabled = vi.fn(async (enabled: boolean) => {
      calls.push(`setWslBackendEnabled:${enabled}`);
      return { ...baseWslState, enabled, wslOnly: persistedWslOnly };
    });

    const state = await applyWslEnableSelection({
      bridge: { setWslDistro, setWslBackendEnabled, setWslOnly },
      mode: "wsl-only",
      nextDistro: null,
      persistedDistro: null,
    });

    expect(calls).toEqual(["setWslOnly:true", "setWslBackendEnabled:true"]);
    expect(setWslDistro).not.toHaveBeenCalled();
    expect(state).toMatchObject({ enabled: true, wslOnly: true });
  });
});

describe("remote pairing field parsing", () => {
  it("preserves Cloudflare Access service-token fields from pairing URL fragments", () => {
    expect(
      parsePairingUrlFields(
        "https://remote.example.test/pair#token=pairing-token&cf_access_client_id=client-id&cf_access_client_secret=client-secret",
      ),
    ).toEqual({
      host: "https://remote.example.test/",
      pairingCode: "pairing-token",
      cloudflareAccessClientId: "client-id",
      cloudflareAccessClientSecret: "client-secret",
    });
  });

  it("clears stale Cloudflare Access fields when host changes to a manual host", () => {
    expect(parseRemotePairingHostChange("remote.example.test")).toEqual({
      host: "remote.example.test",
      cloudflareAccessToken: "",
      cloudflareAccessClientId: "",
      cloudflareAccessClientSecret: "",
    });
  });

  it("repopulates Cloudflare Access fields when host changes to a pairing URL", () => {
    expect(
      parseRemotePairingHostChange(
        "https://remote.example.test/pair#token=pairing-token&cf_access_client_id=client-id&cf_access_client_secret=client-secret",
      ),
    ).toEqual({
      host: "https://remote.example.test/",
      pairingCode: "pairing-token",
      cloudflareAccessToken: "",
      cloudflareAccessClientId: "client-id",
      cloudflareAccessClientSecret: "client-secret",
    });
  });

  it("preserves Cloudflare Access JWT fields from hosted pairing URL fragments", () => {
    expect(
      parsePairingUrlFields(
        "https://app.t3.codes/pair?host=https%3A%2F%2Fdesktop.tailnet.test%3A443#token=pairing-token&cf_access_token=cf-jwt",
      ),
    ).toEqual({
      host: "https://desktop.tailnet.test/",
      pairingCode: "pairing-token",
      cloudflareAccessToken: "cf-jwt",
    });
  });

  it("passes explicit Cloudflare Access service-token fields with manual host pairing", () => {
    expect(
      parseRemotePairingFields({
        host: "remote.example.test",
        pairingCode: "pairing-token",
        cloudflareAccessClientId: " client-id ",
        cloudflareAccessClientSecret: " client-secret ",
      }),
    ).toEqual({
      host: "remote.example.test",
      pairingCode: "pairing-token",
      cloudflareAccessClientId: "client-id",
      cloudflareAccessClientSecret: "client-secret",
    });
  });

  it("rejects incomplete explicit Cloudflare Access service-token fields", () => {
    expect(() =>
      parseRemotePairingFields({
        host: "remote.example.test",
        pairingCode: "pairing-token",
        cloudflareAccessClientId: "client-id",
      }),
    ).toThrowError("Enter both Cloudflare Access service token fields.");
  });
});

describe("matrixBridgeSectionAccess", () => {
  it("hides the subsection on servers without the bridge capability", () => {
    expect(matrixBridgeSectionAccess({ supported: false, canManageAccess: true })).toBe("hidden");
  });

  it("shows status without configuration to clients lacking access:write", () => {
    expect(matrixBridgeSectionAccess({ supported: true, canManageAccess: false })).toBe(
      "status-only",
    );
  });

  it("allows configuration for clients with access:write", () => {
    expect(matrixBridgeSectionAccess({ supported: true, canManageAccess: true })).toBe("manage");
  });
});

describe("matrixBridgeStatusLabel", () => {
  const baseStatus: MatrixBridgeStatus = {
    state: "disabled",
    ownerThreadId: null,
    encryptionReady: false,
    reason: null,
  };
  const statusView = (status: MatrixBridgeStatus) => ({ kind: "status", status }) as const;

  it("waits rather than claiming the bridge is off before a snapshot arrives", () => {
    expect(matrixBridgeStatusLabel({ kind: "pending" }).title).toBe("Checking");
    expect(matrixBridgeStatusLabel(statusView(baseStatus)).title).toBe("Not connected");
  });

  it("says the status cannot be read rather than reporting it as off", () => {
    expect(matrixBridgeStatusLabel({ kind: "unavailable" }).title).toBe("Status unavailable");
  });

  it("names each lifecycle state", () => {
    expect(matrixBridgeStatusLabel(statusView({ ...baseStatus, state: "connecting" })).title).toBe(
      "Connecting",
    );
    expect(
      matrixBridgeStatusLabel(statusView({ ...baseStatus, state: "waiting-for-member" })).title,
    ).toBe("Waiting for you to join");
    expect(
      matrixBridgeStatusLabel(statusView({ ...baseStatus, state: "awaiting-pairing" })).title,
    ).toBe("Waiting for a pairing code");
    expect(
      matrixBridgeStatusLabel(
        statusView({
          ...baseStatus,
          state: "active",
          encryptionReady: true,
          ownerThreadId: ThreadId.make("thread-a"),
        }),
      ).title,
    ).toBe("Active");
  });

  it("prefers the server's sanitized reason for unhealthy states", () => {
    expect(
      matrixBridgeStatusLabel(
        statusView({
          ...baseStatus,
          state: "degraded",
          reason: "An unexpected member joined the room.",
        }),
      ).description,
    ).toBe("An unexpected member joined the room.");
    expect(
      matrixBridgeStatusLabel(statusView({ ...baseStatus, state: "unavailable" })).description,
    ).toBe("The bridge cannot run on this server right now.");
  });
});

describe("matrixBridgeConnectionMode", () => {
  const baseStatus: MatrixBridgeStatus = {
    state: "disabled",
    ownerThreadId: null,
    encryptionReady: false,
    reason: null,
  };

  it("offers a first connection only when the status says there is none", () => {
    expect(matrixBridgeConnectionMode({ kind: "status", status: baseStatus })).toBe("connect");
    expect(showMatrixBridgeDisconnect("connect")).toBe(false);
  });

  it("keeps disconnect available to a client that cannot read the status", () => {
    // access:write without orchestration:read is a legitimate split: that
    // session may configure and disconnect even though it cannot watch.
    expect(matrixBridgeConnectionMode({ kind: "unavailable" })).toBe("unknown");
    expect(matrixBridgeConnectionMode({ kind: "pending" })).toBe("unknown");
    expect(showMatrixBridgeDisconnect("unknown")).toBe(true);
  });

  it("replaces the stored connection once one exists", () => {
    expect(
      matrixBridgeConnectionMode({
        kind: "status",
        status: { ...baseStatus, state: "awaiting-pairing" },
      }),
    ).toBe("reconfigure");
    expect(showMatrixBridgeDisconnect("reconfigure")).toBe(true);
  });
});

describe("Matrix bridge connection form", () => {
  it("requires the bot token on every save, since the server never returns it", () => {
    expect(() =>
      parseMatrixBridgeConfigureInput({
        homeserverUrl: "https://matrix.example.test",
        accessToken: "  ",
        allowedUserIds: "@you:beeper.com",
      }),
    ).toThrowError("Enter the bot access token.");
  });

  it("rejects text that is not a Matrix user ID", () => {
    expect(() =>
      parseMatrixBridgeConfigureInput({
        homeserverUrl: "https://matrix.example.test",
        accessToken: "syt_token",
        allowedUserIds: "you@beeper.com",
      }),
    ).toThrowError('"you@beeper.com" is not a Matrix user ID. They look like @you:beeper.com.');
  });

  it("accepts several Matrix IDs across lines, commas, and spaces without duplicates", () => {
    expect(
      parseMatrixBridgeConfigureInput({
        homeserverUrl: "  https://matrix.example.test  ",
        accessToken: " syt_token ",
        allowedUserIds: "@you:beeper.com, @you:beeper.com\n@phone:beeper.com",
      }),
    ).toEqual({
      homeserverUrl: "https://matrix.example.test",
      accessToken: "syt_token",
      allowedUserIds: ["@you:beeper.com", "@phone:beeper.com"],
    });
  });

  it("repopulates the form from a saved connection without a token", () => {
    expect(
      matrixBridgeDraftFromSavedConfig({
        homeserverUrl: "https://matrix.example.test/",
        allowedUserIds: ["@you:beeper.com", "@phone:beeper.com"],
        roomId: "!room:example.test",
      }),
    ).toEqual({
      homeserverUrl: "https://matrix.example.test/",
      // Write-only on the server, so reconfiguring always retypes it.
      accessToken: "",
      allowedUserIds: "@you:beeper.com\n@phone:beeper.com",
    });
  });

  it("leaves the form blank when no bridge is configured", () => {
    expect(matrixBridgeDraftFromSavedConfig(null)).toEqual(EMPTY_MATRIX_BRIDGE_DRAFT);
  });

  it("clears the token after a successful connect and keeps the readable fields", () => {
    expect(
      matrixBridgeDraftAfterConfigure({
        homeserverUrl: "https://matrix.example.test/",
        allowedUserIds: ["@you:beeper.com", "@phone:beeper.com"],
        roomId: "!room:example.test",
      }),
    ).toEqual({
      homeserverUrl: "https://matrix.example.test/",
      accessToken: "",
      allowedUserIds: "@you:beeper.com\n@phone:beeper.com",
    });
  });
});

describe("activeMatrixBridgePairingCode", () => {
  const code = { credential: "pair_abc", expiresAtMs: 1_000 };

  it("shows a code that can still be redeemed", () => {
    expect(activeMatrixBridgePairingCode(code, 999)).toEqual(code);
  });

  it("stops presenting a code the server would no longer consume", () => {
    // One-time credentials expire in minutes; a panel left open must not keep
    // offering a code whose only outcome is a failed pairing attempt.
    expect(activeMatrixBridgePairingCode(code, 1_000)).toBeNull();
    expect(activeMatrixBridgePairingCode(code, 60_000)).toBeNull();
  });

  it("has nothing to show before a code is minted", () => {
    expect(activeMatrixBridgePairingCode(null, 0)).toBeNull();
  });
});
