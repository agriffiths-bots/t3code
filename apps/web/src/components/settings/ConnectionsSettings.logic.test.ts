import type { DesktopWslState } from "@t3tools/contracts";
import { describe, expect, it, vi } from "vite-plus/test";
import {
  applyWslEnableSelection,
  parsePairingUrlFields,
  parseRemotePairingHostChange,
  parseRemotePairingFields,
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
