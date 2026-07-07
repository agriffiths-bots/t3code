import { afterEach, describe, expect, it, vi } from "vite-plus/test";

import {
  buildHostedChannelSelectionUrl,
  buildHostedPairingUrl,
  hasHostedPairingRequest,
  isHostedStaticApp,
  readHostedPairingRequest,
} from "./hostedPairing";

describe("hostedPairing", () => {
  afterEach(() => {
    vi.unstubAllEnvs();
    vi.unstubAllGlobals();
  });

  it("reads hosted pairing host and query token parameters", () => {
    const url = new URL("https://app.t3.codes/pair?host=100.64.1.2:3773&token=ABCD1234");

    expect(readHostedPairingRequest(url)).toEqual({
      host: "100.64.1.2:3773",
      token: "ABCD1234",
      label: "",
    });
    expect(hasHostedPairingRequest(url)).toBe(true);
  });

  it("prefers hash tokens so generated hosted links do not put credentials in search params", () => {
    vi.stubEnv("VITE_HOSTED_APP_URL", "https://preview.t3.codes");

    const url = new URL(
      buildHostedPairingUrl({
        host: "https://backend.example.com:3773",
        token: "pairing-token",
        label: "Workstation",
      }),
    );

    expect(url.origin).toBe("https://preview.t3.codes");
    expect(url.pathname).toBe("/pair");
    expect(url.searchParams.get("host")).toBe("https://backend.example.com:3773");
    expect(url.searchParams.get("label")).toBe("Workstation");
    expect(url.searchParams.has("token")).toBe(false);
    expect(url.hash).toBe("#token=pairing-token");
  });

  it("uses the current hosted origin when building pairing links from a Pages deployment", () => {
    vi.stubEnv("VITE_HOSTED_APP_URL", "https://oc.agriffiths.dev");
    vi.stubGlobal("window", {
      location: {
        href: "https://t3-code-preview.pages.dev/settings",
      },
    });

    const url = new URL(
      buildHostedPairingUrl({
        host: "https://backend.example.com:3773",
        token: "pairing-token",
      }),
    );

    expect(url.origin).toBe("https://t3-code-preview.pages.dev");
    expect(url.pathname).toBe("/pair");
    expect(url.searchParams.get("host")).toBe("https://backend.example.com:3773");
    expect(url.hash).toBe("#token=pairing-token");
  });

  it("uses the configured hosted origin when the current window is server-backed", () => {
    vi.stubEnv("VITE_HOSTED_APP_URL", "https://oc.agriffiths.dev");
    vi.stubEnv("VITE_HTTP_URL", "http://127.0.0.1:3773");
    vi.stubGlobal("window", {
      location: {
        href: "http://127.0.0.1:5733/settings",
      },
    });

    const url = new URL(
      buildHostedPairingUrl({
        host: "https://backend.example.com:3773",
        token: "pairing-token",
      }),
    );

    expect(url.origin).toBe("https://oc.agriffiths.dev");
  });

  it("uses the configured hosted origin when the current Pages deployment is server-backed", () => {
    vi.stubEnv("VITE_HOSTED_APP_URL", "https://oc.agriffiths.dev");
    vi.stubEnv("VITE_HTTP_URL", "");
    vi.stubEnv("VITE_WS_URL", "");
    vi.stubGlobal("window", {
      location: {
        href: "https://dl5-5uq.pages.dev/settings",
      },
    });

    const url = new URL(
      buildHostedPairingUrl({
        host: "https://backend.example.com:3773",
        token: "pairing-token",
      }),
    );

    expect(url.origin).toBe("https://oc.agriffiths.dev");
    expect(url.pathname).toBe("/pair");
    expect(url.searchParams.get("host")).toBe("https://backend.example.com:3773");
    expect(url.hash).toBe("#token=pairing-token");
  });

  it("uses the configured hosted origin for Pages builds with a configured backend", () => {
    vi.stubEnv("VITE_HOSTED_APP_URL", "https://oc.agriffiths.dev");
    vi.stubEnv("VITE_HTTP_URL", "https://backend.example.com");
    vi.stubEnv("VITE_WS_URL", "");
    vi.stubGlobal("window", {
      location: {
        href: "https://preview-three.pages.dev/settings",
      },
    });

    const url = new URL(
      buildHostedPairingUrl({
        host: "https://backend.example.com:3773",
        token: "pairing-token",
      }),
    );

    expect(url.origin).toBe("https://oc.agriffiths.dev");
  });

  it("builds hosted channel selection URLs through the configured router origin", () => {
    vi.stubEnv("VITE_HOSTED_APP_URL", "https://app.t3.codes");

    const url = new URL(
      buildHostedChannelSelectionUrl({
        channel: "nightly",
      }),
    );

    expect(url.origin).toBe("https://app.t3.codes");
    expect(url.pathname).toBe("/__t3code/channel");
    expect(url.searchParams.get("channel")).toBe("nightly");
    expect(url.searchParams.has("next")).toBe(false);
  });

  it("ignores incomplete hosted pairing requests", () => {
    expect(
      hasHostedPairingRequest(new URL("https://app.t3.codes/pair?host=backend.example.com")),
    ).toBe(false);
    expect(hasHostedPairingRequest(new URL("https://app.t3.codes/pair?token=ABCD1234"))).toBe(
      false,
    );
  });

  it("detects the hosted static app only when no backend URL is configured", () => {
    vi.stubEnv("VITE_HOSTED_APP_URL", "https://preview.t3.codes");
    vi.stubEnv("VITE_HTTP_URL", "");
    vi.stubEnv("VITE_WS_URL", "");

    expect(isHostedStaticApp(new URL("https://preview.t3.codes/"))).toBe(true);
    expect(isHostedStaticApp(new URL("https://preview.t3.codes/pair"))).toBe(true);
    expect(isHostedStaticApp(new URL("https://backend.example.com/"))).toBe(false);

    vi.stubEnv("VITE_HTTP_URL", "https://backend.example.com");
    expect(isHostedStaticApp(new URL("https://preview.t3.codes/"))).toBe(false);
  });

  it("detects hosted channel aliases as static apps", () => {
    vi.stubEnv("VITE_HOSTED_APP_URL", "https://app.t3.codes");
    vi.stubEnv("VITE_HOSTED_APP_CHANNEL", "nightly");
    vi.stubEnv("VITE_HTTP_URL", "");
    vi.stubEnv("VITE_WS_URL", "");

    expect(isHostedStaticApp(new URL("https://nightly.app.t3.codes/"))).toBe(true);

    vi.stubEnv("VITE_HTTP_URL", "https://backend.example.com");
    expect(isHostedStaticApp(new URL("https://nightly.app.t3.codes/"))).toBe(false);
  });

  it("detects Cloudflare Pages origins as hosted static apps unless they are server-backed", () => {
    vi.stubEnv("VITE_HOSTED_APP_URL", "https://oc.agriffiths.dev");
    vi.stubEnv("VITE_HTTP_URL", "");
    vi.stubEnv("VITE_WS_URL", "");

    expect(isHostedStaticApp(new URL("https://t3-code-preview.pages.dev/pair"))).toBe(true);
    expect(isHostedStaticApp(new URL("https://dl5-5uq.pages.dev/pair"))).toBe(false);
    expect(isHostedStaticApp(new URL("https://oc.agriffiths.dev/pair"))).toBe(true);

    vi.stubEnv("VITE_HTTP_URL", "https://backend.example.com");
    expect(isHostedStaticApp(new URL("https://t3-code-preview.pages.dev/pair"))).toBe(false);
  });

  it("allows additional server-backed Pages origins to be configured", () => {
    vi.stubEnv("VITE_HOSTED_APP_URL", "https://oc.agriffiths.dev");
    vi.stubEnv("VITE_HTTP_URL", "");
    vi.stubEnv("VITE_WS_URL", "");
    vi.stubEnv("VITE_SERVER_BACKED_PAGES_HOSTS", "preview-one.pages.dev, preview-two.pages.dev");

    expect(isHostedStaticApp(new URL("https://preview-one.pages.dev/"))).toBe(false);
    expect(isHostedStaticApp(new URL("https://preview-two.pages.dev/"))).toBe(false);
    expect(isHostedStaticApp(new URL("https://preview-three.pages.dev/"))).toBe(true);
  });

  it("keeps configured server-backed Pages origins out of hosted-static mode", () => {
    vi.stubEnv("VITE_HOSTED_APP_URL", "https://dl5-5uq.pages.dev");
    vi.stubEnv("VITE_HOSTED_APP_CHANNEL", "nightly");
    vi.stubEnv("VITE_HTTP_URL", "");
    vi.stubEnv("VITE_WS_URL", "");
    vi.stubEnv("VITE_SERVER_BACKED_PAGES_HOSTS", "preview-one.pages.dev");

    expect(isHostedStaticApp(new URL("https://dl5-5uq.pages.dev/"))).toBe(false);
    expect(isHostedStaticApp(new URL("https://preview-one.pages.dev/"))).toBe(false);
    expect(isHostedStaticApp(new URL("https://preview-two.pages.dev/"))).toBe(true);
  });
});
