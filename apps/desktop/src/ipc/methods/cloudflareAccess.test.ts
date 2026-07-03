import { describe, expect, it } from "vite-plus/test";

import { normalizeCloudflareAccessOrigin } from "./cloudflareAccessOrigin.ts";

describe("normalizeCloudflareAccessOrigin", () => {
  it("normalizes bare, HTTP, and WebSocket backend hosts to HTTP origins", () => {
    expect(normalizeCloudflareAccessOrigin("oc.agriffiths.dev")).toBe("https://oc.agriffiths.dev/");
    expect(normalizeCloudflareAccessOrigin("https://oc.agriffiths.dev/pair#token=abc")).toBe(
      "https://oc.agriffiths.dev/",
    );
    expect(normalizeCloudflareAccessOrigin("wss://oc.agriffiths.dev/ws")).toBe(
      "https://oc.agriffiths.dev/",
    );
    expect(normalizeCloudflareAccessOrigin("ws://127.0.0.1:3773/ws")).toBe(
      "http://127.0.0.1:3773/",
    );
  });
});
