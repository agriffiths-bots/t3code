import { describe, expect, it } from "vite-plus/test";

import { workspaceFileImageSource } from "./workspaceFileImageSource";

describe("workspaceFileImageSource", () => {
  it("force-caches only local cached-file sources", () => {
    expect(workspaceFileImageSource({ uri: "file:///cache/image.png" })).toEqual({
      uri: "file:///cache/image.png",
      headers: undefined,
      cache: "force-cache",
    });
  });

  it("reloads session-bound relay sources instead of force-caching them", () => {
    expect(
      workspaceFileImageSource({
        uri: "https://environment.example/api/assets/relay/token/image.png",
        headers: { "x-t3-asset-surface": "surface.credential" },
      }),
    ).toEqual({
      uri: "https://environment.example/api/assets/relay/token/image.png",
      headers: { "x-t3-asset-surface": "surface.credential" },
      cache: "reload",
    });
  });
});
