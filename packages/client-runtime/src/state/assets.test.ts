import { describe, expect, it } from "@effect/vitest";
import { EnvironmentId } from "@t3tools/contracts";
import * as Layer from "effect/Layer";
import { Atom } from "effect/unstable/reactivity";

import type { EnvironmentRegistry } from "../connection/registry.ts";
import {
  bindAssetSurface,
  createAssetEnvironmentAtoms,
  InvalidAssetCollectionKeyError,
  parseAssetCollectionKey,
} from "./assets.ts";

describe("asset surface binding", () => {
  it("installs a private surface credential on the app relay origin", async () => {
    const requests: Array<{ readonly input: string; readonly init: RequestInit | undefined }> = [];
    const fetchImpl: typeof fetch = async (input, init) => {
      requests.push({ input: String(input), init });
      return new Response(null, { status: 204 });
    };

    await expect(
      bindAssetSurface(
        "https://private-app.example/base/",
        {
          relativeUrl: "/api/assets/relay/signed/private.png",
          expiresAt: 123,
          surfaceCredential: "surface.credential",
        },
        fetchImpl,
      ),
    ).resolves.toBe("https://private-app.example/api/assets/relay/signed/private.png");
    expect(requests).toEqual([
      {
        input: "https://private-app.example/api/assets/relay/surface",
        init: {
          method: "POST",
          credentials: "include",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ credential: "surface.credential" }),
        },
      },
    ]);
  });

  it("refuses to bind private credentials to the direct cross-site asset path", async () => {
    let fetched = false;
    const fetchImpl: typeof fetch = async () => {
      fetched = true;
      return new Response(null, { status: 204 });
    };

    await expect(
      bindAssetSurface(
        "https://environment.example/",
        {
          relativeUrl: "/api/assets/signed/private.png",
          expiresAt: 123,
          surfaceCredential: "surface.credential",
        },
        fetchImpl,
      ),
    ).resolves.toBeNull();
    expect(fetched).toBe(false);
  });

  it("keeps public factory asset URLs cookie-free", async () => {
    let fetched = false;
    const fetchImpl: typeof fetch = async () => {
      fetched = true;
      return new Response(null, { status: 204 });
    };

    await expect(
      bindAssetSurface(
        "https://environment.example/",
        {
          relativeUrl: "/api/assets/signed/factory.png",
          expiresAt: 123,
          surfaceCredential: null,
        },
        fetchImpl,
      ),
    ).resolves.toBe("https://environment.example/api/assets/signed/factory.png");
    expect(fetched).toBe(false);
  });

  it("accepts an unbound URL decoded from an old server result", async () => {
    let fetched = false;
    const fetchImpl: typeof fetch = async () => {
      fetched = true;
      return new Response(null, { status: 204 });
    };

    await expect(
      bindAssetSurface(
        "https://environment.example/",
        {
          relativeUrl: "/api/assets/signed/legacy.png",
          expiresAt: 123,
        },
        fetchImpl,
      ),
    ).resolves.toBe("https://environment.example/api/assets/signed/legacy.png");
    expect(fetched).toBe(false);
  });
});

describe("asset collection keys", () => {
  it("preserves malformed JSON and its native cause", () => {
    const key = "not-json";
    let error: unknown;

    try {
      parseAssetCollectionKey(key);
    } catch (cause) {
      error = cause;
    }

    expect(error).toBeInstanceOf(InvalidAssetCollectionKeyError);
    expect(error).toMatchObject({ key, cause: expect.any(SyntaxError) });
  });

  it("rejects invalid asset collection shapes", () => {
    const key = JSON.stringify(["environment-1", [{ _tag: "unknown" }]]);

    expect(() => parseAssetCollectionKey(key)).toThrowError(InvalidAssetCollectionKeyError);
  });
});

describe("createAssetEnvironmentAtoms", () => {
  it("keys asset URL queries by environment and resource", () => {
    const runtime = Atom.runtime(Layer.empty) as unknown as Atom.AtomRuntime<
      EnvironmentRegistry,
      never
    >;
    const assets = createAssetEnvironmentAtoms(runtime);
    const environmentId = EnvironmentId.make("environment-1");
    const originalTarget = {
      environmentId,
      input: {
        resource: {
          _tag: "project-favicon" as const,
          cwd: "/repo/original",
        },
      },
    };

    expect(assets.createUrl(originalTarget)).toBe(
      assets.createUrl({
        environmentId,
        input: {
          resource: {
            _tag: "project-favicon",
            cwd: "/repo/original",
          },
        },
      }),
    );
    expect(
      assets.createUrl({
        environmentId,
        input: {
          resource: {
            _tag: "project-favicon",
            cwd: "/repo/next",
          },
        },
      }),
    ).not.toBe(assets.createUrl(originalTarget));
    expect(
      assets.createUrl({
        environmentId: EnvironmentId.make("environment-2"),
        input: originalTarget.input,
      }),
    ).not.toBe(assets.createUrl(originalTarget));
  });

  it("keys collections while preserving independent resource queries", () => {
    const runtime = Atom.runtime(Layer.empty) as unknown as Atom.AtomRuntime<
      EnvironmentRegistry,
      never
    >;
    const assets = createAssetEnvironmentAtoms(runtime);
    const environmentId = EnvironmentId.make("environment-1");
    const resources = [
      { _tag: "attachment" as const, attachmentId: "attachment-1" },
      { _tag: "attachment" as const, attachmentId: "attachment-2" },
    ];

    expect(assets.createUrls({ environmentId, resources })).toBe(
      assets.createUrls({
        environmentId,
        resources: resources.map((resource) => ({ ...resource })),
      }),
    );
    expect(
      assets.createUrls({
        environmentId,
        resources: [...resources].toReversed(),
      }),
    ).not.toBe(assets.createUrls({ environmentId, resources }));
  });
});
