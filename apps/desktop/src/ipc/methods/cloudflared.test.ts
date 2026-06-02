import { describe, expect, it } from "@effect/vitest";
import * as Cloudflared from "@t3tools/shared/cloudflared";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import { getCloudflaredStatus, installCloudflared } from "./cloudflared.ts";

const available = {
  status: "available",
  executablePath: "/Users/test/.t3/tools/cloudflared/cloudflared",
  source: "managed",
  version: Cloudflared.CLOUDFLARED_VERSION,
} as const;

describe("Desktop cloudflared IPC", () => {
  it.effect("reads status and delegates installation to the shared manager", () =>
    Effect.gen(function* () {
      const installed: Array<boolean> = [];
      const layer = Layer.succeed(
        Cloudflared.CloudflaredExecutable,
        Cloudflared.CloudflaredExecutable.of({
          resolve: Effect.succeed({
            status: "missing",
            version: Cloudflared.CLOUDFLARED_VERSION,
          }),
          install: Effect.sync(() => {
            installed.push(true);
            return available;
          }),
        }),
      );

      expect(yield* getCloudflaredStatus.handler(undefined).pipe(Effect.provide(layer))).toEqual({
        status: "missing",
        version: Cloudflared.CLOUDFLARED_VERSION,
      });
      expect(yield* installCloudflared.handler(undefined).pipe(Effect.provide(layer))).toEqual(
        available,
      );
      expect(installed).toEqual([true]);
    }),
  );
});
