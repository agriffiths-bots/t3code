import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Headers from "effect/unstable/http/Headers";

import { httpHeaderRedactionLayer } from "./httpObservability.ts";

describe("httpHeaderRedactionLayer", () => {
  it.effect("redacts custom authorization headers used by remote environments", () =>
    Effect.gen(function* () {
      const names = yield* Headers.CurrentRedactedNames;

      expect(names).toEqual(
        expect.arrayContaining([
          "cf-access-client-id",
          "cf-access-client-secret",
          "cf-access-jwt-assertion",
          "dpop",
        ]),
      );
    }).pipe(Effect.provide(httpHeaderRedactionLayer)),
  );
});
