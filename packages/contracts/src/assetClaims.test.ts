import { describe, expect, it } from "@effect/vitest";
import * as Schema from "effect/Schema";

import { AssetClaimJson } from "./assetClaims.ts";

const decodeClaim = Schema.decodeUnknownSync(AssetClaimJson);
const encodeClaim = Schema.encodeSync(AssetClaimJson);

describe("asset claim codec", () => {
  it("round-trips an attachment claim with its audience", () => {
    const claim = {
      version: 1 as const,
      kind: "attachment" as const,
      attachmentId: "attachment-1",
      expiresAt: 123,
      dataAudience: "factory" as const,
      surfaceBindingId: "surface-binding-1",
    };

    expect(decodeClaim(encodeClaim(claim))).toEqual(claim);
  });

  it("decodes legacy claims fail-closed as private and unbound", () => {
    expect(
      decodeClaim(
        JSON.stringify({
          version: 1,
          kind: "attachment",
          attachmentId: "attachment-legacy",
          expiresAt: 123,
        }),
      ),
    ).toEqual({
      version: 1,
      kind: "attachment",
      attachmentId: "attachment-legacy",
      expiresAt: 123,
      dataAudience: "private",
      surfaceBindingId: null,
    });
  });
});
