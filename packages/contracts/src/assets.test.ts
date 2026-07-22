import { describe, expect, it } from "@effect/vitest";
import * as Schema from "effect/Schema";

import {
  ASSET_SAME_ORIGIN_RELAY_V1_CAPABILITY,
  AssetCreateUrlInput,
  AssetCreateUrlResult,
  AssetResource,
} from "./assets.ts";

const LegacyAssetCreateUrlInput = Schema.Struct({ resource: AssetResource });
const LegacyAssetCreateUrlResult = Schema.Struct({
  relativeUrl: Schema.String,
  expiresAt: Schema.Number,
});
const decodeAssetCreateUrlInput = Schema.decodeUnknownSync(AssetCreateUrlInput);
const decodeAssetCreateUrlResult = Schema.decodeUnknownSync(AssetCreateUrlResult);
const decodeLegacyAssetCreateUrlInput = Schema.decodeUnknownSync(LegacyAssetCreateUrlInput);
const decodeLegacyAssetCreateUrlResult = Schema.decodeUnknownSync(LegacyAssetCreateUrlResult);

describe("asset protocol compatibility", () => {
  const resource = {
    _tag: "attachment" as const,
    attachmentId: "attachment-1",
  };

  it("decodes old client requests without advertised capabilities", () => {
    expect(decodeAssetCreateUrlInput({ resource })).toEqual({ resource });
  });

  it("decodes the same-origin relay capability advertised by new clients", () => {
    expect(
      decodeAssetCreateUrlInput({
        resource,
        capabilities: [ASSET_SAME_ORIGIN_RELAY_V1_CAPABILITY],
      }),
    ).toEqual({
      resource,
      capabilities: [ASSET_SAME_ORIGIN_RELAY_V1_CAPABILITY],
    });
  });

  it("lets an old server decode a new client request", () => {
    expect(
      decodeLegacyAssetCreateUrlInput({
        resource,
        capabilities: [ASSET_SAME_ORIGIN_RELAY_V1_CAPABILITY],
      }),
    ).toEqual({ resource });
  });

  it("decodes old server results without a surface credential", () => {
    const oldServerResult = {
      relativeUrl: "/api/assets/signed/attachment.png",
      expiresAt: 123,
    };

    expect(decodeAssetCreateUrlResult(oldServerResult)).toEqual(oldServerResult);
  });

  it("lets an old client decode a new server result", () => {
    expect(
      decodeLegacyAssetCreateUrlResult({
        relativeUrl: "/api/assets/relay/signed/attachment.png",
        expiresAt: 123,
        surfaceCredential: "surface.credential",
      }),
    ).toEqual({
      relativeUrl: "/api/assets/relay/signed/attachment.png",
      expiresAt: 123,
    });
  });
});
