import { withAssetClientCapabilities } from "@t3tools/client-runtime/state/assets";
import { ASSET_SAME_ORIGIN_RELAY_V1_CAPABILITY } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import { WEB_ASSET_SURFACE_CREDENTIAL_BINDING } from "./assetEnvironmentConfig";

describe("web asset environment", () => {
  it("advertises same-origin relay support for browser-bound private assets", () => {
    const resource = { _tag: "attachment" as const, attachmentId: "attachment-1" };

    expect(
      withAssetClientCapabilities({
        resource,
        surfaceCredentialBinding: WEB_ASSET_SURFACE_CREDENTIAL_BINDING,
      }),
    ).toEqual({
      resource,
      capabilities: [ASSET_SAME_ORIGIN_RELAY_V1_CAPABILITY],
    });
  });
});
