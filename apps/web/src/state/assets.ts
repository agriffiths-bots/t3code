import { createAssetEnvironmentAtoms } from "@t3tools/client-runtime/state/assets";

import { connectionAtomRuntime } from "../connection/runtime";
import { WEB_ASSET_SURFACE_CREDENTIAL_BINDING } from "./assetEnvironmentConfig";

export const assetEnvironment = createAssetEnvironmentAtoms(connectionAtomRuntime, {
  // Browser assets bind the returned surface credential as a same-origin HttpOnly relay cookie.
  surfaceCredentialBinding: WEB_ASSET_SURFACE_CREDENTIAL_BINDING,
});
