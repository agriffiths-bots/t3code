import { createAssetEnvironmentAtoms } from "@t3tools/client-runtime/state/assets";

import { connectionAtomRuntime } from "../connection/runtime";

// Browser <img> requests cannot attach the relay's surface credential. Keep web on the
// pre-relay signed URL path until a browser-specific credential binding is implemented.
export const assetEnvironment = createAssetEnvironmentAtoms(connectionAtomRuntime, {
  surfaceCredentialBinding: "none",
});
