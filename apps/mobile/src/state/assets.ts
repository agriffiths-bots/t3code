import { useAtomValue } from "@effect/atom-react";
import { createAssetEnvironmentAtoms, resolveAssetUrl } from "@t3tools/client-runtime/state/assets";
import type { AssetResource, EnvironmentId } from "@t3tools/contracts";
import { AsyncResult, Atom } from "effect/unstable/reactivity";

import { connectionAtomRuntime } from "../connection/runtime";
import { usePreparedConnection } from "./session";

export const assetEnvironment = createAssetEnvironmentAtoms(connectionAtomRuntime);
const ASSET_SURFACE_CREDENTIAL_HEADER = "x-t3-asset-surface";

export interface AssetRequestSource {
  readonly uri: string;
  readonly headers?: Record<string, string>;
  readonly surfaceBinding?: {
    readonly uri: string;
    readonly credential: string;
  };
}

const EMPTY_ASSET_URL_ATOM = Atom.make(AsyncResult.initial<never, never>(false)).pipe(
  Atom.withLabel("mobile-asset-url:empty"),
);

export function useAssetRequestSource(
  environmentId: EnvironmentId | null,
  resource: AssetResource | null,
): AssetRequestSource | null {
  const preparedConnection = usePreparedConnection(environmentId);
  const result = useAtomValue(
    environmentId === null || resource === null
      ? EMPTY_ASSET_URL_ATOM
      : assetEnvironment.createUrl({ environmentId, input: { resource } }),
  );
  if (preparedConnection._tag === "None" || result._tag !== "Success") {
    return null;
  }
  const uri = resolveAssetUrl(preparedConnection.value.httpBaseUrl, result.value.relativeUrl);
  if (uri === null) return null;
  const surfaceCredential = result.value.surfaceCredential;
  return {
    uri,
    ...(surfaceCredential === null || surfaceCredential === undefined
      ? {}
      : {
          headers: {
            [ASSET_SURFACE_CREDENTIAL_HEADER]: surfaceCredential,
          },
          surfaceBinding: {
            uri: new URL(
              "/api/assets/relay/surface",
              preparedConnection.value.httpBaseUrl,
            ).toString(),
            credential: surfaceCredential,
          },
        }),
  };
}
