import { useAtomValue } from "@effect/atom-react";
import { resolveAssetUrl } from "@t3tools/client-runtime/state/assets";
import { bindAssetSurface } from "@t3tools/client-runtime/state/assets";
import type { AssetCreateUrlResult, AssetResource, EnvironmentId } from "@t3tools/contracts";
import { AsyncResult } from "effect/unstable/reactivity";
import { useEffect, useMemo, useState } from "react";

import { assetEnvironment } from "~/state/assets";
import { usePreparedConnection } from "~/state/session";

export { resolveAssetUrl } from "@t3tools/client-runtime/state/assets";

const surfaceBindingPromises = new Map<string, Promise<string | null>>();
const SURFACE_BIND_RETRY_INITIAL_MS = 1_000;
const SURFACE_BIND_RETRY_MAX_MS = 30_000;

export function resolveBrowserAssetSurfaceBaseUrl(href: string): string | null {
  try {
    const url = new URL(href);
    if (!url.host) return null;
    return `${url.protocol}//${url.host}/`;
  } catch {
    return null;
  }
}

export function resolveBoundAssetUrl(
  httpBaseUrl: string,
  surfaceBaseUrl: string | null,
  asset: AssetCreateUrlResult,
): Promise<string | null> {
  if (asset.surfaceCredential === null) {
    return Promise.resolve(resolveAssetUrl(httpBaseUrl, asset.relativeUrl));
  }
  if (surfaceBaseUrl === null) return Promise.resolve(null);
  const key = JSON.stringify([surfaceBaseUrl, asset.surfaceCredential, asset.relativeUrl]);
  const existing = surfaceBindingPromises.get(key);
  if (existing !== undefined) return existing;
  const binding = bindAssetSurface(surfaceBaseUrl, asset);
  surfaceBindingPromises.set(key, binding);
  void binding.then(() => {
    if (surfaceBindingPromises.get(key) === binding) surfaceBindingPromises.delete(key);
  });
  return binding;
}

function useBoundAssetUrls(
  httpBaseUrl: string | null,
  surfaceBaseUrl: string | null,
  assets: ReadonlyArray<AssetCreateUrlResult | null>,
): ReadonlyArray<string | null> {
  const key = JSON.stringify([httpBaseUrl, surfaceBaseUrl, assets]);
  const immediate = useMemo(
    () =>
      assets.map((asset) =>
        asset?.surfaceCredential === null && httpBaseUrl !== null
          ? resolveAssetUrl(httpBaseUrl, asset.relativeUrl)
          : null,
      ),
    [assets, httpBaseUrl],
  );
  const [bound, setBound] = useState<{
    readonly key: string;
    readonly urls: ReadonlyArray<string | null>;
  } | null>(null);
  useEffect(() => {
    let active = true;
    let retryTimer: ReturnType<typeof setTimeout> | undefined;
    let retryDelay = SURFACE_BIND_RETRY_INITIAL_MS;
    if (httpBaseUrl === null) {
      setBound({ key, urls: assets.map(() => null) });
      return () => {
        active = false;
      };
    }
    const bind = async () => {
      const urls = await Promise.all(
        assets.map((asset) =>
          asset === null
            ? Promise.resolve(null)
            : resolveBoundAssetUrl(httpBaseUrl, surfaceBaseUrl, asset),
        ),
      );
      if (!active) return;
      setBound({ key, urls });
      const privateBindingFailed = assets.some(
        (asset, index) =>
          asset !== null && asset.surfaceCredential !== null && urls[index] === null,
      );
      if (privateBindingFailed) {
        retryTimer = setTimeout(() => {
          retryDelay = Math.min(retryDelay * 2, SURFACE_BIND_RETRY_MAX_MS);
          void bind();
        }, retryDelay);
      }
    };
    void bind();
    return () => {
      active = false;
      if (retryTimer !== undefined) clearTimeout(retryTimer);
    };
  }, [assets, httpBaseUrl, key, surfaceBaseUrl]);
  return bound?.key === key ? bound.urls : immediate;
}

export function useAssetUrl(environmentId: EnvironmentId, resource: AssetResource): string | null {
  const preparedConnection = usePreparedConnection(environmentId);
  const surfaceBaseUrl =
    typeof window === "undefined" ? null : resolveBrowserAssetSurfaceBaseUrl(window.location.href);
  const result = useAtomValue(
    assetEnvironment.createUrl({
      environmentId,
      input: { resource },
    }),
  );
  const assets = useMemo(() => [result._tag === "Success" ? result.value : null], [result]);
  const urls = useBoundAssetUrls(
    preparedConnection._tag === "Some" ? preparedConnection.value.httpBaseUrl : null,
    surfaceBaseUrl,
    assets,
  );
  return urls[0] ?? null;
}

export function useAssetUrls(
  environmentId: EnvironmentId,
  resources: ReadonlyArray<AssetResource>,
): ReadonlyArray<string | null> {
  const preparedConnection = usePreparedConnection(environmentId);
  const surfaceBaseUrl =
    typeof window === "undefined" ? null : resolveBrowserAssetSurfaceBaseUrl(window.location.href);
  const results = useAtomValue(
    assetEnvironment.createUrls({
      environmentId,
      resources,
    }),
  );
  const assets = useMemo(
    () => results.map((result) => (AsyncResult.isSuccess(result) ? result.value : null)),
    [results],
  );
  return useBoundAssetUrls(
    preparedConnection._tag === "Some" ? preparedConnection.value.httpBaseUrl : null,
    surfaceBaseUrl,
    assets,
  );
}
