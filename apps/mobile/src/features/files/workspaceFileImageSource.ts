import type { AssetRequestSource } from "../../state/assets";

export function workspaceFileImageSource(source: AssetRequestSource) {
  return {
    uri: source.uri,
    headers: source.headers,
    // Relay-backed images are session-bound and served with no-store. Revalidate them instead of
    // allowing React Native to reuse a surface credential after it has expired or been revoked.
    cache: source.headers === undefined ? ("force-cache" as const) : ("reload" as const),
  };
}
