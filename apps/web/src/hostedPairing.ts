import { getPairingTokenFromUrl, setPairingTokenOnUrl } from "./pairingUrl";

const DEFAULT_HOSTED_APP_URL = "https://app.t3.codes";
const CLOUDFLARE_PAGES_HOST_SUFFIX = ".pages.dev";
const DEFAULT_SERVER_BACKED_CLOUDFLARE_PAGES_HOSTS = ["dl5-5uq.pages.dev"];

export interface HostedPairingRequest {
  readonly host: string;
  readonly token: string;
  readonly label: string;
}

export type HostedAppChannel = "latest" | "nightly";

export function configuredHostedAppUrl(): string {
  return import.meta.env.VITE_HOSTED_APP_URL?.trim() || DEFAULT_HOSTED_APP_URL;
}

function configuredBackendUrl(): string {
  return import.meta.env.VITE_HTTP_URL?.trim() || import.meta.env.VITE_WS_URL?.trim() || "";
}

function configuredHostedAppChannel(): HostedAppChannel | null {
  const channel = import.meta.env.VITE_HOSTED_APP_CHANNEL?.trim().toLowerCase();
  return channel === "latest" || channel === "nightly" ? channel : null;
}

function originFromUrl(value: string): string | null {
  try {
    return new URL(value).origin;
  } catch {
    return null;
  }
}

function isCloudflarePagesOrigin(url: URL): boolean {
  return url.protocol === "https:" && url.hostname.endsWith(CLOUDFLARE_PAGES_HOST_SUFFIX);
}

function configuredServerBackedCloudflarePagesHosts(): Set<string> {
  const configured = import.meta.env.VITE_SERVER_BACKED_PAGES_HOSTS?.trim() || "";
  return new Set(
    [...DEFAULT_SERVER_BACKED_CLOUDFLARE_PAGES_HOSTS, ...configured.split(",")]
      .map((host) => host.trim().toLowerCase())
      .filter(Boolean),
  );
}

function isServerBackedCloudflarePagesOrigin(url: URL): boolean {
  return (
    isCloudflarePagesOrigin(url) &&
    configuredServerBackedCloudflarePagesHosts().has(url.hostname.toLowerCase())
  );
}

function currentHostedAppOrigin(): string | null {
  if (typeof window === "undefined") {
    return null;
  }
  try {
    const currentUrl = new URL(window.location.href);
    return isHostedStaticApp(currentUrl) ||
      (isCloudflarePagesOrigin(currentUrl) && !isServerBackedCloudflarePagesOrigin(currentUrl))
      ? currentUrl.origin
      : null;
  } catch {
    return null;
  }
}

export function isHostedStaticApp(url: URL = new URL(window.location.href)): boolean {
  if (configuredBackendUrl()) {
    return false;
  }

  if (isServerBackedCloudflarePagesOrigin(url)) {
    return false;
  }

  if (configuredHostedAppChannel()) {
    return true;
  }

  const hostedOrigin = originFromUrl(configuredHostedAppUrl());
  if (hostedOrigin !== null && url.origin === hostedOrigin) {
    return true;
  }

  return isCloudflarePagesOrigin(url) && !isServerBackedCloudflarePagesOrigin(url);
}

export function readHostedPairingRequest(url: URL = new URL(window.location.href)) {
  const host = url.searchParams.get("host")?.trim() ?? "";
  const token = getPairingTokenFromUrl(url)?.trim() ?? "";
  const label = url.searchParams.get("label")?.trim() ?? "";

  if (!host || !token) {
    return null;
  }

  return {
    host,
    token,
    label,
  } satisfies HostedPairingRequest;
}

export function hasHostedPairingRequest(url: URL = new URL(window.location.href)): boolean {
  return readHostedPairingRequest(url) !== null;
}

export function buildHostedPairingUrl(input: {
  readonly host: string;
  readonly token: string;
  readonly label?: string | null;
}): string {
  const baseUrl = currentHostedAppOrigin() ?? configuredHostedAppUrl();
  const url = new URL("/pair", baseUrl);
  url.searchParams.set("host", input.host);

  const label = input.label?.trim();
  if (label) {
    url.searchParams.set("label", label);
  }

  return setPairingTokenOnUrl(url, input.token).toString();
}

export function buildHostedChannelSelectionUrl(input: {
  readonly channel: HostedAppChannel;
}): string {
  const url = new URL("/__t3code/channel", configuredHostedAppUrl());
  url.searchParams.set("channel", input.channel);
  return url.toString();
}
