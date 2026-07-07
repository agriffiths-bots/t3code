import type * as ServerConfig from "./config.ts";

export const browserApiCorsAllowedMethods = ["GET", "POST", "OPTIONS"] as const;
export const browserApiCorsAllowedHeaders = [
  "authorization",
  "b3",
  "cf-access-client-id",
  "cf-access-client-secret",
  "cf-access-jwt-assertion",
  "traceparent",
  "content-type",
  "dpop",
] as const;
export const hostedBrowserApiCredentialOrigins = [
  "https://app.t3.codes",
  "https://latest.app.t3.codes",
  "https://nightly.app.t3.codes",
  "https://dl5-5uq.pages.dev",
] as const;

export function normalizeCorsOrigin(value: string | undefined): string | null {
  const trimmed = value?.trim() ?? "";
  if (!trimmed) {
    return null;
  }
  try {
    const url = new URL(trimmed);
    return url.protocol === "http:" || url.protocol === "https:" ? url.origin : trimmed;
  } catch {
    return trimmed;
  }
}

export function configuredBrowserCookieCredentialOrigins(
  config: ServerConfig.ServerConfig["Service"],
) {
  const origins = new Set<string>(hostedBrowserApiCredentialOrigins);
  if (config.devUrl?.origin) {
    origins.add(config.devUrl.origin);
  }
  if (config.hostedAppUrl?.protocol === "https:") {
    origins.add(config.hostedAppUrl.origin);
  }
  return origins;
}

export function configuredHostedBrowserApiCredentialOrigins(
  config: ServerConfig.ServerConfig["Service"],
) {
  const origins = new Set<string>(hostedBrowserApiCredentialOrigins);
  if (config.hostedAppUrl?.protocol === "https:") {
    origins.add(config.hostedAppUrl.origin);
  }
  return origins;
}

function normalizeBrowserCredentialOrigin(value: string | undefined): string | null {
  const trimmed = value?.trim() ?? "";
  if (!trimmed) {
    return null;
  }
  try {
    const url = new URL(trimmed);
    if (url.protocol === "ws:") {
      url.protocol = "http:";
      return url.origin;
    }
    if (url.protocol === "wss:") {
      url.protocol = "https:";
      return url.origin;
    }
    return url.protocol === "http:" || url.protocol === "https:" ? url.origin : trimmed;
  } catch {
    return trimmed;
  }
}

export function browserCookieCredentialOriginAllowed(input: {
  readonly origin: string | undefined;
  readonly requestOrigin?: string | undefined;
  readonly trustedOrigins?: ReadonlySet<string>;
}): boolean {
  const origin = normalizeBrowserCredentialOrigin(input.origin);
  if (origin === null) {
    return true;
  }
  const requestOrigin = normalizeBrowserCredentialOrigin(input.requestOrigin);
  if (requestOrigin !== null && requestOrigin === origin) {
    return true;
  }
  if (input.trustedOrigins?.has(origin)) {
    return true;
  }
  return (hostedBrowserApiCredentialOrigins as readonly string[]).includes(origin);
}

export const browserApiCorsHeaders = {
  "access-control-allow-origin": "*",
  "access-control-allow-methods": browserApiCorsAllowedMethods.join(", "),
  "access-control-allow-headers": browserApiCorsAllowedHeaders.join(", "),
} as const;
