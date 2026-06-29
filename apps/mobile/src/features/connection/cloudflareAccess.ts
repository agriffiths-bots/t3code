import * as Crypto from "expo-crypto";
import * as Linking from "expo-linking";
import * as WebBrowser from "expo-web-browser";

const CLOUDFLARE_ACCESS_CLI_PATH = "/cdn-cgi/access/cli";
const CLOUDFLARE_ACCESS_CALLBACK_PATH = "cloudflare-access";

export class CloudflareAccessLoginError extends Error {
  constructor(message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = "CloudflareAccessLoginError";
  }
}

function normalizeOrigin(input: string): string {
  const trimmed = input.trim();
  if (trimmed.length === 0) {
    throw new CloudflareAccessLoginError("Enter the environment host before signing in.");
  }
  const url = new URL(/^[a-zA-Z][a-zA-Z\d+-]*:\/\//.test(trimmed) ? trimmed : `https://${trimmed}`);
  url.pathname = "/";
  url.search = "";
  url.hash = "";
  return url.toString();
}

function base64Url(bytes: Uint8Array): string {
  const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
  let output = "";
  let index = 0;
  while (index < bytes.length) {
    const first = bytes[index++] ?? 0;
    const second = index < bytes.length ? bytes[index++] : undefined;
    const third = index < bytes.length ? bytes[index++] : undefined;
    output += alphabet[first >> 2];
    output += alphabet[((first & 0x03) << 4) | ((second ?? 0) >> 4)];
    if (second !== undefined) {
      output += alphabet[((second & 0x0f) << 2) | ((third ?? 0) >> 6)];
    }
    if (third !== undefined) {
      output += alphabet[third & 0x3f];
    }
  }
  return output;
}

function isJwt(value: string): boolean {
  return /^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$/.test(value.trim());
}

function extractJwtFromResponse(body: string): string {
  const trimmed = body.trim();
  if (isJwt(trimmed)) {
    return trimmed;
  }

  try {
    const parsed = JSON.parse(trimmed) as Record<string, unknown>;
    for (const key of ["token", "jwt", "access_token"]) {
      const value = parsed[key];
      if (typeof value === "string" && isJwt(value)) {
        return value;
      }
    }
  } catch {
    // Fall through to the consistent error below.
  }

  throw new CloudflareAccessLoginError("Cloudflare Access did not return a usable JWT.");
}

function parseAccessAudience(locationHeader: string | null): string {
  if (!locationHeader) {
    throw new CloudflareAccessLoginError(
      "Could not discover the Cloudflare Access application audience.",
    );
  }
  const location = new URL(locationHeader);
  const aud = location.searchParams.get("kid") ?? location.searchParams.get("aud") ?? "";
  if (aud.length === 0) {
    throw new CloudflareAccessLoginError(
      "Cloudflare Access login challenge did not include an application audience.",
    );
  }
  return aud;
}

async function discoverAccessAudience(origin: string): Promise<string> {
  const response = await fetch(origin, { method: "HEAD", redirect: "manual" });
  const authenticate = response.headers.get("www-authenticate") ?? "";
  if (!authenticate.toLowerCase().includes("cloudflare-access")) {
    throw new CloudflareAccessLoginError(
      "This host did not return a Cloudflare Access login challenge.",
    );
  }
  return parseAccessAudience(response.headers.get("location"));
}

function buildLoginUrl(input: {
  readonly origin: string;
  readonly aud: string;
  readonly redirectUri: string;
  readonly transferToken: string;
}): string {
  const callback = new URL(input.redirectUri);
  callback.searchParams.set("aud", input.aud);
  callback.searchParams.set("token", input.transferToken);

  const login = new URL(CLOUDFLARE_ACCESS_CLI_PATH, input.origin);
  login.searchParams.set("aud", input.aud);
  login.searchParams.set("edge_token_transfer", "true");
  login.searchParams.set("redirect_url", callback.toString());
  login.searchParams.set("send_org_token", "true");
  login.searchParams.set("token", input.transferToken);
  return login.toString();
}

async function exchangeTransferToken(input: {
  readonly origin: string;
  readonly aud: string;
  readonly transferToken: string;
}): Promise<string> {
  const url = new URL(CLOUDFLARE_ACCESS_CLI_PATH, input.origin);
  url.searchParams.set("aud", input.aud);
  url.searchParams.set("send_org_token", "true");
  url.searchParams.set("token", input.transferToken);

  const response = await fetch(url.toString());
  if (!response.ok) {
    throw new CloudflareAccessLoginError(
      `Cloudflare Access token exchange failed with HTTP ${response.status}.`,
    );
  }
  return extractJwtFromResponse(await response.text());
}

export async function authenticateCloudflareAccess(host: string): Promise<string> {
  const origin = normalizeOrigin(host);
  const aud = await discoverAccessAudience(origin);
  const transferToken = base64Url(Crypto.getRandomBytes(32));
  const redirectUri = Linking.createURL(CLOUDFLARE_ACCESS_CALLBACK_PATH);
  const loginUrl = buildLoginUrl({ origin, aud, redirectUri, transferToken });
  const result = await WebBrowser.openAuthSessionAsync(loginUrl, redirectUri);

  if (result.type !== "success") {
    throw new CloudflareAccessLoginError("Cloudflare Access sign-in was cancelled.");
  }

  const callback = new URL(result.url);
  const returnedToken = callback.searchParams.get("token") ?? "";
  if (isJwt(returnedToken)) {
    return returnedToken;
  }
  if (returnedToken !== transferToken) {
    throw new CloudflareAccessLoginError("Cloudflare Access returned an unexpected login token.");
  }
  return exchangeTransferToken({ origin, aud, transferToken });
}
