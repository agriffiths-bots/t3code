import { readHostedPairingRequest } from "@t3tools/shared/remote";
import * as Schema from "effect/Schema";

const MOBILE_PAIRING_URL_PARAM = "pairingUrl";
const CLOUDFLARE_ACCESS_TOKEN_PARAM = "cf_access_token";

export class PairingQrPayloadEmptyError extends Schema.TaggedErrorClass<PairingQrPayloadEmptyError>()(
  "PairingQrPayloadEmptyError",
  {},
) {
  override get message(): string {
    return "Scanned QR code did not contain a pairing URL.";
  }
}

export function buildPairingUrl(
  host: string,
  code: string,
  cloudflareAccessToken?: string,
): string {
  const h = host.trim();
  const c = code.trim();
  const cfAccessToken = cloudflareAccessToken?.trim() ?? "";
  if (!h) return "";
  if (!c) return h;

  try {
    const url = new URL(h.includes("://") ? h : `https://${h}`);
    url.hash = new URLSearchParams([
      ["token", c],
      ...(cfAccessToken.length > 0 ? [[CLOUDFLARE_ACCESS_TOKEN_PARAM, cfAccessToken]] : []),
    ]).toString();
    return url.toString();
  } catch {
    return `${h}#token=${c}`;
  }
}

export function parsePairingUrl(url: string): {
  host: string;
  code: string;
  cloudflareAccessToken: string;
} {
  const trimmed = url.trim();
  if (!trimmed) return { host: "", code: "", cloudflareAccessToken: "" };

  try {
    const parsed = new URL(trimmed);
    const hostedPairingRequest = readHostedPairingRequest(parsed);
    const hashParams = new URLSearchParams(parsed.hash.slice(1));
    const cloudflareAccessToken =
      hashParams.get(CLOUDFLARE_ACCESS_TOKEN_PARAM) ||
      parsed.searchParams.get(CLOUDFLARE_ACCESS_TOKEN_PARAM) ||
      "";
    if (hostedPairingRequest) {
      return {
        host: hostedPairingRequest.host.replace(/\/$/, ""),
        code: hostedPairingRequest.token,
        cloudflareAccessToken,
      };
    }

    const hashToken = hashParams.get("token");
    const queryToken = parsed.searchParams.get("token");
    const code = hashToken || queryToken || "";

    parsed.hash = "";
    parsed.search = "";
    parsed.pathname = "/";
    return { host: parsed.toString().replace(/\/$/, ""), code, cloudflareAccessToken };
  } catch {
    return { host: trimmed, code: "", cloudflareAccessToken: "" };
  }
}

export function extractPairingUrlFromQrPayload(payload: string): string {
  const trimmed = payload.trim();
  if (!trimmed) {
    throw new PairingQrPayloadEmptyError({});
  }

  try {
    const url = new URL(trimmed);
    if (url.protocol === "t3code:") {
      const pairingUrl = url.searchParams.get(MOBILE_PAIRING_URL_PARAM)?.trim() ?? "";
      if (pairingUrl.length > 0) {
        return pairingUrl;
      }
    }
  } catch {
    // Treat non-URL payloads as raw pairing-url text so the normal input validation can decide.
  }

  return trimmed;
}
