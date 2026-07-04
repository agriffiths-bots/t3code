import { normalizeHttpBaseUrl } from "@t3tools/shared/advertisedEndpoint";

export function normalizeCloudflareAccessOrigin(host: string): string {
  const trimmed = host.trim();
  if (trimmed.length === 0) {
    throw new Error("Remote host is required.");
  }

  const rawUrl = /^[a-z][a-z0-9+.-]*:\/\//i.test(trimmed) ? trimmed : `https://${trimmed}`;
  const url = new URL(normalizeHttpBaseUrl(rawUrl));
  url.username = "";
  url.password = "";
  return url.toString();
}
