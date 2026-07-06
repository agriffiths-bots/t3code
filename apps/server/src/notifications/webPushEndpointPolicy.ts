// Web-push endpoint SSRF policy. This runtime classification logic lives in the
// server package (not @t3tools/contracts, which must stay schema-only per the
// root AGENTS.md) and is enforced by WebPushEndpointGuard on register and send.

const WEB_PUSH_ENDPOINT_VALIDATION_MESSAGE =
  "Web push endpoint must be an HTTPS URL with a public host";

function parseIpv4Address(host: string): readonly [number, number, number, number] | null {
  const parts = host.split(".");
  if (parts.length !== 4) {
    return null;
  }
  const octets = parts.map((part) => {
    if (!/^\d{1,3}$/.test(part)) {
      return null;
    }
    const value = Number(part);
    return value >= 0 && value <= 255 ? value : null;
  });
  return octets.every((part): part is number => part !== null)
    ? (octets as [number, number, number, number])
    : null;
}

function ipv4FromHextets(
  hextets: ReadonlyArray<number>,
): readonly [number, number, number, number] {
  return [
    (hextets[6] ?? 0) >> 8,
    (hextets[6] ?? 0) & 0xff,
    (hextets[7] ?? 0) >> 8,
    (hextets[7] ?? 0) & 0xff,
  ] as const;
}

function parseIpv6Address(host: string): ReadonlyArray<number> | null {
  const normalized = host.toLowerCase();
  if (normalized.length === 0 || normalized.includes("%")) {
    return null;
  }
  const [head, tail, extra] = normalized.split("::");
  if (extra !== undefined) {
    return null;
  }
  const parseParts = (value: string): ReadonlyArray<number> | null => {
    if (value.length === 0) {
      return [];
    }
    const rawParts = value.split(":");
    const parts: Array<number> = [];
    for (let index = 0; index < rawParts.length; index += 1) {
      const part = rawParts[index] ?? "";
      if (part.includes(".")) {
        if (index !== rawParts.length - 1) {
          return null;
        }
        const ipv4 = parseIpv4Address(part);
        if (ipv4 === null) {
          return null;
        }
        parts.push((ipv4[0] << 8) | ipv4[1], (ipv4[2] << 8) | ipv4[3]);
        continue;
      }
      if (!/^[0-9a-f]{1,4}$/.test(part)) {
        return null;
      }
      parts.push(Number.parseInt(part, 16));
    }
    return parts;
  };
  const headParts = parseParts(head ?? "");
  const tailParts = tail === undefined ? [] : parseParts(tail);
  if (headParts === null || tailParts === null) {
    return null;
  }
  if (tail === undefined) {
    return headParts.length === 8 ? headParts : null;
  }
  const missing = 8 - headParts.length - tailParts.length;
  if (missing < 1) {
    return null;
  }
  return [...headParts, ...Array.from({ length: missing }, () => 0), ...tailParts];
}

export function isPublicWebPushIpAddress(host: string): boolean {
  const normalizedHost =
    host.startsWith("[") && host.endsWith("]")
      ? host.slice(1, -1).toLowerCase()
      : host.toLowerCase();
  const ipv4 = parseIpv4Address(normalizedHost);
  if (ipv4 !== null) {
    const [a, b, c] = ipv4;
    return !(
      a === 0 ||
      a === 10 ||
      a === 127 ||
      (a === 100 && b >= 64 && b <= 127) ||
      (a === 169 && b === 254) ||
      (a === 172 && b >= 16 && b <= 31) ||
      (a === 192 && b === 0 && (c === 0 || c === 2)) ||
      (a === 192 && b === 88 && c === 99) ||
      (a === 192 && b === 168) ||
      (a === 198 && (b === 18 || b === 19)) ||
      (a === 198 && b === 51 && c === 100) ||
      (a === 203 && b === 0 && c === 113) ||
      a >= 224
    );
  }

  const ipv6 = parseIpv6Address(normalizedHost);
  if (ipv6 === null) {
    return false;
  }
  const first = ipv6[0] ?? 0;
  const second = ipv6[1] ?? 0;
  const isAllZero = ipv6.every((part) => part === 0);
  const isLoopback = ipv6.slice(0, 7).every((part) => part === 0) && ipv6[7] === 1;
  const isIpv4Mapped = ipv6.slice(0, 5).every((part) => part === 0) && ipv6[5] === 0xffff;
  if (isIpv4Mapped) {
    return isPublicWebPushIpAddress(ipv4FromHextets(ipv6).join("."));
  }
  return !(
    isAllZero ||
    isLoopback ||
    (first === 0x64 && second === 0xff9b) ||
    (first === 0x100 && second === 0) ||
    first === 0x2002 ||
    (first === 0x2001 && second === 0xdb8) ||
    (first & 0xfe00) === 0xfc00 ||
    (first & 0xffc0) === 0xfe80 ||
    (first & 0xff00) === 0xff00 ||
    (first & 0xe000) !== 0x2000
  );
}

function normalizeEndpointHost(hostname: string): string {
  const withoutBrackets =
    hostname.startsWith("[") && hostname.endsWith("]") ? hostname.slice(1, -1) : hostname;
  return withoutBrackets.endsWith(".")
    ? withoutBrackets.slice(0, -1).toLowerCase()
    : withoutBrackets.toLowerCase();
}

export function getWebPushEndpointValidationError(endpoint: string): string | null {
  let url: URL;
  try {
    url = new URL(endpoint);
  } catch {
    return WEB_PUSH_ENDPOINT_VALIDATION_MESSAGE;
  }
  if (url.protocol !== "https:" || url.username.length > 0 || url.password.length > 0) {
    return WEB_PUSH_ENDPOINT_VALIDATION_MESSAGE;
  }
  const host = normalizeEndpointHost(url.hostname);
  if (host.length === 0) {
    return WEB_PUSH_ENDPOINT_VALIDATION_MESSAGE;
  }
  const ipv4 = parseIpv4Address(host);
  const ipv6 = parseIpv6Address(host);
  if (ipv4 !== null || ipv6 !== null) {
    return isPublicWebPushIpAddress(host) ? null : WEB_PUSH_ENDPOINT_VALIDATION_MESSAGE;
  }
  if (
    host === "localhost" ||
    host.endsWith(".localhost") ||
    host.endsWith(".local") ||
    host.endsWith(".internal") ||
    host.endsWith(".home") ||
    host.endsWith(".lan") ||
    !host.includes(".")
  ) {
    return WEB_PUSH_ENDPOINT_VALIDATION_MESSAGE;
  }
  return null;
}
