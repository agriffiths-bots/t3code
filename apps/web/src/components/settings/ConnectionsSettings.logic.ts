import type { AdvertisedEndpoint, DesktopBridge, DesktopWslState } from "@t3tools/contracts";
import { resolveRemotePairingTarget } from "@t3tools/shared/remote";

type WslEnableBridge = Pick<DesktopBridge, "setWslBackendEnabled" | "setWslDistro" | "setWslOnly">;

export function isQrShareableEndpoint(endpoint: AdvertisedEndpoint): boolean {
  return endpoint.status !== "unavailable" && endpoint.reachability !== "loopback";
}

export type QrEndpointOption = {
  readonly id: string;
  readonly preferenceKey: string;
  readonly qrShareable: boolean;
};

export function selectQrEndpointOption<T extends QrEndpointOption>(
  options: ReadonlyArray<T>,
  selectedId: string | null,
  defaultPreferenceKey: string | null,
): T | null {
  return (
    (selectedId !== null ? options.find((option) => option.id === selectedId) : undefined) ??
    (defaultPreferenceKey !== null
      ? options.find((option) => option.preferenceKey === defaultPreferenceKey)
      : undefined) ??
    options.find((option) => option.qrShareable) ??
    options[0] ??
    null
  );
}

export interface RemotePairingFields {
  readonly host: string;
  readonly pairingCode: string;
  readonly cloudflareAccessToken?: string;
  readonly cloudflareAccessClientId?: string;
  readonly cloudflareAccessClientSecret?: string;
}

export interface RemotePairingFieldInput {
  readonly host: string;
  readonly pairingCode: string;
  readonly cloudflareAccessToken?: string;
  readonly cloudflareAccessClientId?: string;
  readonly cloudflareAccessClientSecret?: string;
}

export interface RemotePairingHostChangeFields {
  readonly host: string;
  readonly pairingCode?: string;
  readonly cloudflareAccessToken: string;
  readonly cloudflareAccessClientId: string;
  readonly cloudflareAccessClientSecret: string;
}

const optionalTrimmed = (value: string | undefined): string | undefined => {
  const trimmed = value?.trim() ?? "";
  return trimmed.length > 0 ? trimmed : undefined;
};

function explicitCloudflareAccessFields(
  input: Pick<
    RemotePairingFieldInput,
    "cloudflareAccessToken" | "cloudflareAccessClientId" | "cloudflareAccessClientSecret"
  >,
): Omit<RemotePairingFields, "host" | "pairingCode"> {
  const cloudflareAccessToken = optionalTrimmed(input.cloudflareAccessToken);
  const cloudflareAccessClientId = optionalTrimmed(input.cloudflareAccessClientId);
  const cloudflareAccessClientSecret = optionalTrimmed(input.cloudflareAccessClientSecret);
  if (
    (cloudflareAccessClientId !== undefined && cloudflareAccessClientSecret === undefined) ||
    (cloudflareAccessClientId === undefined && cloudflareAccessClientSecret !== undefined)
  ) {
    throw new Error("Enter both Cloudflare Access service token fields.");
  }
  return {
    ...(cloudflareAccessToken ? { cloudflareAccessToken } : {}),
    ...(cloudflareAccessClientId && cloudflareAccessClientSecret
      ? { cloudflareAccessClientId, cloudflareAccessClientSecret }
      : {}),
  };
}

function currentOrigin(): string {
  return typeof window === "undefined" ? "https://app.t3.codes" : window.location.origin;
}

export function parsePairingUrlFields(input: string): RemotePairingFields | null {
  const trimmed = input.trim();
  if (!trimmed) return null;

  try {
    const urlLikeInput =
      /^[a-zA-Z][a-zA-Z\d+.-]*:\/\//u.test(trimmed) || trimmed.startsWith("//")
        ? trimmed
        : `https://${trimmed}`;
    const url = new URL(urlLikeInput, currentOrigin());
    const target = resolveRemotePairingTarget({ pairingUrl: url.toString() });
    return {
      host: target.httpBaseUrl,
      pairingCode: target.credential,
      ...(target.cloudflareAccessToken
        ? { cloudflareAccessToken: target.cloudflareAccessToken }
        : {}),
      ...(target.cloudflareAccessClientId && target.cloudflareAccessClientSecret
        ? {
            cloudflareAccessClientId: target.cloudflareAccessClientId,
            cloudflareAccessClientSecret: target.cloudflareAccessClientSecret,
          }
        : {}),
    };
  } catch {
    return null;
  }
}

export function parseRemotePairingHostChange(input: string): RemotePairingHostChangeFields {
  const parsedPairingUrl = parsePairingUrlFields(input);
  if (parsedPairingUrl) {
    return {
      host: parsedPairingUrl.host,
      pairingCode: parsedPairingUrl.pairingCode,
      cloudflareAccessToken: parsedPairingUrl.cloudflareAccessToken ?? "",
      cloudflareAccessClientId: parsedPairingUrl.cloudflareAccessClientId ?? "",
      cloudflareAccessClientSecret: parsedPairingUrl.cloudflareAccessClientSecret ?? "",
    };
  }

  return {
    host: input,
    cloudflareAccessToken: "",
    cloudflareAccessClientId: "",
    cloudflareAccessClientSecret: "",
  };
}

export function parseRemotePairingFields(input: RemotePairingFieldInput): RemotePairingFields {
  const cloudflareAccess = explicitCloudflareAccessFields(input);
  const parsedPairingUrl = parsePairingUrlFields(input.host);
  if (parsedPairingUrl) {
    return {
      ...parsedPairingUrl,
      ...cloudflareAccess,
    };
  }

  const host = input.host.trim();
  const pairingCode = input.pairingCode.trim();
  if (!host) {
    throw new Error("Enter a backend host.");
  }
  if (!pairingCode) {
    throw new Error("Enter a pairing code.");
  }
  return { host, pairingCode, ...cloudflareAccess };
}

export async function applyWslEnableSelection(input: {
  readonly bridge: WslEnableBridge;
  readonly mode: "both" | "wsl-only";
  readonly nextDistro: string | null;
  readonly persistedDistro: string | null;
}): Promise<DesktopWslState> {
  const { bridge, mode, nextDistro, persistedDistro } = input;

  // Stage every preference before enabling. The desktop only relaunches for
  // mode/distro changes while WSL is active, so the final enable observes the
  // complete selection and is the only call that may relaunch.
  await bridge.setWslOnly(mode === "wsl-only");
  if (persistedDistro !== nextDistro) {
    await bridge.setWslDistro(nextDistro);
  }
  return await bridge.setWslBackendEnabled(true);
}
