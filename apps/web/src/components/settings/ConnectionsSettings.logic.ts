import type {
  AdvertisedEndpoint,
  DesktopBridge,
  DesktopWslState,
  MatrixBridgeConfigView,
  MatrixBridgeConfigureInput,
} from "@t3tools/contracts";

import type { MatrixBridgeStatusView } from "~/state/matrixBridge";
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

/**
 * Matrix bridge subsection.
 *
 * The bot access token is write-only on the server: it is accepted by
 * `matrixBridge.configure` and is never readable again. Everything here is
 * built around that, so the token only ever exists in the draft the user is
 * currently typing.
 */
export interface MatrixBridgeFormDraft {
  readonly homeserverUrl: string;
  readonly accessToken: string;
  /** Free text; one Matrix ID per line, comma, or space. */
  readonly allowedUserIds: string;
}

export const EMPTY_MATRIX_BRIDGE_DRAFT: MatrixBridgeFormDraft = {
  homeserverUrl: "",
  accessToken: "",
  allowedUserIds: "",
};

/** Mirrors the server's Matrix user id check so the form rejects typos locally. */
const MATRIX_USER_ID_PATTERN = /^@[^\s:]+:[^\s]+$/u;

export type MatrixBridgeSectionAccess = "hidden" | "status-only" | "manage";

/**
 * Absent capability hides the subsection entirely (old servers have no bridge
 * RPCs at all). Without `access:write` the status is still readable, because
 * the status subscription only needs `orchestration:read`, but nothing can be
 * configured or minted.
 */
export function matrixBridgeSectionAccess(input: {
  readonly supported: boolean;
  readonly canManageAccess: boolean;
}): MatrixBridgeSectionAccess {
  if (!input.supported) {
    return "hidden";
  }
  return input.canManageAccess ? "manage" : "status-only";
}

export interface MatrixBridgeStatusLabel {
  readonly title: string;
  readonly description: string;
}

/**
 * Lifecycle state as one heading plus one line of what to do next. `degraded`
 * and `unavailable` prefer the server's reason, which is already sanitized for
 * operators (no token, room id, or message text).
 *
 * A failed subscription reads as "cannot see", never as "not connected": a
 * client without `orchestration:read` genuinely cannot watch the bridge, and
 * claiming it is off would be a lie it could act on.
 */
export function matrixBridgeStatusLabel(view: MatrixBridgeStatusView): MatrixBridgeStatusLabel {
  if (view.kind === "pending") {
    return {
      title: "Checking",
      description: "Reading the Matrix bridge status from this environment.",
    };
  }
  if (view.kind === "unavailable") {
    return {
      title: "Status unavailable",
      description:
        "This client cannot read the bridge status. It needs the view-environment scope, or the connection dropped.",
    };
  }
  const status = view.status;
  switch (status.state) {
    case "disabled":
      return {
        title: "Not connected",
        description:
          "Connect a Matrix bot account to bridge one thread into a private encrypted room.",
      };
    case "connecting":
      return {
        title: "Connecting",
        description: "Signing in to the homeserver and preparing the encrypted room.",
      };
    case "waiting-for-member":
      return {
        title: "Waiting for you to join",
        description: "Accept the room invitation from your Matrix account to continue.",
      };
    case "awaiting-pairing":
      return {
        title: "Waiting for a pairing code",
        description: "Send a pairing code from this panel as a message in the Matrix room.",
      };
    case "active":
      return {
        title: "Active",
        description:
          "Messages in the Matrix room reach the bridged thread, and its replies come back.",
      };
    case "degraded":
      return {
        title: "Degraded",
        description: status.reason ?? "The bridge is connected but is not delivering everything.",
      };
    case "unavailable":
      return {
        title: "Unavailable",
        description: status.reason ?? "The bridge cannot run on this server right now.",
      };
  }
}

/**
 * Whether the form is creating a connection or replacing one. "unknown" covers
 * a status this client cannot read: the write controls stay available, because
 * `access:write` alone entitles a session to configure and disconnect.
 */
export type MatrixBridgeConnectionMode = "unknown" | "connect" | "reconfigure";

export function matrixBridgeConnectionMode(
  view: MatrixBridgeStatusView,
): MatrixBridgeConnectionMode {
  if (view.kind !== "status") {
    return "unknown";
  }
  return view.status.state === "disabled" ? "connect" : "reconfigure";
}

/** Disconnect is hidden only where the status positively says there is nothing to disconnect. */
export function showMatrixBridgeDisconnect(mode: MatrixBridgeConnectionMode): boolean {
  return mode !== "connect";
}

/**
 * A minted pairing credential and the moment it stops being redeemable. The
 * server's one-time credentials expire in minutes, so the panel keeps the
 * expiry rather than the bare string.
 */
export interface MatrixBridgePairingCode {
  readonly credential: string;
  readonly expiresAtMs: number;
}

/**
 * The code only while it can still be consumed. An expired credential is worth
 * less than nothing on screen: sending it fails the pairing attempt and reads
 * as a broken bridge rather than a stale code.
 */
export function activeMatrixBridgePairingCode(
  code: MatrixBridgePairingCode | null,
  nowMs: number,
): MatrixBridgePairingCode | null {
  return code !== null && code.expiresAtMs > nowMs ? code : null;
}

export function parseMatrixBridgeAllowedUserIds(raw: string): ReadonlyArray<string> {
  const userIds = [...new Set(raw.split(/[\s,]+/u).filter((value) => value.length > 0))];
  if (userIds.length === 0) {
    throw new Error("Enter at least one Matrix user ID.");
  }
  const invalid = userIds.find((userId) => !MATRIX_USER_ID_PATTERN.test(userId));
  if (invalid !== undefined) {
    throw new Error(`"${invalid}" is not a Matrix user ID. They look like @you:beeper.com.`);
  }
  return userIds;
}

/**
 * Reconfiguring always needs the token again, because the server never returns
 * it and any identity change starts a fresh crypto store anyway.
 */
export function parseMatrixBridgeConfigureInput(
  draft: MatrixBridgeFormDraft,
): MatrixBridgeConfigureInput {
  const homeserverUrl = draft.homeserverUrl.trim();
  if (homeserverUrl.length === 0) {
    throw new Error("Enter the Matrix homeserver URL.");
  }
  const accessToken = draft.accessToken.trim();
  if (accessToken.length === 0) {
    throw new Error("Enter the bot access token.");
  }
  return {
    homeserverUrl,
    accessToken,
    allowedUserIds: parseMatrixBridgeAllowedUserIds(draft.allowedUserIds),
  };
}

/**
 * After a successful connect the form keeps the readable fields (so the next
 * edit starts from what the server stored) and drops the token, which must not
 * outlive the request that carried it.
 */
export function matrixBridgeDraftAfterConfigure(
  view: MatrixBridgeConfigView,
): MatrixBridgeFormDraft {
  return {
    homeserverUrl: view.homeserverUrl,
    accessToken: "",
    allowedUserIds: view.allowedUserIds.join("\n"),
  };
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
