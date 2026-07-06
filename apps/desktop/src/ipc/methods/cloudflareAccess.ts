import {
  DesktopCloudflareAccessCookieInstallInputSchema,
  DesktopCloudflareAccessCredentialsInstallInputSchema,
  DesktopCloudflareAccessLoginInputSchema,
  DesktopCloudflareAccessLoginResultSchema,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Electron from "electron";
import * as NodeTimersPromises from "node:timers/promises";

import * as ElectronWindow from "../../electron/ElectronWindow.ts";
import * as IpcChannels from "../channels.ts";
import * as DesktopIpc from "../DesktopIpc.ts";
import { normalizeCloudflareAccessOrigin } from "./cloudflareAccessOrigin.ts";

const CLOUDFLARE_ACCESS_COOKIE_NAME = "CF_Authorization";
const CLOUDFLARE_ACCESS_COOKIE_HEADER_NAME = "Cookie";
const CLOUDFLARE_ACCESS_TRANSPORT_HEADER_NAMES = [
  "cf-access-client-id",
  "cf-access-client-secret",
  "cf-access-jwt-assertion",
] as const;
const CLOUDFLARE_ACCESS_TRANSPORT_HEADER_NAME_SET = new Set<string>(
  CLOUDFLARE_ACCESS_TRANSPORT_HEADER_NAMES,
);
const ACCESS_LOGIN_TIMEOUT_MS = 5 * 60 * 1000;
const COOKIE_POLL_INTERVAL_MS = 500;
const cloudflareAccessHeaderRules = new Map<string, Readonly<Record<string, string>>>();
const cloudflareAccessHeaderRuleVersions = new Map<string, number>();
let cloudflareAccessHeaderHookInstalled = false;

export class DesktopCloudflareAccessLoginError extends Schema.TaggedErrorClass<DesktopCloudflareAccessLoginError>()(
  "DesktopCloudflareAccessLoginError",
  {
    reason: Schema.Literals(["configuration", "cancelled", "timeout", "authentication"]),
    detail: Schema.String,
    cause: Schema.optionalKey(Schema.Defect()),
  },
) {
  override get message(): string {
    return this.detail;
  }
}
const isDesktopCloudflareAccessLoginError = Schema.is(DesktopCloudflareAccessLoginError);

function readAccessCookies(session: Electron.Session, origin: string): Promise<Electron.Cookie[]> {
  return session.cookies
    .get({ name: CLOUDFLARE_ACCESS_COOKIE_NAME })
    .then((cookies) => cookies.filter((cookie) => cookieMatchesOrigin(cookie, origin)));
}

function readAccessCookie(session: Electron.Session, origin: string): Promise<string | undefined> {
  return readAccessCookies(session, origin).then(
    (cookies) => cookies.find((cookie) => cookie.value.trim().length > 0)?.value,
  );
}

function cookieMatchesOrigin(cookie: Electron.Cookie, origin: string): boolean {
  const url = new URL(origin);
  if (cookie.secure === true && url.protocol !== "https:") {
    return false;
  }

  const host = url.hostname.toLowerCase();
  const domain = cookie.domain?.replace(/^\./, "").toLowerCase();
  if (!domain) {
    return false;
  }
  if (cookie.hostOnly === true) {
    return host === domain;
  }
  return host === domain || host.endsWith(`.${domain}`);
}

function cookieScopeUrl(origin: string, cookie: Pick<Electron.Cookie, "path">): string {
  const url = new URL(origin);
  const path = cookie.path?.trim() ?? "";
  url.pathname = path.startsWith("/") ? path : "/";
  url.search = "";
  url.hash = "";
  return url.toString();
}

function accessCookieSetDetails(
  origin: string,
  cookie: Electron.Cookie,
): Electron.CookiesSetDetails {
  return {
    url: cookieScopeUrl(origin, cookie),
    name: CLOUDFLARE_ACCESS_COOKIE_NAME,
    value: cookie.value,
    ...(cookie.hostOnly === false && cookie.domain ? { domain: cookie.domain } : {}),
    ...(cookie.path ? { path: cookie.path } : {}),
    ...(cookie.expirationDate === undefined ? {} : { expirationDate: cookie.expirationDate }),
    ...(cookie.httpOnly === undefined ? {} : { httpOnly: cookie.httpOnly }),
    ...(cookie.sameSite === undefined ? {} : { sameSite: cookie.sameSite }),
    ...(cookie.secure === undefined ? {} : { secure: cookie.secure }),
  };
}

async function restoreAccessCookies(
  session: Electron.Session,
  origin: string,
  cookies: ReadonlyArray<Electron.Cookie>,
): Promise<void> {
  for (const cookie of cookies) {
    if (cookie.value.trim().length === 0) {
      continue;
    }
    await session.cookies.set(accessCookieSetDetails(origin, cookie)).catch((cause) => {
      throw new DesktopCloudflareAccessLoginError({
        reason: "authentication",
        detail: "Could not restore the previous Cloudflare Access session cookie.",
        cause,
      });
    });
  }
}

async function clearAccessCookies(session: Electron.Session, origin: string): Promise<void> {
  const cookies = await readAccessCookies(session, origin);
  for (const cookie of cookies) {
    await session.cookies
      .remove(cookieScopeUrl(origin, cookie), CLOUDFLARE_ACCESS_COOKIE_NAME)
      .catch((cause) => {
        throw new DesktopCloudflareAccessLoginError({
          reason: "authentication",
          detail: "Could not reset the existing Cloudflare Access session cookie.",
          cause,
        });
      });
  }
}

function installAccessCookie(
  session: Electron.Session,
  origin: string,
  cookieValue: string,
): Promise<void> {
  const isSecureOrigin = origin.startsWith("https://");
  return session.cookies
    .set({
      url: origin,
      name: CLOUDFLARE_ACCESS_COOKIE_NAME,
      value: cookieValue,
      httpOnly: true,
      secure: isSecureOrigin,
      ...(isSecureOrigin ? { sameSite: "no_restriction" as const } : {}),
    })
    .catch((cause) => {
      throw new DesktopCloudflareAccessLoginError({
        reason: "authentication",
        detail: "Could not restore the Cloudflare Access session cookie.",
        cause,
      });
    });
}

function isCloudflareAccessLoginUrl(value: string): boolean {
  try {
    const url = new URL(value);
    return (
      url.protocol === "https:" &&
      (url.hostname === "cloudflareaccess.com" || url.hostname.endsWith(".cloudflareaccess.com")) &&
      url.pathname.startsWith("/cdn-cgi/access/login")
    );
  } catch {
    return false;
  }
}

function cloudflareAccessRuleOrigin(value: string): string | null {
  try {
    const url = new URL(value);
    if (url.protocol === "ws:") {
      url.protocol = "http:";
    } else if (url.protocol === "wss:") {
      url.protocol = "https:";
    }
    return url.origin;
  } catch {
    return null;
  }
}

function filteredHeaders(
  headers: Readonly<Record<string, string | undefined>>,
): Readonly<Record<string, string>> {
  const normalizedHeaders = new Map<string, string>();
  for (const [rawName, rawValue] of Object.entries(headers)) {
    const name = rawName.toLowerCase();
    if (!CLOUDFLARE_ACCESS_TRANSPORT_HEADER_NAME_SET.has(name)) {
      continue;
    }
    const value = rawValue?.trim() ?? "";
    if (value.length > 0) {
      normalizedHeaders.set(name, value);
    }
  }
  return Object.fromEntries(
    CLOUDFLARE_ACCESS_TRANSPORT_HEADER_NAMES.flatMap((name) => {
      const value = normalizedHeaders.get(name);
      return value === undefined ? [] : [[name, value] as const];
    }),
  );
}

function readHeader(
  headers: Readonly<Record<string, string | undefined>>,
  name: string,
): string | undefined {
  const normalizedName = name.toLowerCase();
  for (const [rawName, rawValue] of Object.entries(headers)) {
    if (rawName.toLowerCase() === normalizedName) {
      return rawValue;
    }
  }
  return undefined;
}

function cloudflareAccessCookieHeader(cookieValue: string | undefined): string | undefined {
  const value = cookieValue?.trim() ?? "";
  return value.length > 0 ? `${CLOUDFLARE_ACCESS_COOKIE_NAME}=${value}` : undefined;
}

function mergeCookieHeader(existingHeader: string | undefined, accessCookieHeader: string): string {
  const existingCookies = (existingHeader ?? "")
    .split(";")
    .map((part) => part.trim())
    .filter(
      (part) =>
        part.length > 0 &&
        !part.toLowerCase().startsWith(`${CLOUDFLARE_ACCESS_COOKIE_NAME.toLowerCase()}=`),
    );
  return [...existingCookies, accessCookieHeader].join("; ");
}

function applyCloudflareAccessHeaders(
  requestHeaders: Readonly<Record<string, string>>,
  accessHeaders: Readonly<Record<string, string>>,
): Readonly<Record<string, string>> {
  const next: Record<string, string> = { ...requestHeaders };
  const accessCookieHeader = readHeader(accessHeaders, CLOUDFLARE_ACCESS_COOKIE_HEADER_NAME);

  for (const [name, value] of Object.entries(accessHeaders)) {
    if (name.toLowerCase() === CLOUDFLARE_ACCESS_COOKIE_HEADER_NAME.toLowerCase()) {
      continue;
    }
    next[name] = value;
  }

  if (accessCookieHeader !== undefined) {
    for (const name of Object.keys(next)) {
      if (name.toLowerCase() === CLOUDFLARE_ACCESS_COOKIE_HEADER_NAME.toLowerCase()) {
        delete next[name];
      }
    }
    next[CLOUDFLARE_ACCESS_COOKIE_HEADER_NAME] = mergeCookieHeader(
      readHeader(requestHeaders, CLOUDFLARE_ACCESS_COOKIE_HEADER_NAME),
      accessCookieHeader,
    );
  }

  return next;
}

function installCloudflareAccessHeaderHook(session: Electron.Session) {
  if (cloudflareAccessHeaderHookInstalled) {
    return;
  }
  cloudflareAccessHeaderHookInstalled = true;
  session.webRequest.onBeforeSendHeaders((details, callback) => {
    const origin = cloudflareAccessRuleOrigin(details.url);
    const headers = origin === null ? undefined : cloudflareAccessHeaderRules.get(origin);
    if (headers === undefined || Object.keys(headers).length === 0) {
      callback({ requestHeaders: details.requestHeaders });
      return;
    }
    callback({
      requestHeaders: applyCloudflareAccessHeaders(details.requestHeaders, headers),
    });
  });
}

function bumpCloudflareAccessHeaderRuleVersion(origin: string): number {
  const nextVersion = (cloudflareAccessHeaderRuleVersions.get(origin) ?? 0) + 1;
  cloudflareAccessHeaderRuleVersions.set(origin, nextVersion);
  return nextVersion;
}

function configureCloudflareAccessHeaders(
  session: Electron.Session,
  origin: string,
  headers: Readonly<Record<string, string>>,
  cookieValue?: string,
) {
  const requestOriginKey = cloudflareAccessRuleOrigin(origin);
  if (requestOriginKey === null) {
    return;
  }
  const cookieHeader = cloudflareAccessCookieHeader(cookieValue);
  const nextHeaders = {
    ...filteredHeaders(headers),
    ...(cookieHeader
      ? {
          [CLOUDFLARE_ACCESS_COOKIE_HEADER_NAME]: cookieHeader,
        }
      : {}),
  };
  if (Object.keys(nextHeaders).length === 0) {
    cloudflareAccessHeaderRules.delete(requestOriginKey);
    bumpCloudflareAccessHeaderRuleVersion(requestOriginKey);
    return;
  }
  cloudflareAccessHeaderRules.set(requestOriginKey, nextHeaders);
  bumpCloudflareAccessHeaderRuleVersion(requestOriginKey);
  installCloudflareAccessHeaderHook(session);
}

interface SuspendedCloudflareAccessHeaders {
  readonly requestOriginKey: string;
  readonly headers: Readonly<Record<string, string>> | undefined;
  readonly version: number;
}

function suspendCloudflareAccessHeaders(
  origin: string,
): SuspendedCloudflareAccessHeaders | undefined {
  const requestOriginKey = cloudflareAccessRuleOrigin(origin);
  if (requestOriginKey === null) {
    return undefined;
  }
  const previousHeaders = cloudflareAccessHeaderRules.get(requestOriginKey);
  cloudflareAccessHeaderRules.delete(requestOriginKey);
  return {
    requestOriginKey,
    headers: previousHeaders,
    version: bumpCloudflareAccessHeaderRuleVersion(requestOriginKey),
  };
}

function restoreCloudflareAccessHeaders(suspended: SuspendedCloudflareAccessHeaders | undefined) {
  if (suspended === undefined) {
    return;
  }
  if (
    cloudflareAccessHeaderRules.has(suspended.requestOriginKey) ||
    (cloudflareAccessHeaderRuleVersions.get(suspended.requestOriginKey) ?? 0) !== suspended.version
  ) {
    return;
  }
  if (suspended.headers === undefined || Object.keys(suspended.headers).length === 0) {
    return;
  }
  cloudflareAccessHeaderRules.set(suspended.requestOriginKey, suspended.headers);
  bumpCloudflareAccessHeaderRuleVersion(suspended.requestOriginKey);
}

function errorText(cause: unknown): string {
  if (cause instanceof Error) {
    return `${cause.name} ${cause.message}`;
  }
  return String(cause);
}

function errorField(cause: unknown, field: string): string {
  if (cause === null || typeof cause !== "object" || !(field in cause)) {
    return "";
  }
  const value = (cause as Record<string, unknown>)[field];
  return typeof value === "string" || typeof value === "number" ? String(value) : "";
}

function isLoadUrlRedirectInterruption(
  cause: unknown,
  observedCloudflareAccessLoginNavigation: boolean,
): boolean {
  const text = errorText(cause);
  const code = errorField(cause, "code");
  const errno = errorField(cause, "errno");
  const errorCode = errorField(cause, "errorCode");
  const combined = `${code} ${errno} ${errorCode} ${text}`;
  if (
    combined.includes("ERR_ABORTED") ||
    errno === "-3" ||
    errorCode === "-3" ||
    /(^|[^\d])-3([^\d]|$)/u.test(text)
  ) {
    return true;
  }
  if (
    combined.includes("ERR_HTTP_RESPONSE_CODE_FAILURE") &&
    (observedCloudflareAccessLoginNavigation ||
      text.includes("cloudflareaccess.com/cdn-cgi/access/login"))
  ) {
    return true;
  }
  return false;
}

function captureCloudflareAccessCookie(options: {
  readonly origin: string;
  readonly parent: Electron.BrowserWindow | undefined;
  readonly session: Electron.Session;
  readonly createWindow: ElectronWindow.ElectronWindow["Service"]["create"];
}) {
  return Effect.tryPromise({
    try: async () => {
      const authWindow = await Effect.runPromise(
        options.createWindow({
          title: "Cloudflare Access",
          width: 520,
          height: 720,
          minWidth: 420,
          minHeight: 560,
          show: true,
          modal: false,
          ...(options.parent ? { parent: options.parent } : {}),
          webPreferences: {
            session: options.session,
            sandbox: true,
            contextIsolation: true,
            nodeIntegration: false,
          },
        }),
      );
      const session = options.session;
      const abort = new AbortController();
      let closeHandler: (() => void) | undefined;
      let observedCloudflareAccessLoginNavigation = false;
      let capturedReplacementCookie = false;
      let previousAccessCookies: ReadonlyArray<Electron.Cookie> = [];
      const suspendedAccessHeaders = suspendCloudflareAccessHeaders(options.origin);
      const observeNavigation = (_event: Electron.Event, navigationUrl: string) => {
        if (isCloudflareAccessLoginUrl(navigationUrl)) {
          observedCloudflareAccessLoginNavigation = true;
        }
      };
      authWindow.webContents.on("did-start-navigation", observeNavigation);
      authWindow.webContents.on("will-redirect", observeNavigation);
      authWindow.webContents.on("did-redirect-navigation", observeNavigation);

      try {
        previousAccessCookies = (await readAccessCookies(session, options.origin)).filter(
          (cookie) => cookie.value.trim().length > 0,
        );
        await clearAccessCookies(session, options.origin);

        const waitForCookie = async () => {
          while (!abort.signal.aborted) {
            const cookieValue = await readAccessCookie(session, options.origin).catch((cause) => {
              throw new DesktopCloudflareAccessLoginError({
                reason: "authentication",
                detail: "Could not read the Cloudflare Access session cookie.",
                cause,
              });
            });
            if (cookieValue !== undefined) {
              return cookieValue;
            }
            await NodeTimersPromises.setTimeout(COOKIE_POLL_INTERVAL_MS, undefined, {
              signal: abort.signal,
            }).catch(() => {
              // The controller is aborted during normal cleanup after the race settles.
            });
          }
          throw new DesktopCloudflareAccessLoginError({
            reason: "cancelled",
            detail: "Cloudflare Access sign-in was cancelled.",
          });
        };
        const waitForClose = new Promise<never>((_, reject) => {
          closeHandler = () => {
            reject(
              new DesktopCloudflareAccessLoginError({
                reason: "cancelled",
                detail: "Cloudflare Access sign-in was cancelled.",
              }),
            );
          };
          authWindow.once("closed", closeHandler);
        });
        const waitForTimeout = NodeTimersPromises.setTimeout(ACCESS_LOGIN_TIMEOUT_MS, undefined, {
          signal: abort.signal,
        })
          .then(() => {
            throw new DesktopCloudflareAccessLoginError({
              reason: "timeout",
              detail: "Cloudflare Access sign-in timed out.",
            });
          })
          .catch((cause) => {
            if (abort.signal.aborted) {
              return new Promise<never>(() => {});
            }
            throw cause;
          });
        const waitForLoadFailure = authWindow.loadURL(options.origin).then(
          () => new Promise<never>(() => {}),
          (cause: unknown) => {
            if (isLoadUrlRedirectInterruption(cause, observedCloudflareAccessLoginNavigation)) {
              return new Promise<never>(() => {});
            }
            throw new DesktopCloudflareAccessLoginError({
              reason: "authentication",
              detail: "Could not open the Cloudflare Access sign-in page.",
              cause,
            });
          },
        );
        const cookieValue = await Promise.race([
          waitForCookie(),
          waitForClose,
          waitForTimeout,
          waitForLoadFailure,
        ]);
        capturedReplacementCookie = true;
        return cookieValue;
      } finally {
        try {
          if (!capturedReplacementCookie) {
            await restoreAccessCookies(session, options.origin, previousAccessCookies);
            restoreCloudflareAccessHeaders(suspendedAccessHeaders);
          }
        } finally {
          abort.abort();
          authWindow.webContents.removeListener("did-start-navigation", observeNavigation);
          authWindow.webContents.removeListener("will-redirect", observeNavigation);
          authWindow.webContents.removeListener("did-redirect-navigation", observeNavigation);
          if (closeHandler !== undefined) {
            authWindow.removeListener("closed", closeHandler);
          }
          if (!authWindow.isDestroyed()) {
            authWindow.close();
          }
        }
      }
    },
    catch: (cause) =>
      isDesktopCloudflareAccessLoginError(cause)
        ? cause
        : new DesktopCloudflareAccessLoginError({
            reason: "authentication",
            detail: "Cloudflare Access sign-in failed.",
            cause,
          }),
  });
}

export const authenticateCloudflareAccess = DesktopIpc.makeIpcMethod({
  channel: IpcChannels.AUTHENTICATE_CLOUDFLARE_ACCESS_CHANNEL,
  payload: DesktopCloudflareAccessLoginInputSchema,
  result: DesktopCloudflareAccessLoginResultSchema,
  handler: Effect.fn("desktop.ipc.cloudflareAccess.authenticate")(function* (input) {
    const electronWindow = yield* ElectronWindow.ElectronWindow;
    const parent = yield* electronWindow.focusedMainOrFirst;
    const origin = yield* Effect.try({
      try: () => normalizeCloudflareAccessOrigin(input.host),
      catch: (cause) =>
        isDesktopCloudflareAccessLoginError(cause)
          ? cause
          : new DesktopCloudflareAccessLoginError({
              reason: "configuration",
              detail: "Enter a valid remote host before signing in with Cloudflare Access.",
              cause,
            }),
    });
    const cookieValue = yield* captureCloudflareAccessCookie({
      origin,
      parent: Option.getOrUndefined(parent),
      session: Electron.session.defaultSession,
      createWindow: electronWindow.create,
    });
    return { cookieValue };
  }),
});

export const installCloudflareAccessCookie = DesktopIpc.makeIpcMethod({
  channel: IpcChannels.INSTALL_CLOUDFLARE_ACCESS_COOKIE_CHANNEL,
  payload: DesktopCloudflareAccessCookieInstallInputSchema,
  result: Schema.Void,
  handler: Effect.fn("desktop.ipc.cloudflareAccess.installCookie")(function* (input) {
    const origin = yield* Effect.try({
      try: () => normalizeCloudflareAccessOrigin(input.host),
      catch: (cause) =>
        new DesktopCloudflareAccessLoginError({
          reason: "configuration",
          detail: "Enter a valid remote host before restoring Cloudflare Access.",
          cause,
        }),
    });
    yield* Effect.tryPromise({
      try: async () => {
        await clearAccessCookies(Electron.session.defaultSession, origin);
        await installAccessCookie(Electron.session.defaultSession, origin, input.cookieValue);
      },
      catch: (cause) =>
        isDesktopCloudflareAccessLoginError(cause)
          ? cause
          : new DesktopCloudflareAccessLoginError({
              reason: "authentication",
              detail: "Could not restore the Cloudflare Access session cookie.",
              cause,
            }),
    });
  }),
});

export const installCloudflareAccessCredentials = DesktopIpc.makeIpcMethod({
  channel: IpcChannels.INSTALL_CLOUDFLARE_ACCESS_CREDENTIALS_CHANNEL,
  payload: DesktopCloudflareAccessCredentialsInstallInputSchema,
  result: Schema.Void,
  handler: Effect.fn("desktop.ipc.cloudflareAccess.installCredentials")(function* (input) {
    const origin = yield* Effect.try({
      try: () => normalizeCloudflareAccessOrigin(input.host),
      catch: (cause) =>
        new DesktopCloudflareAccessLoginError({
          reason: "configuration",
          detail: "Enter a valid remote host before installing Cloudflare Access credentials.",
          cause,
        }),
    });
    yield* Effect.tryPromise({
      try: async () => {
        const session = Electron.session.defaultSession;
        const cookieValue = input.cookieValue?.trim() ?? "";
        if (input.clearCookies === true || cookieValue.length > 0) {
          await clearAccessCookies(session, origin);
        }
        configureCloudflareAccessHeaders(session, origin, input.headers, cookieValue);
        if (cookieValue.length > 0) {
          await installAccessCookie(session, origin, cookieValue);
        }
      },
      catch: (cause) =>
        isDesktopCloudflareAccessLoginError(cause)
          ? cause
          : new DesktopCloudflareAccessLoginError({
              reason: "authentication",
              detail: "Could not install the Cloudflare Access credentials.",
              cause,
            }),
    });
  }),
});

export const __testing = {
  resetCloudflareAccessHeaders: () => {
    cloudflareAccessHeaderRules.clear();
    cloudflareAccessHeaderRuleVersions.clear();
    cloudflareAccessHeaderHookInstalled = false;
  },
  restoreCloudflareAccessHeaders,
  suspendCloudflareAccessHeaders,
};
