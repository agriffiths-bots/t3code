import {
  DesktopCloudflareAccessCookieInstallInputSchema,
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
const ACCESS_LOGIN_TIMEOUT_MS = 5 * 60 * 1000;
const COOKIE_POLL_INTERVAL_MS = 500;

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
  return session.cookies.get({ url: origin, name: CLOUDFLARE_ACCESS_COOKIE_NAME });
}

function readAccessCookie(session: Electron.Session, origin: string): Promise<string | undefined> {
  return readAccessCookies(session, origin).then(
    (cookies) => cookies.find((cookie) => cookie.value.trim().length > 0)?.value,
  );
}

function restoreAccessCookie(
  session: Electron.Session,
  origin: string,
  cookie: Electron.Cookie | undefined,
): Promise<void> {
  if (cookie === undefined || cookie.value.trim().length === 0) {
    return Promise.resolve();
  }
  return session.cookies
    .set({
      url: origin,
      name: CLOUDFLARE_ACCESS_COOKIE_NAME,
      value: cookie.value,
      ...(cookie.expirationDate === undefined ? {} : { expirationDate: cookie.expirationDate }),
      ...(cookie.httpOnly === undefined ? {} : { httpOnly: cookie.httpOnly }),
      ...(cookie.sameSite === undefined ? {} : { sameSite: cookie.sameSite }),
      ...(cookie.secure === undefined ? {} : { secure: cookie.secure }),
    })
    .catch((cause) => {
      throw new DesktopCloudflareAccessLoginError({
        reason: "authentication",
        detail: "Could not restore the previous Cloudflare Access session cookie.",
        cause,
      });
    });
}

function clearAccessCookie(session: Electron.Session, origin: string): Promise<void> {
  return session.cookies.remove(origin, CLOUDFLARE_ACCESS_COOKIE_NAME).catch((cause) => {
    throw new DesktopCloudflareAccessLoginError({
      reason: "authentication",
      detail: "Could not reset the existing Cloudflare Access session cookie.",
      cause,
    });
  });
}

function captureCloudflareAccessCookie(options: {
  readonly origin: string;
  readonly parent: Electron.BrowserWindow | undefined;
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
            sandbox: true,
            contextIsolation: true,
            nodeIntegration: false,
          },
        }),
      );
      const session = authWindow.webContents.session;
      const previousAccessCookie = (await readAccessCookies(session, options.origin)).find(
        (cookie) => cookie.value.trim().length > 0,
      );
      await clearAccessCookie(session, options.origin);
      let capturedReplacementCookie = false;

      const abort = new AbortController();
      let closeHandler: (() => void) | undefined;
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
          throw new DesktopCloudflareAccessLoginError({
            reason: "authentication",
            detail: "Could not open the Cloudflare Access sign-in page.",
            cause,
          });
        },
      );
      try {
        const cookieValue = await Promise.race([
          waitForCookie(),
          waitForClose,
          waitForTimeout,
          waitForLoadFailure,
        ]);
        capturedReplacementCookie = true;
        return cookieValue;
      } finally {
        if (!capturedReplacementCookie) {
          await restoreAccessCookie(session, options.origin, previousAccessCookie);
        }
        abort.abort();
        if (closeHandler !== undefined) {
          authWindow.removeListener("closed", closeHandler);
        }
        if (!authWindow.isDestroyed()) {
          authWindow.close();
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
      try: () => {
        const isSecureOrigin = origin.startsWith("https://");
        return Electron.session.defaultSession.cookies.set({
          url: origin,
          name: CLOUDFLARE_ACCESS_COOKIE_NAME,
          value: input.cookieValue,
          httpOnly: true,
          secure: isSecureOrigin,
          ...(isSecureOrigin ? { sameSite: "no_restriction" as const } : {}),
        });
      },
      catch: (cause) =>
        new DesktopCloudflareAccessLoginError({
          reason: "authentication",
          detail: "Could not restore the Cloudflare Access session cookie.",
          cause,
        }),
    });
  }),
});
