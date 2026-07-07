import { describe, expect, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Electron from "electron";
import { beforeEach, vi } from "vite-plus/test";

import * as ElectronWindow from "../../electron/ElectronWindow.ts";
import {
  __testing,
  authenticateCloudflareAccess,
  installCloudflareAccessCookie,
  installCloudflareAccessCredentials,
} from "./cloudflareAccess.ts";
import { normalizeCloudflareAccessOrigin } from "./cloudflareAccessOrigin.ts";

const electronMock = vi.hoisted(() => ({
  cookies: {
    get: vi.fn(),
    remove: vi.fn(),
    set: vi.fn(),
  },
  webRequest: {
    onBeforeSendHeaders: vi.fn(),
  },
}));

vi.mock("electron", () => ({
  session: {
    defaultSession: {
      cookies: electronMock.cookies,
      webRequest: electronMock.webRequest,
    },
  },
}));

beforeEach(() => {
  __testing.resetCloudflareAccessHeaders();
  vi.clearAllMocks();
});

const accessCookie = (overrides: Partial<Electron.Cookie>): Electron.Cookie => ({
  name: "CF_Authorization",
  value: "access-cookie",
  sameSite: "lax",
  ...overrides,
});

function makeAuthWindow() {
  return {
    webContents: {
      session: {
        cookies: electronMock.cookies,
      },
      on: vi.fn(),
      removeListener: vi.fn(),
    },
    once: vi.fn(),
    removeListener: vi.fn(),
    loadURL: vi.fn(),
    isDestroyed: vi.fn(() => false),
    close: vi.fn(),
  };
}

function emitWebContentsEvent(
  authWindow: ReturnType<typeof makeAuthWindow>,
  eventName: string,
  url: string,
) {
  const handler = authWindow.webContents.on.mock.calls.find(([name]) => name === eventName)?.[1];
  if (typeof handler !== "function") {
    throw new Error(`Expected a ${eventName} handler.`);
  }
  handler({} as Electron.Event, url);
}

function electronWindowLayer(
  authWindow: Electron.BrowserWindow,
  onCreate?: (options: Electron.BrowserWindowConstructorOptions) => void,
) {
  return ElectronWindow.ElectronWindow.of({
    create: (options) =>
      Effect.sync(() => {
        onCreate?.(options);
        return authWindow;
      }),
    main: Effect.succeed(Option.none()),
    currentMainOrFirst: Effect.succeed(Option.none()),
    focusedMainOrFirst: Effect.succeed(Option.none()),
    setMain: () => Effect.void,
    clearMain: () => Effect.void,
    reveal: () => Effect.void,
    sendAll: () => Effect.void,
    destroyAll: Effect.void,
    syncAllAppearance: () => Effect.void,
  });
}

describe("normalizeCloudflareAccessOrigin", () => {
  it("normalizes bare, HTTP, and WebSocket backend hosts to HTTP origins", () => {
    expect(normalizeCloudflareAccessOrigin("oc.agriffiths.dev")).toBe("https://oc.agriffiths.dev/");
    expect(normalizeCloudflareAccessOrigin("https://oc.agriffiths.dev/pair#token=abc")).toBe(
      "https://oc.agriffiths.dev/",
    );
    expect(normalizeCloudflareAccessOrigin("wss://oc.agriffiths.dev/ws")).toBe(
      "https://oc.agriffiths.dev/",
    );
    expect(normalizeCloudflareAccessOrigin("ws://127.0.0.1:3773/ws")).toBe(
      "http://127.0.0.1:3773/",
    );
  });
});

describe("desktop Cloudflare Access cookies", () => {
  it.effect("replaces matching Access cookies before installing a saved cookie", () =>
    Effect.gen(function* () {
      const staleCookies = [
        accessCookie({
          value: "stale-wide",
          domain: ".example.test",
          hostOnly: false,
          path: "/team",
          secure: true,
          httpOnly: true,
        }),
        accessCookie({
          value: "stale-host",
          domain: "app.example.test",
          hostOnly: true,
          path: "/",
          secure: true,
          httpOnly: true,
        }),
        accessCookie({
          value: "unrelated",
          domain: ".other.test",
          hostOnly: false,
          path: "/team",
          secure: true,
          httpOnly: true,
        }),
      ];
      electronMock.cookies.get.mockResolvedValue(staleCookies);
      electronMock.cookies.remove.mockResolvedValue(undefined);
      electronMock.cookies.set.mockResolvedValue(undefined);

      yield* installCloudflareAccessCookie.handler({
        host: "https://app.example.test/pair#token=abc",
        cookieValue: "fresh-cookie",
      });

      expect(electronMock.cookies.get).toHaveBeenCalledWith({
        name: "CF_Authorization",
      });
      expect(electronMock.cookies.remove).toHaveBeenNthCalledWith(
        1,
        "https://app.example.test/team",
        "CF_Authorization",
      );
      expect(electronMock.cookies.remove).toHaveBeenNthCalledWith(
        2,
        "https://app.example.test/",
        "CF_Authorization",
      );
      expect(electronMock.cookies.remove).toHaveBeenCalledTimes(2);
      expect(electronMock.cookies.set).toHaveBeenCalledWith({
        url: "https://app.example.test/",
        name: "CF_Authorization",
        value: "fresh-cookie",
        httpOnly: true,
        secure: true,
        sameSite: "no_restriction",
      });
      const [setOrder] = electronMock.cookies.set.mock.invocationCallOrder;
      if (setOrder === undefined) {
        throw new Error("Expected the fresh Access cookie to be installed.");
      }
      expect(Math.max(...electronMock.cookies.remove.mock.invocationCallOrder)).toBeLessThan(
        setOrder,
      );
    }),
  );

  it.effect("attaches saved Cloudflare Access transport through the Electron session", () =>
    Effect.gen(function* () {
      const rendererHeaders: Record<string, string> = {
        "cf-access-client-id": " client-id ",
        "cf-access-client-secret": "client-secret",
        authorization: "Bearer renderer-controlled",
        cookie: "CF_Authorization=renderer-controlled",
      };
      const staleCookie = accessCookie({
        value: "stale-cookie",
        domain: "app.example.test",
        hostOnly: true,
        path: "/",
        secure: true,
      });
      electronMock.cookies.get
        .mockResolvedValueOnce([staleCookie])
        .mockResolvedValueOnce([staleCookie]);
      electronMock.cookies.remove.mockResolvedValue(undefined);
      electronMock.cookies.set.mockResolvedValue(undefined);
      yield* installCloudflareAccessCredentials.handler({
        host: "https://app.example.test",
        headers: rendererHeaders,
        clearCookies: true,
      });

      expect(electronMock.cookies.remove).toHaveBeenCalledWith(
        "https://app.example.test/",
        "CF_Authorization",
      );
      const listener = electronMock.webRequest.onBeforeSendHeaders.mock.calls[0]?.[0];
      expect(listener).toBeTypeOf("function");
      const callback = vi.fn();
      listener(
        {
          url: "wss://app.example.test/ws",
          requestHeaders: {
            "user-agent": "t3code",
            authorization: "Bearer application",
            cookie: "application=1",
          },
        },
        callback,
      );
      expect(callback).toHaveBeenCalledWith({
        requestHeaders: {
          "user-agent": "t3code",
          authorization: "Bearer application",
          cookie: "application=1",
          "cf-access-client-id": "client-id",
          "cf-access-client-secret": "client-secret",
        },
      });

      yield* installCloudflareAccessCredentials.handler({
        host: "https://app.example.test",
        headers: {
          "cf-access-jwt-assertion": " fresh-access-cookie ",
        },
        clearCookies: true,
        cookieValue: "fresh-access-cookie",
      });

      expect(electronMock.cookies.set).toHaveBeenCalledWith({
        url: "https://app.example.test/",
        name: "CF_Authorization",
        value: "fresh-access-cookie",
        httpOnly: true,
        secure: true,
        sameSite: "no_restriction",
      });
      const pairingCallback = vi.fn();
      listener(
        {
          url: "https://app.example.test/.well-known/t3/environment",
          requestHeaders: {
            "user-agent": "t3code",
            Cookie: "application=1; CF_Authorization=stale-cookie",
          },
        },
        pairingCallback,
      );
      expect(pairingCallback).toHaveBeenCalledWith({
        requestHeaders: {
          "user-agent": "t3code",
          "cf-access-jwt-assertion": "fresh-access-cookie",
          Cookie: "application=1; CF_Authorization=fresh-access-cookie",
        },
      });

      yield* installCloudflareAccessCredentials.handler({
        host: "https://app.example.test",
        headers: {},
      });
      expect(electronMock.cookies.get).toHaveBeenCalledTimes(2);
      expect(electronMock.cookies.remove).toHaveBeenCalledTimes(2);
      const clearedCallback = vi.fn();
      listener(
        {
          url: "wss://app.example.test/ws",
          requestHeaders: { "user-agent": "t3code" },
        },
        clearedCallback,
      );
      expect(clearedCallback).toHaveBeenCalledWith({
        requestHeaders: { "user-agent": "t3code" },
      });
    }),
  );

  it.effect("suspends saved Cloudflare Access cookies while reauthenticating", () =>
    Effect.gen(function* () {
      const staleCookie = accessCookie({
        value: "stale-cookie",
        domain: "app.example.test",
        hostOnly: true,
        path: "/",
        secure: true,
      });
      electronMock.cookies.get.mockResolvedValueOnce([staleCookie]);
      electronMock.cookies.remove.mockResolvedValue(undefined);
      electronMock.cookies.set.mockResolvedValue(undefined);

      yield* installCloudflareAccessCredentials.handler({
        host: "https://app.example.test",
        headers: {},
        clearCookies: true,
        cookieValue: "stale-cookie",
      });

      const listener = electronMock.webRequest.onBeforeSendHeaders.mock.calls[0]?.[0];
      expect(listener).toBeTypeOf("function");

      const authWindow = makeAuthWindow();
      const authNavigationCallback = vi.fn();
      authWindow.loadURL.mockImplementation(async () => {
        listener(
          {
            url: "https://app.example.test/",
            requestHeaders: {
              Cookie: "application=1",
            },
          },
          authNavigationCallback,
        );
        return new Promise<never>(() => {});
      });
      electronMock.cookies.get
        .mockResolvedValueOnce([staleCookie])
        .mockResolvedValueOnce([staleCookie])
        .mockResolvedValueOnce([
          accessCookie({
            value: "fresh-cookie",
            domain: "app.example.test",
            hostOnly: true,
            path: "/",
            secure: true,
          }),
        ]);

      const result = yield* authenticateCloudflareAccess
        .handler({ host: "https://app.example.test" })
        .pipe(
          Effect.provideService(
            ElectronWindow.ElectronWindow,
            electronWindowLayer(authWindow as unknown as Electron.BrowserWindow),
          ),
        );

      expect(result).toEqual({ cookieValue: "fresh-cookie" });
      expect(authNavigationCallback).toHaveBeenCalledWith({
        requestHeaders: {
          Cookie: "application=1",
        },
      });

      const postAuthCallback = vi.fn();
      listener(
        {
          url: "https://app.example.test/.well-known/t3/environment",
          requestHeaders: { "user-agent": "t3code" },
        },
        postAuthCallback,
      );
      expect(postAuthCallback).toHaveBeenCalledWith({
        requestHeaders: {
          "user-agent": "t3code",
          "cf-access-jwt-assertion": "fresh-cookie",
          Cookie: "CF_Authorization=fresh-cookie",
        },
      });
    }),
  );

  it.effect("restores suspended Access headers when cancelled login cookie rollback fails", () =>
    Effect.gen(function* () {
      const staleCookie = accessCookie({
        value: "stale-cookie",
        domain: "app.example.test",
        hostOnly: true,
        path: "/",
        secure: true,
      });
      const restoreError = new Error("cookie restore failed");
      electronMock.cookies.get
        .mockResolvedValueOnce([staleCookie])
        .mockResolvedValueOnce([staleCookie])
        .mockResolvedValueOnce([staleCookie])
        .mockResolvedValueOnce([]);
      electronMock.cookies.remove.mockResolvedValue(undefined);
      electronMock.cookies.set.mockResolvedValueOnce(undefined).mockRejectedValueOnce(restoreError);

      yield* installCloudflareAccessCredentials.handler({
        host: "https://app.example.test",
        headers: {},
        clearCookies: true,
        cookieValue: "stale-cookie",
      });

      const listener = electronMock.webRequest.onBeforeSendHeaders.mock.calls[0]?.[0];
      expect(listener).toBeTypeOf("function");

      const authWindow = makeAuthWindow();
      authWindow.loadURL.mockRejectedValue(new Error("load failed"));

      const exit = yield* Effect.exit(
        authenticateCloudflareAccess
          .handler({ host: "https://app.example.test" })
          .pipe(
            Effect.provideService(
              ElectronWindow.ElectronWindow,
              electronWindowLayer(authWindow as unknown as Electron.BrowserWindow),
            ),
          ),
      );

      expect(exit._tag).toBe("Failure");
      if (exit._tag === "Success") {
        return;
      }
      expect(Cause.squash(exit.cause)).toMatchObject({
        reason: "authentication",
        cause: restoreError,
      });

      const callback = vi.fn();
      listener(
        {
          url: "https://app.example.test/.well-known/t3/environment",
          requestHeaders: { "user-agent": "t3code" },
        },
        callback,
      );
      expect(callback).toHaveBeenCalledWith({
        requestHeaders: {
          "user-agent": "t3code",
          Cookie: "CF_Authorization=stale-cookie",
        },
      });
    }),
  );

  it.effect("does not restore stale Cloudflare Access headers over a concurrent update", () =>
    Effect.gen(function* () {
      const staleCookie = accessCookie({
        value: "stale-cookie",
        domain: "app.example.test",
        hostOnly: true,
        path: "/",
        secure: true,
      });
      electronMock.cookies.get.mockResolvedValueOnce([staleCookie]);
      electronMock.cookies.remove.mockResolvedValue(undefined);
      electronMock.cookies.set.mockResolvedValue(undefined);

      yield* installCloudflareAccessCredentials.handler({
        host: "https://app.example.test",
        headers: {},
        clearCookies: true,
        cookieValue: "stale-cookie",
      });

      const listener = electronMock.webRequest.onBeforeSendHeaders.mock.calls[0]?.[0];
      expect(listener).toBeTypeOf("function");

      const suspendedHeaders = __testing.suspendCloudflareAccessHeaders(
        "https://app.example.test/",
      );
      electronMock.cookies.get.mockResolvedValueOnce([staleCookie]);
      yield* installCloudflareAccessCredentials.handler({
        host: "https://app.example.test",
        headers: {},
        clearCookies: true,
        cookieValue: "fresh-cookie",
      });
      __testing.restoreCloudflareAccessHeaders(suspendedHeaders);

      const callback = vi.fn();
      listener(
        {
          url: "https://app.example.test/.well-known/t3/environment",
          requestHeaders: { "user-agent": "t3code" },
        },
        callback,
      );
      expect(callback).toHaveBeenCalledWith({
        requestHeaders: {
          "user-agent": "t3code",
          Cookie: "CF_Authorization=fresh-cookie",
        },
      });
    }),
  );

  it.effect("restores the previous Access header rule when cookie installation fails", () =>
    Effect.gen(function* () {
      const staleCookie = accessCookie({
        value: "stale-cookie",
        domain: "app.example.test",
        hostOnly: true,
        path: "/",
        secure: true,
      });
      electronMock.cookies.get.mockResolvedValueOnce([staleCookie]);
      electronMock.cookies.remove.mockResolvedValue(undefined);
      electronMock.cookies.set.mockResolvedValue(undefined);

      yield* installCloudflareAccessCredentials.handler({
        host: "https://app.example.test",
        headers: {},
        clearCookies: true,
        cookieValue: "stale-cookie",
      });

      const listener = electronMock.webRequest.onBeforeSendHeaders.mock.calls[0]?.[0];
      expect(listener).toBeTypeOf("function");

      const installError = new Error("cookie store rejected fresh cookie");
      electronMock.cookies.get.mockResolvedValueOnce([staleCookie]);
      electronMock.cookies.set.mockRejectedValueOnce(installError);

      const exit = yield* Effect.exit(
        installCloudflareAccessCredentials.handler({
          host: "https://app.example.test",
          headers: {},
          clearCookies: true,
          cookieValue: "fresh-cookie",
        }),
      );

      expect(exit._tag).toBe("Failure");
      const callback = vi.fn();
      listener(
        {
          url: "https://app.example.test/.well-known/t3/environment",
          requestHeaders: { "user-agent": "t3code" },
        },
        callback,
      );
      expect(callback).toHaveBeenCalledWith({
        requestHeaders: {
          "user-agent": "t3code",
          Cookie: "CF_Authorization=stale-cookie",
        },
      });
    }),
  );

  it.effect("restores cancelled login cookies with their original scope", () =>
    Effect.gen(function* () {
      const previousCookies = [
        accessCookie({
          value: "wide-cookie",
          domain: ".example.test",
          hostOnly: false,
          path: "/team",
          secure: true,
          httpOnly: true,
          expirationDate: 1_800_000_000,
        }),
        accessCookie({
          value: "host-cookie",
          domain: "app.example.test",
          hostOnly: true,
          path: "/",
          secure: true,
          httpOnly: false,
          sameSite: "strict",
        }),
      ];
      const authWindow = makeAuthWindow();
      authWindow.loadURL.mockRejectedValue(new Error("load failed"));
      electronMock.cookies.get
        .mockResolvedValueOnce(previousCookies)
        .mockResolvedValueOnce(previousCookies)
        .mockResolvedValueOnce([]);
      electronMock.cookies.remove.mockResolvedValue(undefined);
      electronMock.cookies.set.mockResolvedValue(undefined);

      const exit = yield* Effect.exit(
        authenticateCloudflareAccess
          .handler({ host: "https://app.example.test" })
          .pipe(
            Effect.provideService(
              ElectronWindow.ElectronWindow,
              electronWindowLayer(authWindow as unknown as Electron.BrowserWindow),
            ),
          ),
      );

      expect(exit._tag).toBe("Failure");
      expect(electronMock.cookies.set).toHaveBeenNthCalledWith(1, {
        url: "https://app.example.test/team",
        name: "CF_Authorization",
        value: "wide-cookie",
        domain: ".example.test",
        path: "/team",
        expirationDate: 1_800_000_000,
        httpOnly: true,
        sameSite: "lax",
        secure: true,
      });
      expect(electronMock.cookies.set).toHaveBeenNthCalledWith(2, {
        url: "https://app.example.test/",
        name: "CF_Authorization",
        value: "host-cookie",
        path: "/",
        httpOnly: false,
        sameSite: "strict",
        secure: true,
      });
      expect(authWindow.close).toHaveBeenCalledTimes(1);
    }),
  );

  it.effect(
    "keeps waiting for the Access cookie when Electron aborts the initial load for a redirect",
    () =>
      Effect.gen(function* () {
        const authWindow = makeAuthWindow();
        authWindow.loadURL.mockRejectedValue(
          Object.assign(new Error("ERR_ABORTED (-3) loading https://app.example.test/"), {
            code: "ERR_ABORTED",
            errno: -3,
          }),
        );
        const createdWindows: Electron.BrowserWindowConstructorOptions[] = [];
        electronMock.cookies.get
          .mockResolvedValueOnce([])
          .mockResolvedValueOnce([])
          .mockResolvedValueOnce([accessCookie({ domain: "app.example.test", hostOnly: true })]);
        electronMock.cookies.remove.mockResolvedValue(undefined);

        const result = yield* authenticateCloudflareAccess
          .handler({ host: "https://app.example.test" })
          .pipe(
            Effect.provideService(
              ElectronWindow.ElectronWindow,
              electronWindowLayer(authWindow as unknown as Electron.BrowserWindow, (options) => {
                createdWindows.push(options);
              }),
            ),
          );

        expect(result).toEqual({ cookieValue: "access-cookie" });
        expect(createdWindows[0]?.webPreferences).toEqual(
          expect.objectContaining({
            session: Electron.session.defaultSession,
          }),
        );
        expect(electronMock.cookies.set).not.toHaveBeenCalled();
        expect(authWindow.close).toHaveBeenCalledTimes(1);
      }),
  );

  it.effect(
    "keeps waiting when an HTTP response-code load rejection follows the Access login redirect",
    () =>
      Effect.gen(function* () {
        const authWindow = makeAuthWindow();
        authWindow.loadURL.mockImplementation(async () => {
          emitWebContentsEvent(
            authWindow,
            "did-redirect-navigation",
            "https://team.cloudflareaccess.com/cdn-cgi/access/login/example",
          );
          throw Object.assign(
            new Error("ERR_HTTP_RESPONSE_CODE_FAILURE loading https://app.example.test/"),
            {
              code: "ERR_HTTP_RESPONSE_CODE_FAILURE",
            },
          );
        });
        electronMock.cookies.get
          .mockResolvedValueOnce([])
          .mockResolvedValueOnce([])
          .mockResolvedValueOnce([accessCookie({ domain: "app.example.test", hostOnly: true })]);
        electronMock.cookies.remove.mockResolvedValue(undefined);

        const result = yield* authenticateCloudflareAccess
          .handler({ host: "https://app.example.test" })
          .pipe(
            Effect.provideService(
              ElectronWindow.ElectronWindow,
              electronWindowLayer(authWindow as unknown as Electron.BrowserWindow),
            ),
          );

        expect(result).toEqual({ cookieValue: "access-cookie" });
        expect(authWindow.webContents.removeListener).toHaveBeenCalledWith(
          "did-redirect-navigation",
          expect.any(Function),
        );
        expect(authWindow.close).toHaveBeenCalledTimes(1);
      }),
  );

  it.effect("closes the auth window when pre-login cookie cleanup fails", () =>
    Effect.gen(function* () {
      const authWindow = makeAuthWindow();
      const cookieError = new Error("cookie store unavailable");
      electronMock.cookies.get.mockRejectedValue(cookieError);

      const exit = yield* Effect.exit(
        authenticateCloudflareAccess
          .handler({ host: "https://app.example.test" })
          .pipe(
            Effect.provideService(
              ElectronWindow.ElectronWindow,
              electronWindowLayer(authWindow as unknown as Electron.BrowserWindow),
            ),
          ),
      );

      expect(exit._tag).toBe("Failure");
      if (exit._tag === "Success") {
        return;
      }
      const error = Cause.squash(exit.cause);
      expect(error).toMatchObject({
        reason: "authentication",
        cause: cookieError,
      });
      expect(authWindow.loadURL).not.toHaveBeenCalled();
      expect(authWindow.close).toHaveBeenCalledTimes(1);
    }),
  );
});
