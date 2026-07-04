import { describe, expect, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import type * as Electron from "electron";
import { beforeEach, vi } from "vite-plus/test";

import * as ElectronWindow from "../../electron/ElectronWindow.ts";
import { authenticateCloudflareAccess, installCloudflareAccessCookie } from "./cloudflareAccess.ts";
import { normalizeCloudflareAccessOrigin } from "./cloudflareAccessOrigin.ts";

const electronMock = vi.hoisted(() => ({
  cookies: {
    get: vi.fn(),
    remove: vi.fn(),
    set: vi.fn(),
  },
}));

vi.mock("electron", () => ({
  session: {
    defaultSession: {
      cookies: electronMock.cookies,
    },
  },
}));

beforeEach(() => {
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
    },
    once: vi.fn(),
    removeListener: vi.fn(),
    loadURL: vi.fn(),
    isDestroyed: vi.fn(() => false),
    close: vi.fn(),
  };
}

function electronWindowLayer(authWindow: Electron.BrowserWindow) {
  return ElectronWindow.ElectronWindow.of({
    create: () => Effect.succeed(authWindow),
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
