import { describe, expect, it } from "vite-plus/test";
import { Children, isValidElement, type ReactElement, type ReactNode } from "react";

import {
  canUseNativeDesktopNotifications,
  PwaRuntimeView,
  PwaServiceWorkerRegistration,
  resolveDeepLinkTarget,
} from "./pwaRuntime";

describe("resolveDeepLinkTarget", () => {
  it("keeps notification deep links as pathnames for browser history", () => {
    expect(resolveDeepLinkTarget("/env-1/thread-1", "browser")).toBe("/env-1/thread-1");
  });

  it("routes notification deep links through hash history for Electron", () => {
    expect(resolveDeepLinkTarget("/env-1/thread-1", "hash")).toBe("#/env-1/thread-1");
  });

  it("rejects missing, non-path, and protocol-relative deep links", () => {
    expect(resolveDeepLinkTarget(undefined, "hash")).toBeNull();
    expect(resolveDeepLinkTarget("https://example.com/env-1/thread-1", "hash")).toBeNull();
    expect(resolveDeepLinkTarget("//example.com/env-1/thread-1", "hash")).toBeNull();
  });
});

describe("PwaRuntimeView", () => {
  it("registers the app shell service worker before an environment exists", () => {
    const runtime = PwaRuntimeView({ enabled: true, environmentId: null });

    expect(isValidElement(runtime)).toBe(true);
    const children = Children.toArray(
      (runtime as ReactElement<{ readonly children: ReactNode }>).props.children,
    );
    expect(children).toHaveLength(1);
    expect(isValidElement(children[0]) && children[0].type).toBe(PwaServiceWorkerRegistration);
  });

  it("does not mount browser PWA side effects for unsupported runtimes", () => {
    expect(PwaRuntimeView({ enabled: false, environmentId: null })).toBeNull();
  });
});

describe("canUseNativeDesktopNotifications", () => {
  it("requires both the native show bridge and a positive support probe", async () => {
    await expect(
      canUseNativeDesktopNotifications({
        showNotification: async () => true,
        isNotificationSupported: async () => true,
      }),
    ).resolves.toBe(true);

    await expect(
      canUseNativeDesktopNotifications({
        showNotification: async () => false,
        isNotificationSupported: async () => false,
      }),
    ).resolves.toBe(false);
  });

  it("falls back to renderer permission when the support probe is missing or rejects", async () => {
    await expect(
      canUseNativeDesktopNotifications({
        showNotification: async () => true,
      }),
    ).resolves.toBe(false);

    await expect(
      canUseNativeDesktopNotifications({
        showNotification: async () => true,
        isNotificationSupported: async () => {
          throw new Error("probe failed");
        },
      }),
    ).resolves.toBe(false);
  });
});
