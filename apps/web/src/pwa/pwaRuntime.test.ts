import { describe, expect, it } from "vite-plus/test";
import type { ServerDeviceNotification } from "@t3tools/contracts";
import { Children, isValidElement, type ReactElement, type ReactNode } from "react";

import {
  canUseNativeDesktopNotifications,
  createDesktopNotificationDeliveryCoordinator,
  createSerializedRegistrationRefresh,
  PwaRuntimeView,
  PwaServiceWorkerRegistration,
  resolveDeepLinkTarget,
  shouldRegisterDesktopNotifications,
} from "./pwaRuntime";

const notification: ServerDeviceNotification = {
  notificationId: "notification-1",
  ackToken: "ack-token",
  title: "Task finished",
  body: "The task finished.",
  deepLink: "/environment/thread",
  createdAt: "2026-07-09T00:00:00.000Z" as ServerDeviceNotification["createdAt"],
  requireInteraction: true,
};

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

describe("createDesktopNotificationDeliveryCoordinator", () => {
  it("closes an accepted native notification when dismiss races its pending show", async () => {
    let resolveShow!: (shown: boolean) => void;
    const closeNativeCalls: string[] = [];
    const fallbackNotifications: ServerDeviceNotification[] = [];
    const coordinator = createDesktopNotificationDeliveryCoordinator({
      showNative: () =>
        new Promise<boolean>((resolve) => {
          resolveShow = resolve;
        }),
      closeNative: (notificationId) => closeNativeCalls.push(notificationId),
      showFallback: (nextNotification) => fallbackNotifications.push(nextNotification),
    });

    coordinator.show(notification);
    coordinator.dismiss(notification.notificationId);
    expect(closeNativeCalls).toEqual(["notification-1"]);

    resolveShow(true);
    await Promise.resolve();

    expect(closeNativeCalls).toEqual(["notification-1", "notification-1"]);
    expect(fallbackNotifications).toEqual([]);
  });

  it("does not fall back to renderer notifications after a cancelled native show fails", async () => {
    let rejectShow!: (error: unknown) => void;
    const fallbackNotifications: ServerDeviceNotification[] = [];
    const coordinator = createDesktopNotificationDeliveryCoordinator({
      showNative: () =>
        new Promise<boolean>((_resolve, reject) => {
          rejectShow = reject;
        }),
      closeNative: () => undefined,
      showFallback: (nextNotification) => fallbackNotifications.push(nextNotification),
    });

    coordinator.show(notification);
    coordinator.dismiss(notification.notificationId);
    rejectShow(new Error("native show failed"));
    await Promise.resolve();

    expect(fallbackNotifications).toEqual([]);
  });

  it("falls back to renderer notifications when an uncancelled native show is refused", async () => {
    const fallbackNotifications: ServerDeviceNotification[] = [];
    const coordinator = createDesktopNotificationDeliveryCoordinator({
      showNative: async () => false,
      closeNative: () => undefined,
      showFallback: (nextNotification) => fallbackNotifications.push(nextNotification),
    });

    coordinator.show(notification);
    await Promise.resolve();

    expect(fallbackNotifications).toEqual([notification]);
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

describe("shouldRegisterDesktopNotifications", () => {
  it("stops after the native support probe when registration was cancelled", async () => {
    let resolveProbe!: (supported: boolean) => void;
    let cancelled = false;
    let permissionRequests = 0;
    const registrationAllowed = shouldRegisterDesktopNotifications({
      bridge: {
        showNotification: async () => true,
        isNotificationSupported: () =>
          new Promise<boolean>((resolve) => {
            resolveProbe = resolve;
          }),
      },
      isCancelled: () => cancelled,
      requestPermission: async () => {
        permissionRequests += 1;
        return "granted";
      },
    });

    cancelled = true;
    resolveProbe(false);

    await expect(registrationAllowed).resolves.toBe(false);
    expect(permissionRequests).toBe(0);
  });
});

describe("createSerializedRegistrationRefresh", () => {
  it("prevents a delayed registration response from arriving after a newer refresh", async () => {
    let resolveFirst!: () => void;
    let refreshCalls = 0;
    const refresh = createSerializedRegistrationRefresh(async () => {
      refreshCalls += 1;
      if (refreshCalls === 1) {
        await new Promise<void>((resolve) => {
          resolveFirst = resolve;
        });
      }
    });

    const first = refresh();
    const overlapping = refresh();
    await Promise.resolve();

    expect(overlapping).toBe(first);
    expect(refreshCalls).toBe(1);

    resolveFirst();
    await first;
    await refresh();

    expect(refreshCalls).toBe(2);
  });
});
